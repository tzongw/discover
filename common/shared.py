import functools
import signal
import logging
import time
import atexit
from dataclasses import dataclass
from typing import Union
from weakref import WeakKeyDictionary
import sys
import gevent
from redis import Redis, RedisCluster
from base import Registry, LogSuppress, Exclusion
from base import Executor, Scheduler
from base import UniqueId, snowflake
from base import Publisher, Receiver, Timer
from base import create_invalidator, create_parser
from base import Dispatcher, TimeDispatcher
from base.sharding import ShardingKey, ShardingTimer, ShardingReceiver, ShardingPublisher, ShardingHeavyTask
from base import func_desc, ip_address, base62
from base import AsyncTask, HeavyTask, Poller, Script
import service
from . import const
from .config import options, ctx
from .rpc_service import UserService, GateService, TimerService

executor = Executor(name='shared')
scheduler = Scheduler()
dispatcher = Dispatcher()
tick = TimeDispatcher()

app_name = options.app_name
redis = RedisCluster.from_url(options.redis_cluster, decode_responses=True) if options.redis_cluster else \
    Redis.from_url(options.redis, decode_responses=True)
registry = Registry(redis, const.SERVICES)
unique_id = UniqueId(redis)
app_id = unique_id.gen(app_name, range(snowflake.max_worker_id))
id_generator = snowflake.IdGenerator(options.datacenter, app_id)
hint = f'{options.env.value}:{ip_address()}:{app_id}'
parser = create_parser(redis)
invalidator = create_invalidator(redis)
script = Script(redis)
run_exclusively = Exclusion(redis)

if options.redis_cluster:
    timer = ShardingTimer(redis, hint=hint, sharding_key=ShardingKey(shards=3, fixed=[const.TICK_TIMER]))
    publisher = ShardingPublisher(redis, hint=hint)
    receiver = ShardingReceiver(redis, group=app_name, consumer=hint)
    run_in_background = heavy_task = ShardingHeavyTask(redis, f'heavy_tasks:{options.env.value}')
else:
    timer = Timer(redis, hint=hint)
    publisher = Publisher(redis, hint=hint)
    receiver = Receiver(redis, group=app_name, consumer=hint)
    run_in_background = heavy_task = HeavyTask(redis, f'heavy_tasks:{options.env.value}')

async_task = AsyncTask(timer, publisher, receiver)
poller = Poller(redis, async_task)

user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, service.user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, service.gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER)  # type: Union[TimerService, service.timer.Iface]

_exits = [registry.stop, receiver.stop, heavy_task.stop]
_mains = []

if options.env is const.Environment.DEV:
    # in dev, run in worker to debug
    HeavyTask.push = lambda self, task: spawn_worker(self.exec, task)


@receiver(const.TICK_STREAM)
def _on_tick(_, sid):
    ts = int(sid[:-2])
    tick.dispatch(ts)


@dataclass
class Status:
    inited: bool = False
    exiting: bool = False
    exited: bool = False
    sysexit: bool = True


status = Status()
_workers = WeakKeyDictionary()  # task workers, try to join all before exiting


def at_main(fun):
    assert callable(fun) and not status.inited
    _mains.append(fun)
    return fun


def at_exit(fun):
    assert callable(fun) and not status.exiting
    _exits.append(fun)
    return fun


def init_main():
    if status.inited:
        return
    status.inited = True
    executor.gather(_mains)


atexit.register(unique_id.stop)  # after cleanup


@atexit.register
def _wait_workers():
    gevent.joinall(_workers, timeout=const.SLOW_WORKER)  # try to finish all tasks
    for worker, desc in _workers.items():
        if worker:  # still active
            logging.error(f'worker {desc} not finish')


@atexit.register
def _cleanup():
    if status.exiting:
        return
    status.exiting = True
    with LogSuppress():
        if options.env is const.Environment.DEV:  # ptpython compatible
            for fn in _exits:
                fn()
        else:
            executor.gather(_exits)
    status.exited = True


def spawn_worker(f, *args, **kwargs):
    def worker():
        ctx.trace = trace
        start = time.time()
        with LogSuppress():
            f(*args, **kwargs)
        t = time.time() - start
        if t > const.SLOW_WORKER:
            logging.warning(f'slow worker {t} {desc} {args = } {kwargs = }')

    trace = base62.encode(id_generator.gen())
    desc = func_desc(f)
    g = gevent.spawn(worker)
    _workers[g] = f'{desc} trace: {trace}'


def run_in_worker(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        spawn_worker(f, *args, **kwargs)

    return wrapper


def _sig_handler(sig, frame):
    def graceful_exit():
        logging.info(f'exit {sig}')
        _cleanup()
        if sig == signal.SIGUSR1 or not status.sysexit:
            return
        seconds = {const.Environment.DEV: 1, const.Environment.TEST: 3}.get(options.env, 30)
        gevent.sleep(seconds)  # wait for requests & messages
        sys.exit(0)

    gevent.spawn(graceful_exit)


signal.signal(signal.SIGTERM, _sig_handler)
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGQUIT, _sig_handler)
signal.signal(signal.SIGUSR1, _sig_handler)
