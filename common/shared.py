import functools
import signal
import logging
import time
import sys
import atexit
from dataclasses import dataclass
from typing import Union
from weakref import WeakSet
import gevent
from redis import Redis, RedisCluster
from base import Registry, LogSuppress, Exclusion
from base import Executor, Scheduler
from base import UniqueId, snowflake
from base import Publisher, Receiver, Timer
from base import create_invalidator, create_parser
from base import Dispatcher, TimeDispatcher
from base.sharding import ShardingKey, ShardingTimer, ShardingReceiver, ShardingPublisher, ShardingHeavyTask
from base import func_desc, ip_address, base62, singleflight
from base import AsyncTask, HeavyTask, Poller, Script
import service
from . import const
from .config import options, ctx
from .rpc_service import UserService, GateService, TimerService

executor = Executor(name='shared')
scheduler = Scheduler()
dispatcher = Dispatcher()
time_dispatcher = TimeDispatcher()

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
    run_in_process = heavy_task = ShardingHeavyTask(redis, f'heavy_tasks:{options.env.value}')
else:
    timer = Timer(redis, hint=hint)
    publisher = Publisher(redis, hint=hint)
    receiver = Receiver(redis, group=app_name, consumer=hint)
    run_in_process = heavy_task = HeavyTask(redis, f'heavy_tasks:{options.env.value}')

async_task = AsyncTask(timer, publisher, receiver)
poller = Poller(redis, async_task)

user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, service.user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, service.gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER)  # type: Union[TimerService, service.timer.Iface]

_exits = [registry.stop, receiver.stop, heavy_task.stop]
_mains = []

if options.env is const.Environment.DEV:
    # if dev, run in thread to debug
    ShardingHeavyTask.push = HeavyTask.push = lambda self, task, front=False: spawn_worker(self.exec, task)


@receiver(const.TICK_STREAM)
def _on_tick(_, sid):
    ts = int(sid[:-2])
    time_dispatcher.dispatch_tick(ts)


@dataclass
class Status:
    inited: bool = False
    exiting: bool = False
    sysexit: bool = True


status = Status()
_workers = WeakSet()  # thread workers


def at_main(func):
    assert callable(func) and not status.inited
    _mains.append(func)
    return func


def at_exit(func):
    assert callable(func) and not status.exiting
    _exits.append(func)
    return func


def init_main():
    assert not status.inited
    status.inited = True
    executor.gather(_mains)


atexit.register(unique_id.stop)  # at last
atexit.register(executor.join)  # wait all task done
atexit.register(lambda: gevent.joinall(_workers))  # wait all thread workers


@atexit.register
@singleflight
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


def spawn_worker(f, *args, **kwargs):
    def worker():
        ctx.trace = base62.encode(id_generator.gen())
        start = time.time()
        with LogSuppress():
            f(*args, **kwargs)
        t = time.time() - start
        if t > const.SLOW_WORKER:
            logging.warning(f'slow worker {t} {func_desc(f)} {args = } {kwargs = }')

    g = gevent.spawn(worker)
    _workers.add(g)


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
