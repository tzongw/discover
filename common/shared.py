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
from base import Registry, LogSuppress
from base import Executor, Schedule
from base import UniqueId, snowflake
from base import Publisher, Receiver, Timer
from base import SmartInvalidator, SmartParser
from base import Dispatcher, TimeDispatcher
from base.sharding import ShardingKey, ShardingTimer, ShardingReceiver, ShardingPublisher
from base.utils import func_desc, ip_address
from . import const
from .config import options
import service
from .rpc_service import UserService, GateService, TimerService
from base import AsyncTask, HeavyTask, Poller, Script

executor = Executor(name='shared')
schedule = Schedule()
dispatcher = Dispatcher()
tick = TimeDispatcher()

app_name = options.app_name
registry = Registry(Redis.from_url(options.registry, decode_responses=True))

redis = RedisCluster.from_url(options.redis_cluster, decode_responses=True) if options.redis_cluster else \
    Redis.from_url(options.redis, decode_responses=True)
unique_id = UniqueId(schedule, redis)
app_id = unique_id.gen(app_name, range(snowflake.max_worker_id))
id_generator = snowflake.IdGenerator(options.datacenter, app_id)
hint = f'{options.env.value}:{ip_address()}:{app_id}'
parser = SmartParser(redis)
invalidator = SmartInvalidator(redis)
heavy_task = HeavyTask(redis, 'heavy_tasks')
script = Script(redis)

if options.redis_cluster:
    timer = ShardingTimer(redis, hint=hint, sharding_key=ShardingKey(shards=3, fixed=[const.TICK_TIMER]))
    publisher = ShardingPublisher(redis, hint=hint)
    receiver = ShardingReceiver(redis, group=app_name, consumer=hint)
else:
    timer = Timer(redis, hint=hint)
    publisher = Publisher(redis, hint=hint)
    receiver = Receiver(redis, group=app_name, consumer=hint)

async_task = AsyncTask(timer, publisher, receiver)
poller = Poller(redis, async_task)


def async_heavy(f):
    @heavy_task
    @functools.wraps(f)
    def task(*args, **kwargs):
        return f(*args, **kwargs)

    @async_task
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        heavy_task.push(task(*args, **kwargs))

    wrapper.wrapped = f
    return wrapper


user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, service.user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, service.gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER)  # type: Union[TimerService, service.timer.Iface]

_exits = [registry.stop, receiver.stop]
_mains = []

if options.env is const.Environment.DEV:
    # in dev, run in worker to debug
    HeavyTask.push = lambda self, task: spawn_worker(self.exec, task)

if options.env is not const.Environment.STAGING:
    # staging should not impact prod
    @receiver.group(const.TICK_STREAM)
    def _on_tick(_, sid):
        ts = int(sid[:-2])
        tick.dispatch(ts)


@dataclass
class Status:
    inited: bool = False
    exiting: bool = False


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
    status.inited = True
    executor.gather(*_mains)
    _mains.clear()


atexit.register(unique_id.stop)  # after cleanup


@atexit.register
def _cleanup():
    status.exiting = True
    with LogSuppress():
        if options.env is const.Environment.DEV:  # ptpython compatible
            for fn in _exits:
                fn()
        else:
            executor.gather(*_exits)
    _exits.clear()


def spawn_worker(f, *args, **kwargs):
    desc = func_desc(f)

    def worker():
        start = time.time()
        with LogSuppress():
            f(*args, **kwargs)
        t = time.time() - start
        if t > const.SLOW_WORKER:
            logging.warning(f'slow worker {t} {desc}')

    g = gevent.spawn(worker)
    _workers[g] = desc


def run_in_worker(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        spawn_worker(f, *args, **kwargs)

    return wrapper


def _sig_handler(sig, frame):
    def graceful_exit():
        logging.info(f'exit {sig} {frame}')
        _cleanup()
        if sig != signal.SIGUSR1:
            seconds = {const.Environment.DEV: 0, const.Environment.TEST: 10}.get(options.env, 30)
            gevent.sleep(seconds)  # wait for requests & messages
            gevent.joinall(_workers, timeout=const.SLOW_WORKER)  # try to finish all tasks
            for worker, desc in _workers.items():
                if worker:  # still active
                    logging.error(f'worker {desc} not finish')
            sys.exit(0)

    gevent.spawn(graceful_exit)


signal.signal(signal.SIGTERM, _sig_handler)
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGQUIT, _sig_handler)
signal.signal(signal.SIGUSR1, _sig_handler)
