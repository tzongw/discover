import gc
import sys
import time
import signal
import atexit
import logging
from functools import wraps
from dataclasses import dataclass
from typing import Union
import gevent
from redis import RedisCluster
from base import Registry, LogSuppress, ZTimer
from base import Executor, Scheduler
from base import UniqueId, snowflake
from base import Producer, Consumer, Timer
from base import create_invalidator, create_parser
from base import Dispatcher, TimeDispatcher
from base.utils import create_redis
from base.sharding import Sharding, ShardingTimer, ShardingConsumer, ShardingProducer, ShardingHeavyTask, \
    ShardingZTimer
from base import func_desc, Base62, once
from base import AsyncTask, HeavyTask, Poller, Script
import service
from . import const
from .config import options, ctx
from .rpc_service import UserService, GateService, TimerService

app_name = options.app_name
rpc_service = options.rpc_service
http_service = options.http_service
executor = Executor(name='shared')
dispatcher = Dispatcher(executor)
time_dispatcher = TimeDispatcher(executor)
scheduler = Scheduler()
redis = create_redis(options.redis)
registry = Registry(redis if options.registry is None else create_redis(options.registry), const.SERVICES)
unique_id = UniqueId(redis)
app_id = unique_id.gen(app_name, range(snowflake.max_worker_id))
snowflake = snowflake.Snowflake(options.datacenter, app_id)
hint = f'{options.env}:{options.host}:{app_id}'
parser = create_parser(redis)
script = Script(redis)
invalidator = create_invalidator(redis)

if isinstance(redis, RedisCluster):
    ztimer = ShardingZTimer(redis, app_name, sharding=Sharding(shards=3))
    timer = ShardingTimer(redis, hint=hint, sharding=Sharding(shards=3, fixed_keys=[const.TICK_TIMER]))
    producer = ShardingProducer(redis, hint=hint)
    consumer = ShardingConsumer(redis, group=app_name, name=hint)
    heavy_task = ShardingHeavyTask(redis, app_name)
else:
    ztimer = ZTimer(redis, app_name)
    timer = Timer(redis, hint=hint)
    producer = Producer(redis, hint=hint)
    consumer = Consumer(redis, group=app_name, name=hint)
    heavy_task = HeavyTask(redis, app_name)

async_task = AsyncTask(timer, producer, consumer)
poller = Poller(redis, async_task)

user_service = UserService(registry, const.RPC_USER, options.host)  # type: Union[UserService, service.user.Iface]
gate_service = GateService(registry, const.RPC_GATE, options.host)  # type: Union[GateService, service.gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER, options.host)  # type: Union[TimerService, service.timer.Iface]

if options.env == const.Environment.DEV:
    # for debug
    HeavyTask.push = lambda self, task: spawn_worker(self.exec, task)


@dataclass
class Status:
    inited = False
    exiting = False
    sysexit = True


status = Status()
_workers = set()  # thread workers
_mains = []
_exits = [registry.stop, consumer.stop, heavy_task.stop]
mercy = {const.Environment.DEV: 1, const.Environment.TEST: 5}.get(options.env, 30)  # wait time for graceful exit


def at_main(func):
    assert callable(func) and not status.inited
    _mains.append(func)
    return func


def to_exit(func):
    assert callable(func) and not status.exiting
    _exits.append(func)
    return func


def init_main():
    assert not status.inited
    status.inited = True
    executor.gather(_mains)
    # optimize gc STW
    start = time.time()
    gc.collect()
    gc.freeze()
    logging.info(f'gc freeze: {gc.get_freeze_count()} elapsed: {time.time() - start}')


@atexit.register
def gracefully_exit():
    gevent.joinall(list(_workers))
    consumer.join()
    scheduler.join()
    executor.join()
    unique_id.stop()  # at last


@atexit.register
@once
def _cleanup():  # call once
    logging.info(f'cleanup')
    with LogSuppress():
        if sys.argv[0].endswith('ptpython'):  # ptpython compatible
            for fn in _exits:
                fn()
        else:
            executor.gather(_exits)


def spawn_worker(f, *args, **kwargs):
    def worker():
        ctx.trace = Base62.encode(snowflake.gen())
        start = time.time()
        with LogSuppress():
            f(*args, **kwargs)
        t = time.time() - start
        if t > 30:
            logging.warning(f'slow worker {t} {func_desc(f)} {args = } {kwargs = }')
        _workers.remove(g)

    g = gevent.spawn(worker)
    _workers.add(g)
    return g


def async_worker(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        spawn_worker(f, *args, **kwargs)

    return wrapper


def _sig_handler(sig, frame):
    logging.info(f'received signal {sig}')
    if status.exiting:  # signal again
        if sig == signal.SIGINT:
            sys.exit(1)
    else:
        def sig_exit():
            _cleanup()
            if sig == signal.SIGUSR1 or not status.sysexit:
                return
            gevent.sleep(mercy)
            sys.exit(0)

        status.exiting = True
        gevent.spawn(sig_exit)


signal.signal(signal.SIGTERM, _sig_handler)
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGHUP, _sig_handler)
signal.signal(signal.SIGUSR1, _sig_handler)
