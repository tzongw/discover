import signal
import logging
import time
import atexit
from typing import Union
from weakref import WeakKeyDictionary
import sys
import gevent
from redis import Redis
from base import Registry, LogSuppress, Dispatcher, snowflake, Receiver
from base import Executor
from base import Schedule
from base import UniqueId
from base import Parser
from base import Publisher
from base import Timer
from base import Invalidator
from base.utils import func_desc
from . import const
from .config import options
import service
from .rpc_service import UserService, GateService, TimerService
from .task import AsyncTask, HeavyTask

executor = Executor(name='shared')
schedule = Schedule()
dispatcher = Dispatcher(sep=':')

app_name = options.app_name
env = options.env
registry = Registry(Redis.from_url(options.registry, decode_responses=True))

redis = Redis.from_url(options.redis, decode_responses=True)
parser = Parser(redis)
invalidator = Invalidator(redis)
unique_id = UniqueId(schedule, redis)
app_id = unique_id.gen(app_name, range(snowflake.max_worker_id))
id_generator = snowflake.IdGenerator(options.datacenter, app_id)
publisher = Publisher(redis, hint=str(app_id))
receiver = Receiver(redis, group=app_name, consumer=str(app_id))
timer = Timer(redis, hint=str(app_id))
async_task = AsyncTask(timer, receiver)
heavy_task = HeavyTask(redis, 'heavy_tasks')

user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, service.user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, service.gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER)  # type: Union[TimerService, service.timer.Iface]

_exits = [registry.stop, receiver.stop]
_mains = []

inited = False
exited = False
_workers = WeakKeyDictionary()  # task workers, try to join all before exiting


def at_main(fun):
    assert callable(fun) and not inited
    _mains.append(fun)
    return fun


def at_exit(fun):
    assert callable(fun) and not exited
    _exits.append(fun)
    return fun


def init_main():
    global inited
    inited = True
    executor.gather(*_mains)
    _mains.clear()


atexit.register(unique_id.stop)  # after cleanup


@atexit.register
def _cleanup():
    global exited
    exited = True
    with LogSuppress(Exception):
        executor.gather(*_exits)
    _exits.clear()


def spawn_worker(f, *args, **kwargs):
    desc = func_desc(f)

    def worker():
        start = time.time()
        with LogSuppress(Exception):
            f(*args, **kwargs)
        t = time.time() - start
        if t > const.SLOW_WORKER:
            logging.warning(f'slow worker {t} {desc}')

    g = gevent.spawn(worker, *args, **kwargs)
    _workers[g] = desc


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
