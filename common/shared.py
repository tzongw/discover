import signal
import logging
from typing import Union
from weakref import WeakSet
import sys
import gevent
from redis import Redis
from base import Registry, LogSuppress
from base import Executor
from base import Schedule
from base import UniqueId
from base import Dispatcher, Parser
from base import Publisher
from base import Timer
from base import Invalidator
from . import const
from .config import options
import service
from .rpc_service import UserService, GateService, TimerService

redis = Redis.from_url(options.redis, decode_responses=True)
registry = Registry(redis)
publisher = Publisher(redis)
parser = Parser(redis)
timer = Timer(redis)
user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, service.user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, service.gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER)  # type: Union[TimerService, service.timer.Iface]

executor = Executor(name='shared')
schedule = Schedule()
unique_id = UniqueId(schedule, redis)
dispatcher = Dispatcher(sep=':')
invalidator = Invalidator(redis)

_exits = [registry.stop]
_mains = []

inited = False
exited = False
workers = WeakSet()  # task workers, try to join all before exiting


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


def _sig_handler(sig, frame):
    def graceful_exit():
        global exited
        exited = True
        logging.info(f'exit {sig} {frame}')
        with LogSuppress(Exception):
            executor.gather(*_exits)
        _exits.clear()
        if sig != signal.SIGUSR1:
            seconds = {const.Environment.DEV: 0, const.Environment.TEST: 10}.get(options.env, 30)
            gevent.sleep(seconds)  # wait for requests & messages
            gevent.joinall(workers, timeout=30)  # try to finish all tasks
            if workers:
                logging.error(f'workers not finish {workers}')
            unique_id.stop()
            sys.exit(0)

    gevent.spawn(graceful_exit)


signal.signal(signal.SIGTERM, _sig_handler)
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGQUIT, _sig_handler)
signal.signal(signal.SIGUSR1, _sig_handler)
