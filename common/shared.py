import signal
from typing import Union
from weakref import WeakSet
import sys
import gevent
from redis import Redis
from base import Registry
from base import Executor
from base import Schedule
from base import UniqueId
from base import Dispatcher, LogSuppress, Parser
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

exiting = False  # grace exiting, stop receive new tasks
workers = WeakSet()  # heavy task workers, try join all before exit


def at_main(fun):
    assert callable(fun)
    _mains.append(fun)


def at_exit(fun):
    assert callable(fun)
    _exits.append(fun)


def init_main():
    executor.gather(*_mains)


def _sig_handler(sig, frame):
    def grace_exit():
        global exiting
        exiting = True
        with LogSuppress(Exception):
            executor.gather(*_exits)
            gevent.sleep(1)
            greenlets = list(workers)
            gevent.joinall(greenlets, timeout=30, raise_error=False)  # try finish all jobs
            unique_id.stop()
        sys.exit(0)

    gevent.spawn(grace_exit)


signal.signal(signal.SIGTERM, _sig_handler)
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGQUIT, _sig_handler)
