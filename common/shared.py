import signal
from typing import Union
import gevent
from redis import Redis
from . import const
import service
from base.registry import Registry
from .rpc_service import UserService, GateService, TimerService
import sys
from base.executor import Executor
from base.schedule import Schedule
from base.unique import UniqueId
from base.utils import Dispatcher, LogSuppress, Parser
from base.mq import Publisher
from base.timer import Timer
from tornado.options import options

redis = Redis.from_url(options.redis, decode_responses=True)
registry = Registry(redis)
publisher = Publisher(redis)
parser = Parser(redis)
timer = Timer(redis, cache=True)
user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, service.user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, service.gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER)  # type: Union[TimerService, service.timer.Iface]

executor = Executor(name='shared')
schedule = Schedule()
unique_id = UniqueId(schedule, redis)
timer_dispatcher = Dispatcher(sep=':')

_exits = [registry.stop]


def at_exit(fun):
    _exits.append(fun)


def _sig_handler(sig, frame):
    def grace_exit():
        for fun in _exits:
            with LogSuppress(Exception):
                fun()
        gevent.sleep(1)
        unique_id.stop()
        sys.exit(0)

    gevent.spawn(grace_exit)


signal.signal(signal.SIGTERM, _sig_handler)
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGQUIT, _sig_handler)
