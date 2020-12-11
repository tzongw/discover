import logging
import signal
from typing import Union
import gevent
from redis import Redis
from tornado.log import LogFormatter
from . import const
from service import gate, user, timer
from base.registry import Registry
from .rpc_service import UserService, GateService, TimerService
import sys
from base.executor import Executor
from base.schedule import Schedule
from base.unique import UniqueId
from base.utils import Dispatcher, LogSuppress
from base.mq import Publisher

LOG_FORMAT = "%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s"
channel = logging.StreamHandler()
channel.setFormatter(LogFormatter(fmt=LOG_FORMAT, datefmt=None))
logger = logging.getLogger()
logger.addHandler(channel)

redis = Redis(decode_responses=True)
registry = Registry(redis)
publisher = Publisher(redis)
user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER)  # type: Union[TimerService, timer.Iface]

executor = Executor()
schedule = Schedule(executor)
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
