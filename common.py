import logging
import signal
from typing import Union
import gevent
from redis import Redis
from tornado.log import LogFormatter
import const
from service import gate, user, timer
from registry import Registry
from rpc_service import UserService, GateService, TimerService
import sys
from executor import Executor
from schedule import Schedule
from unique import UniqueId

LOG_FORMAT = "%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s"
channel = logging.StreamHandler()
channel.setFormatter(LogFormatter(fmt=LOG_FORMAT, datefmt=None))
logger = logging.getLogger()
logger.addHandler(channel)

redis = Redis(decode_responses=True)
registry = Registry(redis)
user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, gate.Iface]
timer_service = TimerService(registry, const.RPC_TIMER)  # type: Union[TimerService, timer.Iface]

executor = Executor()
schedule = Schedule(executor)
unique_id = UniqueId(schedule, redis, "worker", range(1024))


def sig_handler(sig, frame):
    def grace_exit():
        registry.stop()
        unique_id.stop()
        gevent.sleep(1)
        sys.exit(0)

    gevent.spawn(grace_exit)


signal.signal(signal.SIGTERM, sig_handler)
signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGQUIT, sig_handler)
