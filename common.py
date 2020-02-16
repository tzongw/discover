import contextlib
import logging
import signal
from typing import Union
import gevent
from redis import Redis
from tornado.log import LogFormatter
import const
from generated.service import gate, user
from registry import Registry
from service import UserService, GateService
import sys

_redis = Redis(decode_responses=True)
registry = Registry(_redis)
user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, gate.Iface]


def sig_handler(sig, frame):
    def grace_exit():
        registry.stop()
        gevent.sleep(1)
        sys.exit(0)

    gevent.spawn(grace_exit)


signal.signal(signal.SIGTERM, sig_handler)
signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGQUIT, sig_handler)

LOG_FORMAT = "%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s"
channel = logging.StreamHandler()
channel.setFormatter(LogFormatter(fmt=LOG_FORMAT, datefmt=None))
logger = logging.getLogger()
logger.addHandler(channel)
