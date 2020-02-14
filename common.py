import contextlib
import logging
import signal
from functools import partial
from typing import ContextManager, Union
import gevent
from redis import Redis
from tornado.log import LogFormatter
import const
from generated.service import gate, user
from registry import Registry
from service_pools import ServicePools
from thrift_pool import ThriftPool
from utils import LogSuppress


class Selector:
    @staticmethod
    def _one_shot(client_factory, item, *args, **kwargs):
        with client_factory() as client:
            return getattr(client, item)(*args, **kwargs)

    @staticmethod
    def _retry(client_factory, item, *args, **kwargs):
        try:
            return Selector._one_shot(client_factory, item, *args, **kwargs)
        except Exception as e:
            if ThriftPool.acceptable(e):
                raise
        # will retry another node
        return Selector._one_shot(client_factory, item, *args, **kwargs)

    @staticmethod
    def _traverse(address_client_factory, addresses, item, *args, **kwargs):
        for address in addresses:
            with LogSuppress(Exception):
                with address_client_factory(address) as client:
                    getattr(client, item)(*args, **kwargs)


class UserService(ServicePools, Selector):
    @contextlib.contextmanager
    def client(self) -> ContextManager[user.Iface]:
        with self.connection() as conn:
            yield user.Client(conn)

    def __getattr__(self, item):
        if hasattr(user.Iface, item):
            return partial(self._one_shot, self.client, item)
        return super().__getattr__(item)


class GateService(ServicePools, Selector):
    @contextlib.contextmanager
    def client(self, address) -> ContextManager[gate.Iface]:
        with self.address_connection(address) as conn:
            yield gate.Client(conn)

    def __getattr__(self, item):
        if hasattr(gate.Iface, item):
            addresses = self._service.addresses(const.RPC_GATE)
            return partial(self._traverse, self.client, addresses, item)
        return super().__getattr__(item)


_redis = Redis(decode_responses=True)
registry = Registry(_redis)
user_service = UserService(registry, const.RPC_USER)  # type: Union[UserService, user.Iface]
gate_service = GateService(registry, const.RPC_GATE)  # type: Union[GateService, gate.Iface]

clean_ups = [registry.stop]


def sig_handler(sig, frame):
    for item in clean_ups:
        gevent.spawn(item)


signal.signal(signal.SIGTERM, sig_handler)
signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGQUIT, sig_handler)

LOG_FORMAT = "%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s"
channel = logging.StreamHandler()
channel.setFormatter(LogFormatter(fmt=LOG_FORMAT, datefmt=None))
logger = logging.getLogger()
logger.addHandler(channel)
