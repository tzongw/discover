import contextlib
from redis import Redis
from service_pools import ServicePools
from service import Service
import const
from .generated.service import gate, user
from typing import ContextManager, Union


class _ServicePools(ServicePools):
    def __init__(self, service, **settings):
        super().__init__(service, **settings)

    @contextlib.contextmanager
    def user_client(self) -> ContextManager[user.Iface]:
        with self.connection(const.SERVICE_USER) as conn:
            yield user.Client(conn)

    @contextlib.contextmanager
    def address_gate_client(self, address) -> ContextManager[gate.Iface]:
        with self.address_connection(const.SERVICE_GATE, address) as conn:
            yield gate.Client(conn)

    @staticmethod
    def _retry_strategy(client_factory, item, *args, **kwargs):
        with client_factory() as client:


    def __getattr__(self, item):
        if hasattr(user.Iface, item):
            return


redis = Redis()
service = Service(redis)
service_pools = _ServicePools(service)  # type: Union[_ServicePools, user.Iface, gate.Iface]
