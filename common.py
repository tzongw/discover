import contextlib
from redis import Redis
from service_pools import ServicePools
from service import Service
import const
from generated.service import gate, user
from typing import ContextManager, Union
from functools import partial


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
    def _one_shot(client_factory, item, *args, **kwargs):
        with client_factory() as client:
            return getattr(client, item)(client, *args, **kwargs)

    @staticmethod
    def _traverse(address_client_factory, addresses, item, *args, **kwargs):
        for address in addresses:
            with address_client_factory(address) as client:
                getattr(client, item)(client, *args, **kwargs)

    def __getattr__(self, item):
        if hasattr(user.Iface, item):
            return partial(self._one_shot, self.user_client, item)
        if hasattr(gate.Iface, item):
            addresses = self._service.addresses(const.SERVICE_GATE)
            return partial(self._traverse, addresses, item)
        return super().__getattr__(item)


redis = Redis()
service = Service(redis)
service_pools = _ServicePools(service)  # type: Union[_ServicePools, user.Iface, gate.Iface]
