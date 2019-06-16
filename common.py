import contextlib
from redis import Redis
from service_pools import ServicePools
from service import Service
import const
from .generated.service import gate, user


class _ServicePools(ServicePools):
    def __init__(self, service, **settings):
        super().__init__(service, **settings)

    @contextlib.contextmanager
    def user_client(self):
        with self.connection(const.SERVICE_USER) as conn:
            yield user.Client(conn)

    @contextlib.contextmanager
    def gate_client(self):
        with self.connection(const.SERVICE_GATE) as conn:
            yield gate.Client(conn)

    @contextlib.contextmanager
    def address_gate_client(self, address):
        with self.address_connection(const.SERVICE_GATE, address) as conn:
            yield gate.Client(conn)


redis = Redis()
service = Service(redis)
service_pools = _ServicePools(service)
