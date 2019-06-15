from collections import defaultdict
from random import choice
import contextlib
from service import Service
from thrift_pool import ThriftPool


class ServicePools:
    def __init__(self, service: Service):
        self._service = service
        self._service_pools = defaultdict(dict)  # type: dict[str, dict[str, ThriftPool]]

    @contextlib.contextmanager
    def connection(self, service_name):
        addresses = self._service.address(service_name)
        address = choice(tuple(addresses))  # type: str
        pools = self._service_pools[service_name]
        pool = pools.get(address)
        if not pool:
            host, port = address.split(':')
            pool = ThriftPool(host, int(port))
            pools[address] = pool
        with pool.connection() as conn:
            yield conn
