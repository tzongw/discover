from collections import defaultdict
from random import choice
import contextlib
from service import Service
from thrift_pool import ThriftPool
from thrift.Thrift import TException
import logging
import gevent


class ServicePools:
    _INTERVAL = 10

    def __init__(self, service: Service):
        self._service = service
        self._service_pools = defaultdict(dict)  # type: dict[str, dict[str, ThriftPool]]
        self._runner = gevent.spawn(self._run)

    def __del__(self):
        gevent.kill(self._runner)

    @contextlib.contextmanager
    def connection(self, service_name):
        addresses = self._service.address(service_name)
        address = choice(tuple(addresses))  # type: str
        with self.address_connection(service_name, address) as conn:
            yield conn

    @contextlib.contextmanager
    def address_connection(self, service_name, address):
        addresses = self._service.address(service_name)
        if address not in addresses:
            raise LookupError()
        pools = self._service_pools[service_name]
        pool = pools.get(address)
        if not pool:
            host, port = address.split(':')
            pool = ThriftPool(host, int(port))
            pools[address] = pool
        with pool.connection() as conn:
            try:
                yield conn
            except TException as e:
                logging.error(f'error: {e}')

    def _clean_pools(self):
        for service_name, pools in self._service_pools:
            available_addresses = self._service.address(service_name)
            holding_addresses = set(pools.keys())
            for removed_address in (holding_addresses - available_addresses):
                pool = pools.pop(removed_address)
                pool.close_all()

    def _run(self):
        while True:
            try:
                self._clean_pools()
            except Exception as e:
                logging.error(f'error: {e}')
            finally:
                gevent.sleep(self._INTERVAL)


