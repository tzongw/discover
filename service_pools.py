import contextlib
from collections import defaultdict
from random import choice
from typing import Dict, DefaultDict, ContextManager

from thrift.protocol.TProtocol import TProtocolBase

from service import Service
from thrift_pool import ThriftPool
import logging
import time

Pools = Dict[str, ThriftPool]


class ServicePools:
    def __init__(self, service: Service, **settings):
        self._service = service
        self._service_pools = defaultdict(dict)  # type: DefaultDict[str, Pools]
        self._cool_down = {}  # type: Dict[str, float]
        self._settings = settings
        service.refresh_callback = self._clean_pools

    @contextlib.contextmanager
    def connection(self, service_name) -> ContextManager[TProtocolBase]:
        addresses = self._service.addresses(service_name)
        now = time.time()
        good_ones = [addr for addr in addresses if now > self._cool_down.get(addr, 0)]
        address = choice(good_ones or tuple(addresses))  # type: str
        with self.address_connection(service_name, address) as conn:
            yield conn

    @contextlib.contextmanager
    def address_connection(self, service_name, address) -> ContextManager[TProtocolBase]:
        addresses = self._service.addresses(service_name)
        if address not in addresses:
            raise ValueError(f"{service_name} {address} {addresses}")
        pools = self._service_pools[service_name]
        pool = pools.get(address)
        if not pool:
            host, port = address.rsplit(':', maxsplit=1)
            pool = ThriftPool(host, int(port), **self._settings)
            pools[address] = pool
        with pool.connection() as conn:
            try:
                yield conn
            except Exception as e:
                logging.exception(f'')
                if not ThriftPool.acceptable(e):
                    self._cool_down[address] = time.time() + Service.COOL_DOWN
                raise

    def _clean_pools(self):
        logging.info(f'{self._service_pools}')
        for service_name, pools in self._service_pools.items():
            available_addresses = self._service.addresses(service_name)
            holding_addresses = set(pools.keys())
            for removed_address in (holding_addresses - available_addresses):
                logging.warning(f'clean {service_name} {removed_address}')
                self._cool_down.pop(removed_address, None)
                pool = pools.pop(removed_address)
                pool.close_all()
