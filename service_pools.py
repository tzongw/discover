import contextlib
import logging
from collections import defaultdict
from random import choice
from typing import Dict, DefaultDict, ContextManager

import gevent
from thrift.protocol.TProtocol import TProtocolBase

from service import Service
from thrift_pool import ThriftPool
import common

Pools = Dict[str, ThriftPool]


class ServicePools:
    _INTERVAL = 10

    def __init__(self, service: Service, **settings):
        self._service = service
        self._service_pools = defaultdict(dict)  # type: DefaultDict[str, Pools]
        self._settings = settings
        self._runner = gevent.spawn(self._run)

    def __del__(self):
        gevent.kill(self._runner)

    @contextlib.contextmanager
    def connection(self, service_name) -> ContextManager[TProtocolBase]:
        addresses = self._service.addresses(service_name)
        address = choice(tuple(addresses))  # type: str
        with self.address_connection(service_name, address) as conn:
            yield conn

    @contextlib.contextmanager
    def address_connection(self, service_name, address) -> ContextManager[TProtocolBase]:
        addresses = self._service.addresses(service_name)
        if address not in addresses:
            raise ValueError("address")
        pools = self._service_pools[service_name]
        pool = pools.get(address)
        if not pool:
            host, port = address.split(':')
            pool = ThriftPool(host, int(port), **self._settings)
            pools[address] = pool
        with pool.connection() as conn:
            yield conn

    def _clean_pools(self):
        for service_name, pools in self._service_pools.items():
            available_addresses = self._service.addresses(service_name)
            holding_addresses = set(pools.keys())
            for removed_address in (holding_addresses - available_addresses):
                pool = pools.pop(removed_address)
                pool.close_all()

    def _run(self):
        while True:
            with common.LogSuppress(Exception):
                self._clean_pools()
            gevent.sleep(self._INTERVAL)
