import contextlib
from random import choice
from typing import Dict, ContextManager, Union
from thrift.protocol.TProtocol import TProtocolBase
from .registry import Registry
from .thrift_pool import ThriftPool
import logging
import time
from .utils import ip_address, Addr


class ServicePools:
    def __init__(self, registry: Registry, name, **settings):
        self._name = name
        self._registry = registry
        self._pools = {}  # type: Dict[str, ThriftPool]
        self._cool_down = {}  # type: Dict[str, float]
        self._settings = settings
        registry.add_callback(self._clean_pools)

    def addresses(self):
        return self._registry.addresses(self._name)

    @contextlib.contextmanager
    def connection(self) -> ContextManager[TProtocolBase]:
        addresses = self.addresses()
        now = time.time()
        good_ones = [addr for addr in addresses if now > self._cool_down.get(addr, 0)]
        local_host = ip_address()
        local_ones = [addr for addr in good_ones if addr.host == local_host]
        address = choice(local_ones or good_ones or tuple(addresses))  # type: str
        with self.address_connection(address) as conn:
            yield conn

    @contextlib.contextmanager
    def address_connection(self, address: str) -> ContextManager[TProtocolBase]:
        addresses = self.addresses()
        if address not in addresses:
            raise ValueError(f"{self._name} {address} {addresses}")
        pool = self._pools.get(address)
        if not pool:
            addr = Addr(address)
            pool = ThriftPool(addr.host, addr.port, **self._settings)
            self._pools[address] = pool
        with pool.connection() as conn:
            try:
                yield conn
            except Exception as e:
                if not ThriftPool.acceptable(e):
                    self._cool_down[address] = time.time() + Registry.COOL_DOWN
                raise

    def _clean_pools(self):
        available = self.addresses()
        holding = set(self._pools.keys())
        for removed in (holding - available):
            logging.info(f'clean {self._name} {removed}')
            self._cool_down.pop(removed, None)
            pool = self._pools.pop(removed)
            pool.close_all()
