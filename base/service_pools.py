import contextlib
from random import choice
from typing import Dict, ContextManager
import gevent
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
        self._cooldown = {}  # type: Dict[str, float]
        self._settings = settings
        self._local_addresses = []
        self._good_addresses = []
        self._all_addresses = []  # same as addresses, but as a list
        registry.add_callback(self._clean_pools)

    def addresses(self):
        return self._registry.addresses(self._name)

    def address(self, hint):
        addresses = self._local_addresses or self._good_addresses or self._all_addresses
        return addresses[hash(hint) % len(addresses)]

    @contextlib.contextmanager
    def connection(self, address=None) -> ContextManager[TProtocolBase]:
        if address is None:
            addresses = self._local_addresses or self._good_addresses or self._all_addresses
            address = choice(addresses)

        pool = self._pools.get(address)
        if not pool:
            addr = Addr(address)
            pool = ThriftPool(addr.host, addr.port, **self._settings)
            self._pools[address] = pool
        with pool.connection() as conn:
            try:
                yield conn
            except Exception as e:
                if not ThriftPool.biz_exception(e):
                    count = len(self._cooldown)
                    self._cooldown[address] = time.time() + Registry.COOLDOWN
                    if len(self._cooldown) > count:
                        logging.warning(f'+ cool down {self._name} {address}')
                        if not count:
                            gevent.spawn(self._clean_cooldown)
                        else:
                            self._update_addresses()
                raise

    def _clean_pools(self):
        available = self.addresses()
        holding = set(self._pools.keys())
        for removed in (holding - available):
            logging.info(f'clean {self._name} {removed}')
            pool = self._pools.pop(removed)
            pool.close_all()
        self._update_addresses()

    def _update_addresses(self):
        addresses = self._all_addresses = sorted(self.addresses())
        now = time.time()
        expires = [addr for addr, cd in self._cooldown.items() if now >= cd]
        if expires:
            logging.info(f'- cool down {self._name} {expires}')
        for addr in expires:
            self._cooldown.pop(addr)
        self._good_addresses = [addr for addr in addresses if addr not in self._cooldown]
        local_host = ip_address()
        self._local_addresses = [addr for addr in self._good_addresses if Addr(addr).host == local_host]
        return min(self._cooldown.values()) - now if self._cooldown else 0  # next expire

    def _clean_cooldown(self):
        while expire := self._update_addresses():
            gevent.sleep(expire)
