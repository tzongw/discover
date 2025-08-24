import contextlib
import logging
import time
from binascii import crc32
from random import choice
from typing import Dict, ContextManager
import gevent
from thrift.protocol.TProtocol import TProtocolBase
from .registry import Registry
from .thrift_pool import ThriftPool
from .utils import Addr, DefaultDict


class Service:
    def __init__(self, registry: Registry, name, host, **settings):
        self._name = name
        self._local_host = host
        self._registry = registry
        self._pools = DefaultDict(lambda address: ThriftPool(Addr(address), **settings))  # type: Dict[str, ThriftPool]
        self._cooldown = {}  # type: Dict[str, float]
        self._healthy_addresses = []  # addresses not in cooldown
        self._local_addresses = []  # healthy addresses with same host
        registry.add_callback(self._update_addresses)
        self._update_addresses()
        self._reaping = False

    def addresses(self):
        return self._registry.addresses(self._name)

    def address(self, hint: str):
        addresses = self._local_addresses or self._healthy_addresses
        return addresses[crc32(hint.encode()) % len(addresses)]

    @contextlib.contextmanager
    def connection(self, address=None) -> ContextManager[TProtocolBase]:
        if address is None:
            addresses = self._local_addresses or self._healthy_addresses
            address = choice(addresses)
        else:
            if address in self._cooldown and address not in self.addresses():
                raise ValueError(f'cooling down unhealthy address {address}')
        pool = self._pools[address]
        try:
            with pool.connection() as conn:
                yield conn
        except Exception as e:
            if not pool.biz_exception(e):
                exists = address in self._cooldown
                self._cooldown[address] = time.monotonic() + Registry.COOLDOWN
                if not exists:
                    logging.error(f'+ cool down {self._name} {address}')
                    interval = self._update_addresses()
                    if not self._reaping:
                        self._reaping = True
                        gevent.spawn_later(interval, self._reap_cooldown)
            raise

    def _clean_pools(self):
        available = self.addresses()
        holding = set(self._pools.keys())
        for removing in holding - available:
            logging.info(f'clean {self._name} {removing}')
            pool = self._pools.pop(removing)
            pool.close_all()

    def _update_addresses(self):
        self._clean_pools()
        now = time.monotonic()
        expired = [addr for addr, cd in self._cooldown.items() if cd <= now]
        if expired:
            logging.info(f'- cool down {self._name} {expired}')
        for addr in expired:
            self._cooldown.pop(addr)
        addresses = sorted(self.addresses())
        self._healthy_addresses = [addr for addr in addresses if addr not in self._cooldown]
        self._local_addresses = [addr for addr in self._healthy_addresses if Addr(addr).host == self._local_host]
        return min(self._cooldown.values()) - now if self._cooldown else 0  # next expire interval

    def _reap_cooldown(self):
        while interval := self._update_addresses():
            gevent.sleep(interval)
        self._reaping = False
