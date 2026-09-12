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
        self._closing = {}  # type: Dict[str, float]
        self._healthy_addresses = []  # addresses not in cooldown
        self._local_addresses = []  # healthy addresses with same host
        registry.add_callback(self._update_addresses)
        self._update_addresses()
        gevent.spawn(self._reap_expired)

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
                self._cooldown[address] = time.time() + Registry.COOLDOWN
                if not exists:
                    logging.info(f'+ cool down {self._name} {address}')
                    self._update_addresses()
            raise

    def _update_addresses(self):
        now = time.time()
        expired = [addr for addr, at in self._cooldown.items() if at <= now]
        if expired:
            logging.info(f'- cool down {self._name} {expired}')
        for addr in expired:
            self._cooldown.pop(addr)
        addresses = sorted(self.addresses())
        self._healthy_addresses = [addr for addr in addresses if addr not in self._cooldown]
        self._local_addresses = [addr for addr in self._healthy_addresses if Addr(addr).host == self._local_host]
        available = set(addresses)
        for addr in available & self._closing.keys():
            self._closing.pop(addr)
            logging.info(f'- closing {self._name} {addr}')
        for addr in self._pools.keys() - available - self._closing.keys():
            logging.info(f'+ closing {self._name} {addr}')
            self._closing[addr] = now + Registry.COOLDOWN
        expired = [addr for addr, at in self._closing.items() if at <= now]
        for addr in expired:
            self._closing.pop(addr)
            logging.info(f'close {self._name} {addr}')
            pool = self._pools.pop(addr)
            pool.close()

    def _reap_expired(self):
        while True:
            try:
                if self._cooldown or self._closing:
                    self._update_addresses()
            except Exception:
                logging.exception(f'reap error {self._name}')
            gevent.sleep(1)
