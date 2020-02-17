# -*- coding: utf-8 -*-
from functools import partial
import contextlib
from typing import ContextManager
from service_pools import ServicePools
from thrift_pool import ThriftPool
from utils import LogSuppress
from generated.service import gate, user


class Selector:
    @staticmethod
    def _one_shot(client_factory, item, *args, **kwargs):
        with client_factory() as client:
            return getattr(client, item)(*args, **kwargs)

    @staticmethod
    def _retry(client_factory, item, *args, **kwargs):
        try:
            return Selector._one_shot(client_factory, item, *args, **kwargs)
        except Exception as e:
            if ThriftPool.acceptable(e):
                raise
        # will retry another node
        return Selector._one_shot(client_factory, item, *args, **kwargs)

    @staticmethod
    def _traverse(address_client_factory, addresses, item, *args, **kwargs):
        for address in addresses:
            with LogSuppress(Exception):
                with address_client_factory(address) as client:
                    getattr(client, item)(*args, **kwargs)


class UserService(ServicePools, Selector):
    @contextlib.contextmanager
    def client(self) -> ContextManager[user.Iface]:
        with self.connection() as conn:
            yield user.Client(conn)

    def __getattr__(self, item):
        if hasattr(user.Iface, item):
            return partial(self._one_shot, self.client, item)
        return super().__getattr__(item)


class GateService(ServicePools, Selector):
    @contextlib.contextmanager
    def client(self, address) -> ContextManager[gate.Iface]:
        with self.address_connection(address) as conn:
            yield gate.Client(conn)

    def __getattr__(self, item):
        if hasattr(gate.Iface, item):
            addresses = self.addresses()
            return partial(self._traverse, self.client, addresses, item)
        return super().__getattr__(item)
