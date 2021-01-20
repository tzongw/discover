# -*- coding: utf-8 -*-
from functools import partial
import contextlib
from typing import ContextManager
from base.service_pools import ServicePools
from base.thrift_pool import ThriftPool
from base.utils import LogSuppress
from service import gate, user, timer
import logging


class Selector:
    @staticmethod
    def _one_shot(client_factory, name, *args, **kwargs):
        with client_factory() as client:
            return getattr(client, name)(*args, **kwargs)

    @staticmethod
    def _retry(client_factory, name, *args, **kwargs):
        try:
            return Selector._one_shot(client_factory, name, *args, **kwargs)
        except Exception as e:
            if ThriftPool.acceptable(e):
                raise
        # will retry another node
        logging.warning(f'retry {name} {args} {kwargs}')
        return Selector._one_shot(client_factory, name, *args, **kwargs)

    @staticmethod
    def _traverse(address_client_factory, addresses, name, *args, **kwargs):
        for address in addresses:
            with LogSuppress(Exception):
                with address_client_factory(address) as client:
                    getattr(client, name)(*args, **kwargs)


class UserService(ServicePools, Selector):
    @contextlib.contextmanager
    def client(self) -> ContextManager[user.Iface]:
        with self.connection() as conn:
            yield user.Client(conn)

    def __getattr__(self, name):
        if hasattr(user.Iface, name):
            return partial(self._one_shot, self.client, name)
        return super().__getattr__(name)


class GateService(ServicePools, Selector):
    @contextlib.contextmanager
    def client(self, address) -> ContextManager[gate.Iface]:
        with self.address_connection(address) as conn:
            yield gate.Client(conn)

    def __getattr__(self, name):
        if hasattr(gate.Iface, name):
            addresses = self.addresses()
            return partial(self._traverse, self.client, addresses, name)
        return super().__getattr__(name)


class TimerService(ServicePools, Selector):
    @contextlib.contextmanager
    def client(self) -> ContextManager[timer.Iface]:
        with self.connection() as conn:
            yield timer.Client(conn)

    def __getattr__(self, name):
        if hasattr(timer.Iface, name):
            return partial(self._one_shot, self.client, name)
        return super().__getattr__(name)
