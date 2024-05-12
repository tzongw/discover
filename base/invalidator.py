# -*- coding: utf-8 -*-
from typing import Union
import uuid
from weakref import WeakValueDictionary
from concurrent.futures import Future
from redis import Redis, RedisCluster
import gevent
import logging
from .executor import Executor
from .dispatcher import Dispatcher


class Invalidator:
    def __init__(self, redis: Union[Redis, RedisCluster], sep=':'):
        self.redis = redis
        self.sep = sep
        self.executor = Executor(name='invalidator')
        self.dispatcher = Dispatcher(self.executor)
        self.futures = WeakValueDictionary()
        self.getters = {}

    def __call__(self, key):
        return self.dispatcher(key)

    def getter(self, group):
        def decorator(f):
            assert self.sep not in group and group not in self.getters
            self.getters[group] = f
            return f

        return decorator

    @property
    def groups(self):
        return self.dispatcher.keys() | self.getters.keys()

    def start(self):
        return [gevent.spawn(self._run, self.redis)]

    def _invalidate_all(self):
        for group in self.groups:
            self.dispatcher.dispatch(group, '')

    def future(self, group, key):
        assert group in self.getters
        full_key = f'{group}{self.sep}{key}'
        fut = self.futures.get(full_key)
        if not fut:
            fut = self.futures[full_key] = Future()
        return fut

    def publish(self, group, key):
        assert self.sep not in group
        full_key = f'{group}{self.sep}{key}'
        self.redis.publish('__redis__:invalidate', full_key)

    def _get_result(self, fut: Future, group, key):
        try:
            getter = self.getters.get(group)
            value = getter(key) if getter else self.redis.get(key)
            fut.set_result(value)
        except Exception as e:
            fut.set_exception(e)
            raise

    def _run(self, redis, subscribe=True):
        sub = None
        while True:
            try:
                if not sub:
                    sub = redis.pubsub()
                    sub.execute_command('CLIENT ID')
                    client_id = sub.parse_response()
                    prefixes = ' '.join([f'PREFIX {group}{self.sep}' for group in self.groups])
                    command = f'CLIENT TRACKING ON {prefixes} BCAST REDIRECT {client_id}'
                    sub.execute_command(command)
                    res = sub.parse_response()
                    logging.info(f'{command} {res}')
                    sub.subscribe('__redis__:invalidate' if subscribe else str(uuid.uuid4()))
                    res = sub.parse_response()
                    logging.info(res)
                    self._invalidate_all()
                msg = sub.get_message(timeout=None)
                if msg is None:
                    continue
                logging.debug(f'got {msg}')
                data = msg['data']
                if data is None:
                    logging.warning(f'db flush all')
                    self._invalidate_all()
                    continue
                if isinstance(data, (bytes, str)):
                    data = [data]
                for full_key in data:
                    group, key = full_key.split(self.sep, maxsplit=1)
                    self.dispatcher.dispatch(group, key)
                    if fut := self.futures.pop(full_key, None):
                        self.executor.submit(self._get_result, fut, group, key)
            except Exception:
                logging.exception(f'')
                sub = None
                gevent.sleep(1)


class InvalidatorCluster(Invalidator):
    def start(self):
        return [gevent.spawn(self.monitor)]

    def monitor(self):
        monitoring = set()
        while True:
            for node in self.redis.get_primaries():
                if node.name in monitoring:
                    continue
                redis = self.redis.get_redis_connection(node)
                first_node = not monitoring
                gevent.spawn(self._run, redis, subscribe=first_node)
                monitoring.add(node.name)
            gevent.sleep(1)


def create_invalidator(redis, sep=':'):
    return InvalidatorCluster(redis, sep) if isinstance(redis, RedisCluster) else Invalidator(redis, sep)
