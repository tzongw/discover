# -*- coding: utf-8 -*-
import uuid
import logging
from typing import Union
from weakref import WeakValueDictionary, WeakKeyDictionary
from gevent.event import AsyncResult as Future
import gevent
from redis import Redis, RedisCluster
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
        self.conns = WeakKeyDictionary()
        self.bcast_groups = set()

    def __call__(self, group, bcast=True):
        if bcast:
            self.bcast_groups.add(group)
        return self.dispatcher(group)

    def getter(self, group, bcast=True):
        def decorator(f):
            assert self.sep not in group and group not in self.getters
            self.getters[group] = f
            return f

        if bcast:
            self.bcast_groups.add(group)
        return decorator

    @property
    def groups(self):
        return self.dispatcher.keys() | self.getters.keys()

    def start(self):
        return [gevent.spawn(self._run, self.redis)]

    def publish(self, group, *keys):
        if isinstance(self.redis, RedisCluster):
            node = self.redis.get_node_from_key(keys[0])
            redis = self.redis.get_redis_connection(node)
        else:
            redis = self.redis
        with redis.pipeline(transaction=False) as pipe:
            for key in keys:
                full_key = self.full_key(group, key)
                pipe.publish('__redis__:invalidate', full_key)
            pipe.execute()

    def future(self, group, key):
        assert group in self.getters
        full_key = self.full_key(group, key)
        fut = self.futures.get(full_key)
        if not fut:
            fut = self.futures[full_key] = Future()
        return fut

    def full_key(self, group, key):
        assert self.sep not in group
        return f'{group}{self.sep}{key}'

    def _get_result(self, fut: Future, full_key, group):
        try:
            value = self.getters[group](full_key)
            fut.set_result(value)
        except Exception as e:
            fut.set_exception(e)
            raise

    def _invalidate_all(self):
        for group in self.groups:
            self.dispatcher.dispatch(group, '')

    def _run(self, redis: Redis, subscribe=True):
        def get_connection(*args, **kwargs):
            conn = origin_get_connection(*args, **kwargs)
            if client_id is not None and self.conns.get(conn) != client_id:
                redirect_to = client_id  # may change, save snapshot
                command = f'CLIENT TRACKING ON REDIRECT {redirect_to}'
                conn.send_command(command)
                res = conn.read_response()
                prefixes = ' '.join(f'PREFIX {group}{self.sep}' for group in tracking_groups)
                logging.info(f'{command} {prefixes} {res}')
                self.conns[conn] = redirect_to
            return conn

        pubsub = None
        client_id = None
        tracking_groups = [group for group in self.groups if group not in self.bcast_groups]
        if tracking_groups:
            pool = redis.connection_pool
            origin_get_connection = pool.get_connection
            pool.get_connection = get_connection
        while True:
            try:
                if not pubsub:
                    pubsub = redis.pubsub()
                    pubsub.execute_command('CLIENT ID')
                    cid = pubsub.parse_response()
                    if self.bcast_groups:
                        prefixes = ' '.join([f'PREFIX {group}{self.sep}' for group in self.bcast_groups])
                        command = f'CLIENT TRACKING ON {prefixes} BCAST REDIRECT {cid}'
                        pubsub.execute_command(command)
                        res = pubsub.parse_response()
                        logging.info(f'{command} {res}')
                    pubsub.subscribe('__redis__:invalidate' if subscribe else str(uuid.uuid4()))
                    res = pubsub.parse_response()
                    logging.info(res)
                    client_id = cid  # can receive push message now
                    self._invalidate_all()
                msg = pubsub.get_message(timeout=None)
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
                        self.executor.submit(self._get_result, fut, full_key, group)
            except Exception:
                logging.exception(f'')
                if pubsub:
                    pubsub.reset()
                    pubsub = None
                    client_id = None
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
