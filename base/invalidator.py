# -*- coding: utf-8 -*-
from typing import Union
import uuid
from redis import Redis, RedisCluster
import gevent
import logging
from .executor import Executor
from .dispatcher import Dispatcher


class Invalidator:
    def __init__(self, redis: Union[Redis, RedisCluster], sep=':'):
        self.redis = redis
        self.sep = sep
        self.dispatcher = Dispatcher(executor=Executor(name='invalidator'))

    @property
    def handler(self):
        return self.dispatcher.handler

    @property
    def prefixes(self):
        return self.dispatcher.handlers.keys()

    def start(self):
        gevent.spawn(self._run, self.redis)

    def invalidate(self, key, *args, **kwargs):
        self.dispatcher.dispatch(key, *args, **kwargs)

    def _invalidate_all(self):
        for key in self.prefixes:
            self.dispatcher.dispatch(key, '')

    def publish(self, key):
        self.redis.publish('__redis__:invalidate', key)

    def _run(self, redis, subscribe=True):
        sub = None
        while True:
            try:
                if not sub:
                    sub = redis.pubsub()
                    sub.execute_command('CLIENT ID')
                    client_id = sub.parse_response()
                    prefixes = ' '.join([f'PREFIX {prefix}{self.sep}' for prefix in self.prefixes])
                    command = f'CLIENT TRACKING ON {prefixes} BCAST REDIRECT {client_id}'
                    sub.execute_command(command)
                    res = sub.parse_response()
                    logging.info(f'{command} {res}')
                    sub.subscribe('__redis__:invalidate' if subscribe else str(uuid.uuid4()))
                    res = sub.parse_response()
                    logging.info(res)
                    self._invalidate_all()
                msg = sub.get_message(ignore_subscribe_messages=True, timeout=None)
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
                for key in data:
                    group, key = key.split(self.sep, maxsplit=1)
                    self.dispatcher.dispatch(group, key)
            except Exception:
                logging.exception(f'')
                sub = None
                gevent.sleep(1)
