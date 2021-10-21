# -*- coding: utf-8 -*-
from redis import Redis
import gevent
import logging
from .utils import Dispatcher


class Invalidator:
    def __init__(self, redis: Redis, sep=':'):
        self.redis = redis
        self.sub = None
        self.dispatcher = Dispatcher(sep=sep)

    @property
    def handler(self):
        return self.dispatcher.handler

    @property
    def prefixes(self):
        return self.dispatcher.handlers.keys()

    def start(self):
        gevent.spawn(self._run)

    def publish(self, key):
        self.redis.publish('__redis__:invalidate', key)

    def _invalidate_all(self):
        for prefix in self.prefixes:
            self.dispatcher.dispatch(prefix, '')

    def _run(self):
        while True:
            try:
                if not self.sub:
                    self.sub = self.redis.pubsub()
                    self.sub.execute_command('CLIENT ID')
                    client_id = self.sub.parse_response()
                    prefixes = ' '.join([f'PREFIX {prefix}' for prefix in self.prefixes])
                    command = f'CLIENT TRACKING ON {prefixes} BCAST REDIRECT {client_id}'
                    self.sub.execute_command(command)
                    res = self.sub.parse_response()
                    logging.info(f'TRACKING ON {client_id} {res}')
                    self.sub.subscribe('__redis__:invalidate')
                    self._invalidate_all()
                msg = self.sub.get_message(ignore_subscribe_messages=True, timeout=None)
                if msg is None:
                    continue
                logging.debug(f'got {msg}')
                data = msg['data']
                if data is None:
                    logging.warning(f'db flush all')
                    self._invalidate_all()
                    continue
                if isinstance(data, str):
                    data = [data]
                for key in data:
                    self.dispatcher.dispatch(key, key)
            except Exception:
                logging.exception(f'')
                self.sub = None
                gevent.sleep(1)
