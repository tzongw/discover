# -*- coding: utf-8 -*-
from redis import Redis
import gevent
import logging
from .executor import Executor
from .utils import Dispatcher


class Invalidator:
    def __init__(self, redis: Redis, sep=':'):
        self.redis = redis
        self.sep = sep
        self.dispatcher = Dispatcher(sep=sep, executor=Executor(name='invalidator'))

    @property
    def handler(self):
        return self.dispatcher.handler

    @property
    def prefixes(self):
        return self.dispatcher.handlers.keys()

    def start(self):
        gevent.spawn(self._run)

    def invalidate(self, key, publish=False):
        if publish:
            self.redis.publish('__redis__:invalidate', key)
        else:
            self.dispatcher.dispatch(key, key)

    def invalidate_all(self):
        for prefix in self.prefixes:
            self.dispatcher.dispatch(prefix, '')

    def _run(self):
        sub = None
        while True:
            try:
                if not sub:
                    sub = self.redis.pubsub()
                    sub.execute_command('CLIENT ID')
                    client_id = sub.parse_response()
                    prefixes = ' '.join([f'PREFIX {prefix}{self.sep}' for prefix in self.prefixes])
                    command = f'CLIENT TRACKING ON {prefixes} BCAST REDIRECT {client_id}'
                    sub.execute_command(command)
                    res = sub.parse_response()
                    logging.info(f'{command} {res}')
                    sub.subscribe('__redis__:invalidate')
                    res = sub.parse_response()
                    logging.info(res)
                    self.invalidate_all()
                msg = sub.get_message(ignore_subscribe_messages=True, timeout=None)
                if msg is None:
                    continue
                logging.debug(f'got {msg}')
                data = msg['data']
                if data is None:
                    logging.warning(f'db flush all')
                    self.invalidate_all()
                    continue
                if isinstance(data, str):
                    data = [data]
                for key in data:
                    self.dispatcher.dispatch(key, key)
            except Exception:
                logging.exception(f'')
                sub = None
                gevent.sleep(1)
