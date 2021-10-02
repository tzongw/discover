# -*- coding: utf-8 -*-
from redis import Redis
import gevent
import logging
from .utils import Dispatcher


class Invalidator:
    def __init__(self, redis: Redis):
        self.redis = redis
        self.sub = None
        self.dispatcher = Dispatcher()

    @property
    def handler(self):
        return self.dispatcher.handler

    @property
    def prefixes(self):
        return self.dispatcher.handlers.keys()

    def start(self):
        gevent.spawn(self._run)

    def _run(self):
        while True:
            try:
                if not self.sub:
                    self.sub = self.redis.pubsub()
                    self.sub.execute_command('CLIENT ID')
                    client_id = self.sub.parse_response()
                    prefixes = ' '.join([f'PREFIX {prefix}' for prefix in self.prefixes])
                    command = f'CLIENT TRACKING ON {prefixes} BCAST REDIRECT {client_id}'
                    self.redis.execute_command(command)
                    self.sub.subscribe('__redis__:invalidate')
                    for prefix in self.prefixes:
                        self.dispatcher.dispatch(prefix, '')  # invalidate all
                msg = self.sub.get_message(ignore_subscribe_messages=True, timeout=None)
                if msg is not None:
                    logging.debug(f'got {msg}')
                    for key in msg['data']:
                        for prefix in self.prefixes:
                            if key.startswith(prefix):
                                self.dispatcher.dispatch(prefix, key)
            except Exception:
                logging.exception(f'')
                self.sub = None
                gevent.sleep(1)
