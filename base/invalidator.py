# -*- coding: utf-8 -*-
from redis import Redis
import gevent
import logging
from utils import Dispatcher


class Invalidator:
    def __init__(self, redis: Redis):
        self.redis = redis
        self.sub = None
        self.prefixes = []
        self.dispatcher = Dispatcher()

    @property
    def dispatch(self):
        return self.dispatcher.dispatch

    def start(self, prefixes):
        self.prefixes = prefixes
        gevent.spawn(self._run)

    def _run(self):
        while True:
            try:
                if not self.sub:
                    self.sub = self.redis.pubsub()
                    self.sub.subscribe('__redis__:invalidate')
                    self.sub.execute_command('CLIENT ID')
                    client_id = self.sub.parse_response()
                    prefixes = [f' PREFIX {prefix} ' for prefix in self.prefixes]
                    command = f'CLIENT TRACKING ON {prefixes} BCAST REDIRECT {client_id}'
                    self.redis.execute_command(command)
                    for prefix in self.prefixes:
                        self.dispatcher.dispatch(prefix, '')  # invalidate all
                msg = self.sub.get_message(ignore_subscribe_messages=True, timeout=10)
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
