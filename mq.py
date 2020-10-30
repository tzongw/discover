# -*- coding: utf-8 -*-
import gevent
import logging
import uuid
from redis import Redis, ResponseError
from utils import Dispatcher


class MQ(Dispatcher):
    def __init__(self, redis: Redis, group: str, consumer: str = ''):
        super().__init__()
        self._redis = redis
        self._group = group
        self._stopped = False
        self._waker = str(uuid.uuid4())
        self._consumer = consumer or str(uuid.uuid4())

    def start(self):
        @self.handler(self._waker)
        def wakeup(data):
            logging.info(f'{data}')

        for stream in self._handlers:
            try:
                self._redis.xgroup_create(stream, self._group, mkstream=True)
            except ResponseError:
                pass
        gevent.spawn(self._run)

    def stop(self):
        self._redis.xadd(self._waker, {'wake': 'up'})

    def _run(self):
        streams = {stream: '>' for stream in self._handlers}
        while not self._stopped:
            try:
                self._redis.xreadgroup(self._group, self._consumer, streams, noack=True)
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        self._redis.delete(self._waker)
        logging.info(f'stopped successfully')
