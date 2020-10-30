# -*- coding: utf-8 -*-
import gevent
import logging
from redis import Redis, ResponseError
from utils import Dispatcher


class MQ(Dispatcher):
    def __init__(self, redis: Redis, group: str, consumer: str):
        super().__init__()
        self._redis = redis
        self._group = group
        self._consumer = consumer
        self._waker = f'waker:{self._group}:{self._consumer}'
        self._stopped = False

    def start(self):
        @self.handler(self._waker)
        def wakeup(id, data):
            logging.info(f'{id} {data}')

        for stream in self._handlers:
            try:
                self._redis.xgroup_create(stream, self._group, mkstream=True)
            except ResponseError:
                pass  # group already exists
        gevent.spawn(self._run)

    def stop(self):
        self._stopped = True
        self._redis.xadd(self._waker, {'wake': 'up'})

    def _run(self):
        streams = {stream: '>' for stream in self._handlers}
        while not self._stopped:
            try:
                result = self._redis.xreadgroup(self._group, self._consumer, streams, noack=True)
                for stream, messages in result:
                    for message in messages:
                        self.dispatch(stream, *message)
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        with self._redis.pipeline(transaction=False) as pipe:
            for stream in self._handlers:
                pipe.xgroup_delconsumer(stream, self._group, self._consumer)
            pipe.delete(self._waker)
            pipe.execute()
        logging.info(f'delete {self._waker}')
