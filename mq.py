# -*- coding: utf-8 -*-
import gevent
import logging
from redis import Redis, ResponseError
from utils import Dispatcher


class MQ:
    def __init__(self, redis: Redis, group: str, consumer: str):
        super().__init__()
        self._redis = redis
        self._group = group
        self._consumer = consumer
        self._waker = f'waker:{self._group}:{self._consumer}'
        self._stopped = False
        self._group_dispatcher = Dispatcher()
        self._fanout_dispatcher = Dispatcher()
        self.group_handler = self._group_dispatcher.handler
        self.fanout_handler = self._fanout_dispatcher.handler

    def start(self):
        @self.group_handler(self._waker)
        def group_wakeup(id, data):
            logging.info(f'{id} {data}')

        @self.fanout_handler(self._waker)
        def fanout_wakeup(id, data):
            logging.info(f'{id} {data}')

        for stream in self._group_dispatcher.handlers:
            try:
                self._redis.xgroup_create(stream, self._group, mkstream=True)
            except ResponseError as e:
                logging.info(f'group already exists: {e}')
        gevent.spawn(self._group_run)

    def stop(self):
        self._stopped = True
        self._redis.xadd(self._waker, {'wake': 'up'})

    def _group_run(self):
        streams = {stream: '>' for stream in self._group_dispatcher.handlers}
        while not self._stopped:
            try:
                result = self._redis.xreadgroup(self._group, self._consumer, streams, block=60, noack=True)
                for stream, messages in result:
                    for message in messages:
                        self._group_dispatcher.dispatch(stream, *message)
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        with self._redis.pipeline(transaction=False) as pipe:
            for stream in self._group_dispatcher.handlers:
                pipe.xgroup_delconsumer(stream, self._group, self._consumer)
            pipe.delete(self._waker)
            pipe.execute()
        logging.info(f'delete {self._waker}')

    def _fanout_run(self):
        streams = {stream: '$' for stream in self._fanout_dispatcher.handlers}
        while not self._stopped:
            try:
                result = self._redis.xread(streams, block=60)
                for stream, messages in result:
                    for message in messages:
                        self._fanout_dispatcher.dispatch(stream, *message)
                        streams[stream] = message[0]  # update last id
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        logging.info(f'fanout exit')
