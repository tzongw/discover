# -*- coding: utf-8 -*-
import gevent
import logging
from typing import Dict
from redis import Redis
from pydantic import BaseModel
from .utils import stream_name, var_args
from .dispatcher import Dispatcher
from .executor import Executor


class Publisher:
    def __init__(self, redis: Redis, *, maxlen=4096, hint=None):
        self.redis = redis
        self.hint = hint
        self.maxlen = maxlen

    def publish(self, message: BaseModel, stream=None):
        stream = stream or stream_name(message)
        params = [stream, 'MAXLEN', '~', self.maxlen]
        if self.hint:
            params += ['HINT', self.hint]
        params += ['*', '', message.json(exclude_defaults=True)]
        return self.redis.execute_command('XADD', *params)


class ProtoDispatcher(Dispatcher):
    def __call__(self, key_or_cls, *, stream=None):
        if not isinstance(key_or_cls, type) or not issubclass(key_or_cls, BaseModel):
            assert stream is None
            return super().__call__(key_or_cls)

        message_cls = key_or_cls
        key = stream or stream_name(message_cls)
        super_handler = super().__call__

        def decorator(f):
            vf = var_args(f)

            @super_handler(key)
            def inner(data: Dict, sid):
                proto = data.get('proto')
                if proto is None:
                    json = data.pop('')
                    proto = message_cls.parse_raw(json)
                    data['proto'] = proto
                vf(proto, sid)

            return f

        return decorator


class Receiver:
    def __init__(self, redis: Redis, group: str, consumer: str, workers=32, dispatcher=ProtoDispatcher):
        self.redis = redis
        self._group = group
        self._consumer = consumer
        self._waker = f'waker:{self._group}:{self._consumer}'
        self._stopped = True
        self._workers = workers
        self._dispatcher = dispatcher(executor=Executor(max_workers=workers, queue_size=1, name='receiver'))

        @self._dispatcher(self._waker)
        def _wakeup(data, sid):
            logging.info(f'{sid} {data}')

    def __call__(self, key_or_cls, *, stream=None):
        return self._dispatcher(key_or_cls, stream=stream)

    def start(self):
        logging.info(f'start {self._group} {self._consumer}')
        self._stopped = False
        streams = self._dispatcher.keys()
        with self.redis.pipeline(transaction=False) as pipe:
            for stream in streams:
                # create group & stream
                pipe.xgroup_create(stream, self._group, mkstream=True)
            pipe.execute(raise_on_error=False)  # group already exists
        return [gevent.spawn(self._run, streams)]

    def stop(self):
        if self._stopped:
            return
        logging.info(f'stop {self._group} {self._consumer}')
        self._stopped = True
        streams = self._dispatcher.keys()
        with self.redis.pipeline(transaction=False) as pipe:
            pipe.xadd(self._waker, {'wake': 'up'})
            for stream in streams:
                pipe.xgroup_delconsumer(stream, self._group, self._consumer)
            pipe.delete(self._waker)
            pipe.execute()

    def _run(self, streams):
        streams = {stream: '>' for stream in streams}
        count = self._workers * 2
        while not self._stopped:
            try:
                result = self.redis.xreadgroup(self._group, self._consumer, streams, count=count, block=0, noack=True)
                for stream, messages in result:
                    for message in messages:
                        self._dispatcher.dispatch(stream, *message[::-1])
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        logging.info(f'receiver exit {streams.keys()}')
