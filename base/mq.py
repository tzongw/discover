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
    def __init__(self, redis: Redis, hint=None):
        self.redis = redis
        self.hint = hint

    def publish(self, message: BaseModel, maxlen=4096, do_hint=True, stream=None):
        stream = stream or stream_name(message)
        params = [stream, 'MAXLEN', '~', maxlen]
        if do_hint and self.hint:
            params += ['HINT', self.hint]
        params += ['*', '', message.json(exclude_defaults=True)]
        return self.redis.execute_command('XADD', *params)


class ProtoDispatcher(Dispatcher):
    def handler(self, key_or_cls, stream=None):
        if not isinstance(key_or_cls, type) or not issubclass(key_or_cls, BaseModel):
            return super().handler(key_or_cls)

        message_cls = key_or_cls
        key = stream or stream_name(message_cls)
        super_handler = super().handler

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
    def __init__(self, redis: Redis, group: str, consumer: str, batch=10, dispatcher=ProtoDispatcher):
        self.redis = redis
        self._group = group
        self._consumer = consumer
        self._waker = f'waker:{self._group}:{self._consumer}'
        self._stopped = False
        self._group_dispatcher = dispatcher(executor=Executor(max_workers=batch, queue_size=0, name='group_dispatch'))
        self._fanout_dispatcher = dispatcher(executor=Executor(max_workers=batch, queue_size=0, name='fanout_dispatch'))
        self._batch = batch

        @self.group(self._waker)
        def group_wakeup(data, sid):
            logging.info(f'{sid} {data}')

        @self.fanout(self._waker)
        def fanout_wakeup(data, sid):
            logging.info(f'{sid} {data}')

    @property
    def group(self):
        return self._group_dispatcher.handler

    @property
    def fanout(self):
        return self._fanout_dispatcher.handler

    def start(self):
        with self.redis.pipeline() as pipe:
            streams = set(self._group_dispatcher.handlers) | set(self._fanout_dispatcher.handlers)
            for stream in streams:
                # create group & stream
                pipe.xgroup_create(stream, self._group, mkstream=True)
            pipe.execute(raise_on_error=False)  # group already exists
        gevent.spawn(self._group_run, self._group_dispatcher.handlers)
        gevent.spawn(self._fanout_run, self._fanout_dispatcher.handlers)

    def stop(self):
        logging.info(f'stop')
        self._stopped = True
        self.redis.xadd(self._waker, {'wake': 'up'})
        with self.redis.pipeline() as pipe:
            for stream in self._group_dispatcher.handlers:
                pipe.xgroup_delconsumer(stream, self._group, self._consumer)
            pipe.delete(self._waker)
            pipe.execute(raise_on_error=False)  # stop but no start
        logging.info(f'delete waker {self._waker}')

    def _group_run(self, streams):
        streams = {stream: '>' for stream in streams}
        while not self._stopped:
            try:
                result = self.redis.xreadgroup(self._group, self._consumer, streams, count=self._batch,
                                               block=0,
                                               noack=True)
                for stream, messages in result:
                    for message in messages:
                        self._group_dispatcher.dispatch(stream, *message[::-1])
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        logging.info(f'group exit {streams.keys()}')

    def _fanout_run(self, streams):
        with self.redis.pipeline() as pipe:
            for stream in streams:
                pipe.xinfo_stream(stream)
            last_ids = [xinfo['last-generated-id'] for xinfo in pipe.execute()]
        # race happen if use $ as last id
        # 1. xread stream1 stream2 $ $
        # 2. xadd stream1 message1
        # 3. while handling message1, xadd stream2 message2
        # 4. xread stream1 stream2 $ $, message2 is missing
        streams = dict(zip(streams, last_ids))
        while not self._stopped:
            try:
                result = self.redis.xread(streams, count=self._batch, block=0)
                for stream, messages in result:
                    for message in messages:
                        self._fanout_dispatcher.dispatch(stream, *message[::-1])
                        streams[stream] = message[0]  # update last id
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        logging.info(f'fanout exit {streams.keys()}')
