# -*- coding: utf-8 -*-
import gevent
import logging
import uuid
from typing import Type, Dict
from redis import Redis
from .utils import Dispatcher
from google.protobuf.message import Message
from google.protobuf.json_format import Parse, MessageToJson


class Publisher:
    def __init__(self, redis: Redis):
        self._redis = redis

    def publish(self, message: Message, maxlen=4096):
        # noinspection PyUnresolvedReferences
        stream = message.stream
        json = MessageToJson(message)
        return self._redis.xadd(stream, {'': json}, maxlen=maxlen)


class ProtoDispatcher(Dispatcher):
    def handler(self, key_or_cls):
        if isinstance(key_or_cls, str):
            return super().handler(key_or_cls)

        assert issubclass(key_or_cls, Message)
        message_cls = key_or_cls  # type: Type[Message]
        # noinspection PyUnresolvedReferences
        key = message_cls().stream
        super_handler = super().handler

        def decorator(f):
            @super_handler(key)
            def inner(id, data: Dict):
                json = data.pop('')
                proto = Parse(json, message_cls(), ignore_unknown_fields=True)
                f(id, proto)

            return f

        return decorator


class Receiver:
    def __init__(self, redis: Redis, group: str, consumer: str):
        super().__init__()
        self._redis = redis
        self._group = group
        self._consumer = consumer
        self._waker = f'waker:{self._group}:{self._consumer}'
        self._stopped = False
        self._group_dispatcher = ProtoDispatcher()
        self._fanout_dispatcher = ProtoDispatcher()
        self.group_handler = self._group_dispatcher.handler
        self.fanout_handler = self._fanout_dispatcher.handler

    def start(self):
        @self.group_handler(self._waker)
        def group_wakeup(id, data):
            logging.info(f'{id} {data}')

        @self.fanout_handler(self._waker)
        def fanout_wakeup(id, data):
            logging.info(f'{id} {data}')

        with self._redis.pipeline() as pipe:
            for stream in self._group_dispatcher.handlers:
                pipe.xgroup_create(stream, self._group, mkstream=True)
            unique_group = str(uuid.uuid4())
            for stream in self._fanout_dispatcher.handlers:
                # create empty stream if not exist
                pipe.xgroup_create(stream, unique_group, mkstream=True)
                pipe.xgroup_destroy(stream, unique_group)
            pipe.execute(raise_on_error=False)
        gevent.spawn(self._group_run)
        gevent.spawn(self._fanout_run)

    def stop(self):
        self._stopped = True
        self._redis.xadd(self._waker, {'wake': 'up'})

    def _group_run(self):
        streams = {stream: '>' for stream in self._group_dispatcher.handlers}
        while not self._stopped:
            try:
                result = self._redis.xreadgroup(self._group, self._consumer, streams, count=10, block=0, noack=True)
                for stream, messages in result:
                    for message in messages:
                        self._group_dispatcher.dispatch(stream, *message)
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        with self._redis.pipeline() as pipe:
            for stream in self._group_dispatcher.handlers:
                pipe.xgroup_delconsumer(stream, self._group, self._consumer)
            pipe.delete(self._waker)
            pipe.execute()
        logging.info(f'delete {self._waker}')

    def _fanout_run(self):
        with self._redis.pipeline() as pipe:
            stream_names = self._fanout_dispatcher.handlers.keys()
            for stream in stream_names:
                pipe.xinfo_stream(stream)
            last_ids = [xinfo['last-generated-id'] for xinfo in pipe.execute()]
        # race happen if use $ as last id
        # 1. xread stream1 stream2 $ $
        # 2. while handling stream1 message1 id1, xadd stream2 message2 id2
        # 3. xread stream1 id1 stream2 $, message2 is missing
        streams = dict(zip(stream_names, last_ids))
        while not self._stopped:
            try:
                result = self._redis.xread(streams, count=10, block=0)
                for stream, messages in result:
                    for message in messages:
                        self._fanout_dispatcher.dispatch(stream, *message)
                        streams[stream] = message[0]  # update last id
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)
        logging.info(f'fanout exit')
