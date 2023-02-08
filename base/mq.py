# -*- coding: utf-8 -*-
import gevent
import logging
import uuid
from typing import Dict
from redis import Redis

from common.shared import clustered, all_clustered
from .utils import stream_name, var_args
from .dispatcher import Dispatcher
from .executor import Executor
from pydantic import BaseModel


class Publisher:
    def __init__(self, redis: Redis, hint=None):
        self.redis = redis
        self.hint = hint

    def publish(self, message: BaseModel, maxlen=4096, do_hint=True, stream=None):
        stream = stream or stream_name(message)
        stream = clustered(stream)
        params = [stream, 'MAXLEN', '~', maxlen]
        if do_hint and self.hint:
            params += ['HINT', self.hint]
        params += ['*', '', message.json()]
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
    def __init__(self, redis: Redis, group: str, consumer: str, batch=10):
        self.redis = redis
        self._group = group
        self._consumer = consumer
        self._wakers = all_clustered(f'waker:{self._group}:{self._consumer}')
        self._stopped = False
        self._group_dispatcher = ProtoDispatcher(
            executor=Executor(max_workers=batch, queue_size=0, name='group_dispatch'))
        self._fanout_dispatcher = ProtoDispatcher(executor=Executor(max_workers=batch, queue_size=0,
                                                                    name='fanout_dispatch'))
        self._batch = batch
        self._workers = []

    @property
    def group(self):
        return self._group_dispatcher.handler

    @property
    def fanout(self):
        return self._fanout_dispatcher.handler

    def start(self):
        for waker in self._wakers:
            @self.group(waker)
            def group_wakeup(data, sid):
                logging.info(f'{sid} {data}')

            @self.fanout(waker)
            def fanout_wakeup(data, sid):
                logging.info(f'{sid} {data}')

        with self.redis.pipeline(transaction=False) as pipe:
            for stream in self._group_dispatcher.handlers:
                for name in all_clustered(stream):
                    pipe.xgroup_create(name, self._group, mkstream=True)
            unique_group = str(uuid.uuid4())
            for stream in self._fanout_dispatcher.handlers:
                # create empty stream if not exist
                for name in all_clustered(stream):
                    pipe.xgroup_create(name, unique_group, mkstream=True)
                    pipe.xgroup_destroy(name, unique_group)
            pipe.execute(raise_on_error=False)
        self._group_run()
        self._fanout_run()

    def stop(self):
        logging.info(f'stop')
        self._stopped = True
        with self.redis.pipeline(transaction=False) as pipe:
            for waker in self._wakers:
                pipe.xadd(waker, {'wake': 'up'})
                pipe.delete(waker)
            for stream in self._group_dispatcher.handlers:
                for name in all_clustered(stream):
                    pipe.xgroup_delconsumer(name, self._group, self._consumer)
            pipe.execute(raise_on_error=False)
        logging.info(f'delete consumers {self._group_dispatcher.handlers.keys()}')
        logging.info(f'delete wakers {self._wakers}')

    def _group_run(self):
        def run(streams):
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

        for names in zip(*[all_clustered(stream) for stream in self._group_dispatcher.handlers]):
            gevent.spawn(run, names)

    def _fanout_run(self):
        with self.redis.pipeline(transaction=False) as pipe:
            stream_names = self._fanout_dispatcher.handlers.keys()
            for stream in stream_names:
                pipe.xinfo_stream(stream)
            last_ids = [xinfo['last-generated-id'] for xinfo in pipe.execute()]
        stream_last_ids = dict(zip(stream_names, last_ids))

        def run(streams):
            streams = {stream: stream_last_ids[stream] for stream in streams}
            while not self._stopped:
                try:
                    result = self.redis.xread(streams, count=self._batch, block=0)
                    for stream, messages in result:
                        for message in messages:
                            self._fanout_dispatcher.dispatch(stream, *message[::-1])
                            if stream in streams:  # may removed in dispatch
                                streams[stream] = message[0]  # update last id
                except Exception:
                    logging.exception(f'')
                    gevent.sleep(1)
            logging.info(f'fanout exit {streams.keys()}')

        for names in zip(*[all_clustered(stream) for stream in self._fanout_dispatcher.handlers]):
            gevent.spawn(run, names)
