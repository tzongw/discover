# -*- coding: utf-8 -*-
import logging
from typing import Union
from datetime import timedelta
import gevent
from pydantic import BaseModel
from redis.crc import key_slot
from .mq import Publisher, Receiver, ProtoDispatcher
from .utils import stream_name, timer_name
from .timer import Timer


class ShardedKey:
    def __init__(self, shards, fixed=()):
        self.shards = shards
        self.fixed = fixed  # keys fixed in shard 0

    def all_sharded_keys(self, key: str):
        assert not key.startswith('{')
        return [f'{{{i}}}:{key}' for i in range(self.shards)]

    def sharded_key(self, key):
        return self.sharded_keys(key)[0]

    def sharded_keys(self, *keys):
        assert all(not key.startswith('{') for key in keys)
        # consistent across different runs
        shard = 0 if keys[0] in self.fixed else key_slot(keys[0].encode()) % self.shards
        return [f'{{{shard}}}:{key}' for key in keys]

    @staticmethod
    def normalized_key(key: str):
        assert key.startswith('{')
        return key[key.index(':') + 1:]


class ShardedPublisher(Publisher):
    def __init__(self, redis, *, sharded_key: ShardedKey, hint=None):
        super().__init__(redis, hint)
        self._sharded_key = sharded_key

    def publish(self, message: BaseModel, maxlen=4096, do_hint=True, stream=None):
        stream = stream or stream_name(message)
        stream = self._sharded_key.sharded_key(stream)
        return super().publish(message, maxlen, do_hint, stream)


class NormalizedDispatcher(ProtoDispatcher):
    def dispatch(self, stream, *args, **kwargs):
        stream = ShardedKey.normalized_key(stream)
        return super().dispatch(stream, *args, **kwargs)


class ShardedReceiver(Receiver):
    def __init__(self, redis, group: str, consumer: str, *, sharded_key: ShardedKey, batch=10):
        super().__init__(redis, group, consumer, batch, dispatcher=NormalizedDispatcher)
        self._sharded_key = sharded_key

    def start(self):
        with self.redis.pipeline(transaction=False) as pipe:
            streams = set(self._group_dispatcher.handlers) | set(self._fanout_dispatcher.handlers)
            for stream in streams:
                for sharded_stream in self._sharded_key.all_sharded_keys(stream):
                    # create group & stream
                    pipe.xgroup_create(sharded_stream, self._group, mkstream=True)
            pipe.execute(raise_on_error=False)  # group already exists

        for streams in zip(*[self._sharded_key.all_sharded_keys(stream) for stream in self._group_dispatcher.handlers]):
            gevent.spawn(self._group_run, streams)
        for streams in zip(
                *[self._sharded_key.all_sharded_keys(stream) for stream in self._fanout_dispatcher.handlers]):
            gevent.spawn(self._fanout_run, streams)

    def stop(self):
        logging.info(f'stop')
        self._stopped = True
        with self.redis.pipeline(transaction=False) as pipe:
            for waker in self._sharded_key.all_sharded_keys(self._waker):
                pipe.xadd(waker, {'wake': 'up'})
                pipe.delete(waker)
            self._group_dispatcher.handlers.pop(self._waker)  # already deleted
            for stream in self._group_dispatcher.handlers:
                for sharded_stream in self._sharded_key.all_sharded_keys(stream):
                    pipe.xgroup_delconsumer(sharded_stream, self._group, self._consumer)
            pipe.execute()
        logging.info(f'delete waker {self._waker}')


class ShardedTimer(Timer):
    def __init__(self, redis, *, sharded_key: ShardedKey, hint=None):
        super().__init__(redis, hint)
        self._sharded_key = sharded_key

    def create(self, message: BaseModel, interval: Union[int, timedelta], *, loop=False, key=None, maxlen=4096,
               do_hint=True, stream=None):
        stream = stream or stream_name(message)
        if key is None:
            data = message.json(exclude_defaults=True)
            key = f'{timer_name(message)}:{data}'
        sharded_key, stream = self._sharded_key.sharded_keys(key, stream)
        super().create(message, interval, loop=loop, key=sharded_key, maxlen=maxlen, do_hint=do_hint, stream=stream)
        return key

    def kill(self, key):
        key = self._sharded_key.sharded_key(key)
        return super().kill(key)

    def exists(self, key: str):
        key = self._sharded_key.sharded_key(key)
        return super().exists(key)

    def info(self, key: str):
        key = self._sharded_key.sharded_key(key)
        return super().info(key)

    def tick(self, key, interval: Union[int, timedelta], stream, offset=10, maxlen=1024):
        assert key in self._sharded_key.fixed, 'SHOULD fixed shard to avoid duplicated timestamp'
        key, stream = self._sharded_key.sharded_keys(key, stream)
        return super().tick(key, interval, stream, offset=offset, maxlen=maxlen)


class MigratingTimer(ShardedTimer):
    def __init__(self, redis, *, sharded_new: ShardedKey, sharded_old: ShardedKey, hint=None):
        super().__init__(redis, sharded_key=sharded_new, hint=hint)
        self._timer_old = ShardedTimer(redis, sharded_key=sharded_old, hint=hint)

    def create(self, message: BaseModel, interval: Union[int, timedelta], *, loop=False, key=None, maxlen=4096,
               do_hint=True, stream=None):
        key = super().create(message, interval, loop=loop, key=key, maxlen=maxlen, do_hint=do_hint, stream=stream)
        self._timer_old.kill(key)
        return key

    def kill(self, key):
        return self._timer_old.kill(key) or super().kill(key)

    def exists(self, key: str):
        return self._timer_old.exists(key) or super().exists(key)

    def info(self, key: str):
        return self._timer_old.info(key) or super().info(key)
