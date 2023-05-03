# -*- coding: utf-8 -*-
import logging
from datetime import timedelta
from binascii import crc32
from typing import Type, List, Dict, TypeVar
import gevent
from pydantic import BaseModel
from redis import RedisCluster
from .parser import Parser, patch_callbacks
from .invalidator import Invalidator
from .mq import Publisher, Receiver, ProtoDispatcher
from .utils import stream_name
from .timer import Timer


class ShardingKey:
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
        # `shard` consistent across different processes
        shard = 0 if keys[0] in self.fixed else crc32(keys[0].encode()) % self.shards
        return [f'{{{shard}}}:{key}' for key in keys]

    @staticmethod
    def normalized_key(key: str):
        assert key.startswith('{')
        return key[key.index(':') + 1:]


class ShardingPublisher(Publisher):
    def __init__(self, redis, *, sharding_key: ShardingKey, hint=None):
        super().__init__(redis, hint)
        self._sharding_key = sharding_key

    def publish(self, message: BaseModel, maxlen=4096, stream=None):
        stream = stream or stream_name(message)
        stream = self._sharding_key.sharded_key(stream)
        return super().publish(message, maxlen, stream)


class NormalizedDispatcher(ProtoDispatcher):
    def dispatch(self, stream, *args, **kwargs):
        stream = ShardingKey.normalized_key(stream)
        return super().dispatch(stream, *args, **kwargs)


class ShardingReceiver(Receiver):
    def __init__(self, redis, group: str, consumer: str, *, sharding_key: ShardingKey, batch=50):
        super().__init__(redis, group, consumer, batch, dispatcher=NormalizedDispatcher)
        self._sharding_key = sharding_key

    def start(self):
        with self.redis.pipeline(transaction=False) as pipe:
            streams = set(self._group_dispatcher.handlers) | set(self._fanout_dispatcher.handlers)
            for stream in streams:
                for sharded_stream in self._sharding_key.all_sharded_keys(stream):
                    # create group & stream
                    pipe.xgroup_create(sharded_stream, self._group, mkstream=True)
            pipe.execute(raise_on_error=False)  # group already exists

        for streams in zip(
                *[self._sharding_key.all_sharded_keys(stream) for stream in self._group_dispatcher.handlers]):
            gevent.spawn(self._group_run, streams)
        for streams in zip(
                *[self._sharding_key.all_sharded_keys(stream) for stream in self._fanout_dispatcher.handlers]):
            gevent.spawn(self._fanout_run, streams)

    def stop(self):
        logging.info(f'stop')
        self._stopped = True
        with self.redis.pipeline(transaction=False) as pipe:
            for waker in self._sharding_key.all_sharded_keys(self._waker):
                pipe.xadd(waker, {'wake': 'up'})
                pipe.delete(waker)
            self._group_dispatcher.handlers.pop(self._waker)  # already deleted
            for stream in self._group_dispatcher.handlers:
                for sharded_stream in self._sharding_key.all_sharded_keys(stream):
                    pipe.xgroup_delconsumer(sharded_stream, self._group, self._consumer)
            pipe.execute(raise_on_error=False)  # stop but no start
        logging.info(f'delete waker {self._waker}')


class ShardingTimer(Timer):
    def __init__(self, redis, *, sharding_key: ShardingKey, hint=None):
        super().__init__(redis, hint)
        self._sharding_key = sharding_key

    def create(self, key: str, message: BaseModel, interval: timedelta, *, loop=False, maxlen=4096, stream=None):
        stream = stream or stream_name(message)
        key, stream = self._sharding_key.sharded_keys(key, stream)
        return super().create(key, message, interval, loop=loop, maxlen=maxlen, stream=stream)

    def kill(self, key):
        key = self._sharding_key.sharded_key(key)
        return super().kill(key)

    def exists(self, key: str):
        key = self._sharding_key.sharded_key(key)
        return super().exists(key)

    def info(self, key: str):
        key = self._sharding_key.sharded_key(key)
        return super().info(key)

    def tick(self, key: str, stream: str, interval=timedelta(seconds=1), offset=10, maxlen=1024):
        assert key in self._sharding_key.fixed, 'SHOULD fixed shard to avoid duplicated timestamp'
        key, stream = self._sharding_key.sharded_keys(key, stream)
        return super().tick(key, stream, interval, offset=offset, maxlen=maxlen)


class MigratingTimer(ShardingTimer):
    def __init__(self, redis, *, sharding_key: ShardingKey, old_timer: Timer, hint=None):
        super().__init__(redis, sharding_key=sharding_key, hint=hint)
        self.old_timer = old_timer

    def create(self, key: str, message: BaseModel, interval: timedelta, *, loop=False, maxlen=4096, stream=None):
        self.old_timer.kill(key)
        return super().create(key, message, interval, loop=loop, maxlen=maxlen, stream=stream)

    def kill(self, key):
        return super().kill(key) or self.old_timer.kill(key)

    def exists(self, key: str):
        return super().exists(key) or self.old_timer.exists(key)

    def info(self, key: str):
        return super().info(key) or self.old_timer.info(key)


class MigratingReceiver(ShardingReceiver):
    def __init__(self, redis, group: str, consumer: str, *, sharding_key: ShardingKey, batch=50,
                 old_receiver: Receiver):
        super().__init__(redis, group, consumer, sharding_key=sharding_key, batch=batch)
        self.old_receiver = old_receiver

    def start(self):
        self.old_receiver.start()
        super().start()

    def stop(self):
        self.old_receiver.stop()
        super().stop()

    def _group(self, *args, **kwargs):
        def decorator(f):
            self.old_receiver.group(*args, **kwargs)(f)
            self._group_dispatcher.handler(*args, **kwargs)(f)

        return decorator

    def _fanout(self, *args, **kwargs):
        def decorator(f):
            self.old_receiver.fanout(*args, **kwargs)(f)
            self._fanout_dispatcher.handler(*args, **kwargs)(f)

        return decorator

    @property
    def group(self):
        return self._group

    @property
    def fanout(self):
        return self._fanout


class ShardingInvalidator(Invalidator):
    def start(self):
        for node in self.redis.get_primaries():
            redis = self.redis.get_redis_connection(node)
            gevent.spawn(self._run, redis)


M = TypeVar('M', bound=BaseModel)


class ShardingParser(Parser):
    def update_callbacks(self):
        for node in self._redis.get_nodes():
            if redis := node.redis_connection:
                patch_callbacks(redis.response_callbacks)

    def mget_nonatomic(self, keys, cls: Type[M]) -> List[M]:
        assert type(self._redis) is RedisCluster
        with self._redis.pipeline(transaction=False) as pipe:
            parser = ShardingParser(pipe)
            for key in keys:
                parser.get(key, cls)
            return pipe.execute()

    def mset_nonatomic(self, mapping: Dict[str, M]) -> bool:
        assert type(self._redis) is RedisCluster
        with self._redis.pipeline(transaction=False) as pipe:
            parser = ShardingParser(pipe)
            for k, v in mapping.items():
                parser.set(k, v)
            pipe.execute()
        return True
