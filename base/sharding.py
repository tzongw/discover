# -*- coding: utf-8 -*-
import logging
import random
from datetime import timedelta, datetime
from binascii import crc32
from random import shuffle
from typing import Union
import gevent
from pydantic import BaseModel
from redis import Redis, RedisCluster
from .mq import Publisher, Receiver, ProtoDispatcher
from .utils import stream_name
from .misc import Stock
from .timer import Timer
from .chunk import batched


class ShardingKey:
    def __init__(self, shards, fixed=()):
        self.shards = shards
        self.fixed = fixed  # keys fixed in shard 0

    def all_sharded_keys(self, key: str):
        assert not key.startswith('{')
        return [f'{{{i}}}:{key}' for i in range(self.shards)]

    def random_sharded_key(self, key: str):
        assert not key.startswith('{')
        shard = random.randrange(self.shards)
        return f'{{{shard}}}:{key}'

    def get_shard(self, key):
        # `shard` consistent across different processes
        return 0 if key in self.fixed else crc32(key.encode()) % self.shards

    def sharded_key(self, key: str):
        return self.sharded_keys(key)[0]

    def sharded_keys(self, *keys):
        assert all(not key.startswith('{') for key in keys)
        shard = self.get_shard(keys[0])
        return [f'{{{shard}}}:{key}' for key in keys]

    @staticmethod
    def normalized_key(key: str):
        assert key.startswith('{')
        return key[key.index(':') + 1:]


class ShardingPublisher(Publisher):
    def __init__(self, redis: Union[Redis, RedisCluster], *, hint=None):
        super().__init__(redis, hint)
        self._sharding_key = ShardingKey(shards=len(redis.get_primaries()))

    def publish(self, message: BaseModel, maxlen=4096, stream=None):
        stream = stream or stream_name(message)
        stream = self._sharding_key.random_sharded_key(stream)
        return super().publish(message, maxlen, stream)


class NormalizedDispatcher(ProtoDispatcher):
    def dispatch(self, stream, *args, **kwargs):
        stream = ShardingKey.normalized_key(stream)
        return super().dispatch(stream, *args, **kwargs)


class ShardingReceiver(Receiver):
    def __init__(self, redis: Union[Redis, RedisCluster], group: str, consumer: str, *, batch=50):
        super().__init__(redis, group, consumer, batch, dispatcher=NormalizedDispatcher)
        self._sharding_key = ShardingKey(shards=len(redis.get_primaries()))

    def start(self):
        streams = self._dispatcher.keys()
        with self.redis.pipeline(transaction=False) as pipe:
            for stream in streams:
                for sharded_stream in self._sharding_key.all_sharded_keys(stream):
                    # create group & stream
                    pipe.xgroup_create(sharded_stream, self._group, mkstream=True)
            pipe.execute(raise_on_error=False)  # group already exists
        return [gevent.spawn(self._run, sharded_streams) for sharded_streams in
                zip(*[self._sharding_key.all_sharded_keys(stream) for stream in streams])]

    def stop(self):
        logging.info(f'stop {self._waker}')
        self._stopped = True
        streams = self._dispatcher.keys()
        with self.redis.pipeline(transaction=False) as pipe:
            for waker in self._sharding_key.all_sharded_keys(self._waker):
                pipe.xadd(waker, {'wake': 'up'})
                pipe.delete(waker)
            for stream in streams:
                if stream == self._waker:  # already deleted
                    continue
                for sharded_stream in self._sharding_key.all_sharded_keys(stream):
                    pipe.xgroup_delconsumer(sharded_stream, self._group, self._consumer)
            pipe.execute(raise_on_error=False)  # stop but no start


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
    def __init__(self, redis, *, sharding_key: ShardingKey, old_timer: Timer, start_time: datetime, hint=None):
        super().__init__(redis, sharding_key=sharding_key, hint=hint)
        self.old_timer = old_timer
        self.start_time = start_time  # migration start time, after deployment

    def create(self, key: str, message: BaseModel, interval: timedelta, *, loop=False, maxlen=4096, stream=None):
        if datetime.now() >= self.start_time:
            self.old_timer.kill(key)
            return super().create(key, message, interval, loop=loop, maxlen=maxlen, stream=stream)
        else:
            return self.old_timer.create(key, message, interval, loop=loop, maxlen=maxlen, stream=stream)

    def kill(self, key):
        return super().kill(key) or self.old_timer.kill(key)

    def exists(self, key: str):
        return super().exists(key) or self.old_timer.exists(key)

    def info(self, key: str):
        return super().info(key) or self.old_timer.info(key)

    def tick(self, key: str, stream: str, interval=timedelta(seconds=1), offset=10, maxlen=1024):
        if self.redis is not self.old_timer.redis and self.old_timer.kill(key):
            _, new_stream = self._sharding_key.sharded_keys(key, stream)
            if isinstance(self.old_timer, ShardingTimer):
                _, old_stream = self.old_timer._sharding_key.sharded_keys(key, stream)
            else:
                old_stream = stream
            last_id = self.old_timer.redis.xinfo_stream(old_stream)['last-generated-id']
            last_tick = int(last_id[:-2])
            self.redis.xadd(new_stream, fields={'': ''}, id=str(last_tick + 1))
        return super().tick(key, stream, interval, offset=offset, maxlen=maxlen)


class MigratingReceiver(ShardingReceiver):
    def __init__(self, redis, group: str, consumer: str, *, batch=50, old_receiver: Receiver):
        super().__init__(redis, group, consumer, batch=batch)
        self.old_receiver = old_receiver

    def start(self):
        return self.old_receiver.start() + super().start()

    def stop(self):
        self.old_receiver.stop()
        super().stop()

    def __call__(self, key_or_cls, stream=None):
        def decorator(f):
            self.old_receiver(key_or_cls, stream=stream)(f)
            self._dispatcher(key_or_cls, stream=stream)(f)
            return f

        return decorator


class ShardingStock(Stock):
    def __init__(self, redis: RedisCluster):
        super().__init__(redis)
        self.sharding_key = ShardingKey(shards=len(redis.get_primaries()))

    def mget(self, keys, hint=None):
        with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                for sharded_key in self.sharding_key.all_sharded_keys(key):
                    pipe.bitfield(sharded_key).get(fmt='u32', offset=0).execute()
            shard = None if hint is None else self.sharding_key.get_shard(hint)
            return [0 if shard is not None and chunk[shard][0] == 0 else sum(values[0] for values in chunk)
                    for chunk in batched(pipe.execute(), self.sharding_key.shards)]

    def _fair_amounts(self, total):
        shards = self.sharding_key.shards
        amounts = [total // shards] * shards
        for i in range(total - sum(amounts)):
            amounts[i] += 1
        shuffle(amounts)
        return amounts

    def reset(self, key, total=0, expire=None):
        assert total >= 0
        with self.redis.pipeline(transaction=False) as pipe:
            for sharded_key, amount in zip(self.sharding_key.all_sharded_keys(key), self._fair_amounts(total)):
                pipe.bitfield(sharded_key).set(fmt='u32', offset=0, value=amount).execute()
                if expire is not None:
                    pipe.expire(sharded_key, expire)
            pipe.execute()

    def incrby(self, key, total):
        assert total >= 0
        with self.redis.pipeline(transaction=False) as pipe:
            for sharded_key, amount in zip(self.sharding_key.all_sharded_keys(key), self._fair_amounts(total)):
                pipe.bitfield(sharded_key).incrby(fmt='u32', offset=0, increment=amount).execute()
            return sum(values[0] for values in pipe.execute())

    def try_lock(self, key, hint=None) -> bool:
        if hint is None:
            sharded_key = self.sharding_key.random_sharded_key(key)
        else:
            _, sharded_key = self.sharding_key.sharded_keys(hint, key)
        bitfield = self.redis.bitfield(sharded_key, default_overflow='FAIL')
        return bitfield.incrby(fmt='u32', offset=0, increment=-1).execute()[0] is not None
