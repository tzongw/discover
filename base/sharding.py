# -*- coding: utf-8 -*-
import logging
import random
import time
from datetime import timedelta, datetime
from functools import lru_cache
from typing import TypeVar, Generic, Generator
from random import shuffle
from typing import Union
import gevent
from pydantic import BaseModel
from redis import Redis, RedisCluster
from .mq import Publisher, Receiver, ProtoDispatcher
from .utils import stream_name, CHash
from .misc import Stock
from .timer import Timer
from .ztimer import ZTimer
from .chunk import batched
from .task import HeavyTask


class Sharding:
    @staticmethod
    @lru_cache
    def get_chash(shards, replicas):
        return CHash(range(shards), replicas)

    def __init__(self, shards, fixed_keys=(), replicas=10):
        self.shards = shards
        self.fixed_keys = fixed_keys  # keys fixed in shard 0
        self.chash = self.get_chash(shards, replicas)

    def get_shard(self, key: str):
        if key in self.fixed_keys:
            return 0
        return self.chash(key)

    def all_sharded_keys(self, key: str):
        assert not key.startswith('{')
        return [f'{{{i}}}:{key}' for i in range(self.shards)]

    def random_sharded_key(self, key: str):
        assert not key.startswith('{')
        shard = random.randrange(self.shards)
        return f'{{{shard}}}:{key}'

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
    def __init__(self, redis: Union[Redis, RedisCluster], *, maxlen=4096, hint=None):
        super().__init__(redis, maxlen=maxlen, hint=hint)
        self._sharding = Sharding(shards=len(redis.get_primaries()))

    def publish(self, message: BaseModel, stream=None):
        stream = stream or stream_name(message)
        stream = self._sharding.random_sharded_key(stream)
        return super().publish(message, stream)


class NormalizedDispatcher(ProtoDispatcher):
    def dispatch(self, stream, *args, **kwargs):
        stream = Sharding.normalized_key(stream)
        return super().dispatch(stream, *args, **kwargs)


class ShardingReceiver(Receiver):
    def __init__(self, redis: Union[Redis, RedisCluster], group: str, consumer: str, *, workers=32):
        super().__init__(redis, group, consumer, workers, dispatcher=NormalizedDispatcher)
        self._sharding = Sharding(shards=len(redis.get_primaries()))

    def start(self):
        logging.info(f'start {self._group} {self._consumer}')
        self._stopped = False
        streams = self._dispatcher.keys()
        with self.redis.pipeline(transaction=False) as pipe:
            for stream in streams:
                for sharded_stream in self._sharding.all_sharded_keys(stream):
                    # create group & stream
                    pipe.xgroup_create(sharded_stream, self._group, mkstream=True)
            pipe.execute(raise_on_error=False)  # group already exists
        return [gevent.spawn(self._run, sharded_streams) for sharded_streams in
                zip(*[self._sharding.all_sharded_keys(stream) for stream in streams])]

    def stop(self):
        if self._stopped:
            return
        logging.info(f'stop {self._group} {self._consumer}')
        self._stopped = True
        streams = self._dispatcher.keys()
        with self.redis.pipeline(transaction=False) as pipe:
            for waker in self._sharding.all_sharded_keys(self._waker):
                pipe.xadd(waker, {'wake': 'up'})
                pipe.delete(waker)
            for stream in streams:
                if stream == self._waker:  # already deleted
                    continue
                for sharded_stream in self._sharding.all_sharded_keys(stream):
                    pipe.xgroup_delconsumer(sharded_stream, self._group, self._consumer)
            pipe.execute()


class ShardingTimer(Timer):
    def __init__(self, redis, *, sharding: Sharding, maxlen=4096, hint=None):
        super().__init__(redis, maxlen=maxlen, hint=hint)
        self._sharding = sharding

    def create(self, key: str, message: BaseModel, interval: timedelta, *, loop=False, stream=None):
        stream = stream or stream_name(message)
        key, stream = self._sharding.sharded_keys(key, stream)
        return super().create(key, message, interval, loop=loop, stream=stream)

    def kill(self, key):
        key = self._sharding.sharded_key(key)
        return super().kill(key)

    def exists(self, key: str):
        key = self._sharding.sharded_key(key)
        return super().exists(key)

    def info(self, key: str):
        key = self._sharding.sharded_key(key)
        return super().info(key)

    def tick(self, key: str, stream: str, interval=timedelta(seconds=1), offset=10):
        assert key in self._sharding.fixed_keys, 'SHOULD fixed shard to avoid duplicated timestamp'
        key, stream = self._sharding.sharded_keys(key, stream)
        return super().tick(key, stream, interval, offset=offset)


class MigratingTimer(ShardingTimer):
    def __init__(self, redis, *, sharding: Sharding, old_timer: Timer, start_time: datetime, maxlen=4096, hint=None):
        super().__init__(redis, sharding=sharding, maxlen=maxlen, hint=hint)
        self.old_timer = old_timer
        self.start_time = start_time  # migration start time, after deployment

    def is_moved(self, key):
        consistent = self.redis is self.old_timer.redis and isinstance(self.old_timer, ShardingTimer) and \
                     self._sharding.get_shard(key) == self.old_timer._sharding.get_shard(key)
        return not consistent

    @property
    def is_migrating(self):
        return datetime.now() >= self.start_time

    def create(self, key: str, message: BaseModel, interval: timedelta, *, loop=False, stream=None):
        if not self.is_migrating:
            return self.old_timer.create(key, message, interval, loop=loop, stream=stream)
        added = super().create(key, message, interval, loop=loop, stream=stream)
        if added and self.is_moved(key) and self.old_timer.kill(key):
            added = 0
        return added

    def kill(self, key):
        if not self.is_migrating:
            return self.old_timer.kill(key)
        deleted = super().kill(key)
        if not deleted and self.is_moved(key):
            deleted = self.old_timer.kill(key)
        return deleted

    def exists(self, key: str):
        if not self.is_migrating:
            return self.old_timer.exists(key)
        exists = super().exists(key)
        if not exists and self.is_moved(key):
            exists = self.old_timer.exists(key)
        return exists

    def info(self, key: str):
        if not self.is_migrating:
            return self.old_timer.info(key)
        info = super().info(key)
        if info is None and self.is_moved(key):
            info = self.old_timer.info(key)
        return info

    def tick(self, key: str, stream: str, interval=timedelta(seconds=1), offset=10, maxlen=1024):
        if self.redis is not self.old_timer.redis and self.old_timer.kill(key):
            if isinstance(self.old_timer, ShardingTimer):
                _, old_stream = self.old_timer._sharding.sharded_keys(key, stream)
            else:
                old_stream = stream
            last_id = self.old_timer.redis.xinfo_stream(old_stream)['last-generated-id']
            last_tick = int(last_id[:-2])
            _, new_stream = self._sharding.sharded_keys(key, stream)
            self.redis.xadd(new_stream, fields={'': ''}, id=str(last_tick + 1))
        return super().tick(key, stream, interval, offset=offset, maxlen=maxlen)


class MigratingReceiver(ShardingReceiver):
    def __init__(self, redis, group: str, consumer: str, *, workers=32, old_receiver: Receiver):
        assert old_receiver.redis is not redis, 'same redis, use ShardingReceiver instead'
        super().__init__(redis, group, consumer, workers=workers)
        self.old_receiver = old_receiver

    def start(self):
        return self.old_receiver.start() + super().start()

    def stop(self):
        self.old_receiver.stop()
        super().stop()

    def __call__(self, key_or_cls, *, stream=None):
        def decorator(f):
            self.old_receiver(key_or_cls, stream=stream)(f)
            self._dispatcher(key_or_cls, stream=stream)(f)
            return f

        return decorator


class ShardingStock(Stock):
    def __init__(self, redis: RedisCluster):
        super().__init__(redis)
        self.sharding = Sharding(shards=len(redis.get_primaries()))

    def mget(self, keys, hint=None):
        with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                for sharded_key in self.sharding.all_sharded_keys(key):
                    pipe.bitfield(sharded_key).get(fmt='u32', offset=0).execute()
            shard = None if hint is None else self.sharding.get_shard(hint)
            return [0 if shard is not None and chunk[shard][0] == 0 else sum(values[0] for values in chunk)
                    for chunk in batched(pipe.execute(), self.sharding.shards)]

    def _fair_amounts(self, total):
        shards = self.sharding.shards
        amounts = [total // shards] * shards
        for i in range(total - sum(amounts)):
            amounts[i] += 1
        shuffle(amounts)
        return amounts

    def reset(self, key, value=0, expire=None):
        assert value >= 0
        with self.redis.pipeline(transaction=False) as pipe:
            for sharded_key, amount in zip(self.sharding.all_sharded_keys(key), self._fair_amounts(value)):
                pipe.bitfield(sharded_key).set(fmt='u32', offset=0, value=amount).execute()
                if expire is not None:
                    pipe.expire(sharded_key, expire)
            pipe.execute()

    def incrby(self, key, increment):
        assert increment >= 0
        with self.redis.pipeline(transaction=False) as pipe:
            for sharded_key, amount in zip(self.sharding.all_sharded_keys(key), self._fair_amounts(increment)):
                pipe.bitfield(sharded_key).incrby(fmt='u32', offset=0, increment=amount).execute()
            return sum(values[0] for values in pipe.execute())

    def try_lock(self, key, hint=None) -> bool:
        if hint is None:
            sharded_key = self.sharding.random_sharded_key(key)
        else:
            _, sharded_key = self.sharding.sharded_keys(hint, key)
        bitfield = self.redis.bitfield(sharded_key, default_overflow='FAIL')
        return bitfield.incrby(fmt='u32', offset=0, increment=-1).execute()[0] is not None


class ShardingZTimer(ZTimer):
    def __init__(self, redis: RedisCluster, biz, *, sharding: Sharding):
        super().__init__(redis, biz)
        self._sharding = sharding
        self._orig_timeout_key = self._timeout_key
        self._orig_meta_key = self._meta_key

    def _update_sharded(self, key: str):
        _, self._timeout_key, self._meta_key = self._sharding.sharded_keys(key, self._orig_timeout_key,
                                                                           self._orig_meta_key)

    def new(self, key: str, data: str, interval: timedelta, *, loop=False):
        self._update_sharded(key)
        return super().new(key, data, interval, loop=loop)

    def kill(self, key: str):
        self._update_sharded(key)
        return super().kill(key)

    def exists(self, key: str):
        self._update_sharded(key)
        return super().exists(key)

    def info(self, key: str):
        self._update_sharded(key)
        return super().info(key)

    def poll(self, limit=100):
        with self.redis.pipeline(transaction=False) as pipe:
            now = time.time()
            for timeout_key, meta_key in zip(self._sharding.all_sharded_keys(self._orig_timeout_key),
                                             self._sharding.all_sharded_keys(self._orig_meta_key)):
                keys_and_args = [timeout_key, meta_key, now, limit]
                pipe.fcall('ztimer_poll', 2, *keys_and_args)
            res = sum(pipe.execute(), [])
        return dict(zip(res[::2], res[1::2]))


class MigratingZTimer(ShardingZTimer):
    def __init__(self, redis, biz, *, sharding: Sharding, old_timer: ZTimer, start_time: datetime):
        super().__init__(redis, biz, sharding=sharding)
        self.old_timer = old_timer
        self.start_time = start_time  # migration start time, after deployment

    def is_moved(self, key):
        consistent = self.redis is self.old_timer.redis and isinstance(self.old_timer, ShardingZTimer) and \
                     self._sharding.get_shard(key) == self.old_timer._sharding.get_shard(key)
        return not consistent

    @property
    def is_migrating(self):
        return datetime.now() >= self.start_time

    def new(self, key: str, data: str, interval: timedelta, *, loop=False):
        if not self.is_migrating:
            return self.old_timer.new(key, data, interval, loop=loop)
        added = super().new(key, data, interval, loop=loop)
        if added and self.is_moved(key) and self.old_timer.kill(key):
            added = 0
        return added

    def kill(self, key):
        if not self.is_migrating:
            return self.old_timer.kill(key)
        deleted = super().kill(key)
        if not deleted and self.is_moved(key):
            deleted = self.old_timer.kill(key)
        return deleted

    def exists(self, key: str):
        if not self.is_migrating:
            return self.old_timer.exists(key)
        exists = super().exists(key)
        if not exists and self.is_moved(key):
            exists = self.old_timer.exists(key)
        return exists

    def info(self, key: str):
        if not self.is_migrating:
            return self.old_timer.info(key)
        info = super().info(key)
        if info is None and self.is_moved(key):
            info = self.old_timer.info(key)
        return info

    def poll(self, limit=100):
        if not self.is_migrating:
            return self.old_timer.poll(limit)
        if self.redis is self.old_timer.redis and isinstance(self.old_timer, ShardingZTimer):
            return super().poll(limit) if self._sharding.shards >= self.old_timer._sharding.shards \
                else self.old_timer.poll(limit)
        return super().poll(limit) | self.old_timer.poll(limit)


class ShardingHeavyTask(HeavyTask):
    def __init__(self, redis: RedisCluster, biz: str):
        super().__init__(redis, biz)
        self._sharding = Sharding(shards=len(redis.get_primaries()))

    def _get_queue(self, task):
        key = self._sharding.random_sharded_key(self._key)
        priority = self._priorities[task.path]
        queue = f'{key}:{priority}'
        return queue

    def start(self, exec_func=None):
        logging.info(f'start {self._key}')
        self._stopped = False
        return [gevent.spawn(self._run, exec_func or self.exec, key, waker)
                for key, waker in zip(self._sharding.all_sharded_keys(self._key),
                                      self._sharding.all_sharded_keys(self._waker))]

    def stop(self):
        if self._stopped:
            return
        logging.info(f'stop {self._key}')
        self._stopped = True
        with self.redis.pipeline(transaction=False) as pipe:
            for waker in self._sharding.all_sharded_keys(self._waker):
                pipe.rpush(waker, 'wake up')
                pipe.delete(waker)
            pipe.execute()


class MigratingHeavyTask(ShardingHeavyTask):
    def __init__(self, redis: RedisCluster, biz: str, *, old_heavy_task: HeavyTask):
        assert old_heavy_task.redis is not redis or not isinstance(old_heavy_task, ShardingHeavyTask), \
            'same redis, use ShardingHeavyTask instead'
        super().__init__(redis, biz)
        self.old_heavy_task = old_heavy_task

    def start(self, exec_func=None):
        return self.old_heavy_task.start(exec_func) + super().start(exec_func)

    def stop(self):
        self.old_heavy_task.stop()
        super().stop()


K = TypeVar('K')
V = TypeVar('V')


class ShardingDict(Generic[K, V]):
    def __init__(self, shards):
        self._dicts = [{} for _ in range(shards)]

    def _get_shard(self, key) -> dict:
        index = hash(key) % len(self._dicts)
        return self._dicts[index]

    def __getitem__(self, key) -> V:
        d = self._get_shard(key)
        return d[key]

    def __setitem__(self, key, value):
        d = self._get_shard(key)
        d[key] = value

    def __delitem__(self, key):
        d = self._get_shard(key)
        del d[key]

    def __len__(self):
        return sum(len(d) for d in self._dicts)

    def __iter__(self):
        return self.keys()

    def pop(self, key, *args) -> V | None:
        d = self._get_shard(key)
        return d.pop(key, *args)

    def get(self, key, *args) -> V | None:
        d = self._get_shard(key)
        return d.get(key, *args)

    def clear(self):
        for d in self._dicts:
            d.clear()

    def items(self) -> Generator[tuple[K, V], None, None]:
        done = 0
        for d in self._dicts:
            if done >= 512:
                done = 0
                gevent.sleep(0)
            for item in d.items():
                yield item
            done += len(d)

    def keys(self) -> Generator[K, None, None]:
        for key, _ in self.items():
            yield key

    def values(self) -> Generator[V, None, None]:
        for _, value in self.items():
            yield value


E = TypeVar('E')


class ShardingSet(Generic[E]):
    def __init__(self, shards):
        self._sets = [set() for _ in range(shards)]

    def _get_shard(self, elem) -> set:
        index = hash(elem) % len(self._sets)
        return self._sets[index]

    def __len__(self):
        return sum(len(s) for s in self._sets)

    def __iter__(self) -> Generator[tuple[K, V], None, None]:
        done = 0
        for s in self._sets:
            if done >= 512:
                done = 0
                gevent.sleep(0)
            for elem in s:
                yield elem
            done += len(s)

    def clear(self):
        for s in self._sets:
            s.clear()

    def add(self, elem):
        s = self._get_shard(elem)
        s.add(elem)

    def remove(self, elem):
        s = self._get_shard(elem)
        s.remove(elem)

    def discard(self, elem):
        s = self._get_shard(elem)
        s.discard(elem)
