# -*- coding: utf-8 -*-
import time
import uuid
import functools
from datetime import datetime, timedelta
from collections import namedtuple, OrderedDict
from typing import TypeVar, Optional, Generic, Callable, Sequence
from redis import Redis, RedisCluster
import gevent
from . import utils
from .singleflight import Singleflight, singleflight
from .invalidator import Invalidator
from .chunk import LazySequence
from .redis_script import Script

T = TypeVar('T')
_NONE = object()
_Pair = namedtuple('_Pair', ['value', 'expire_at'])


def expire_at(expire):
    if expire is None:
        return float('inf')
    if isinstance(expire, datetime):
        return expire.timestamp()
    if isinstance(expire, timedelta):
        return time.time() + expire.total_seconds()
    if isinstance(expire, (int, float)):
        return time.time() + expire
    raise ValueError(f'{expire} not valid')


class Cache(Singleflight[T]):
    # https://redis.io/docs/latest/develop/reference/client-side-caching/#avoiding-race-conditions

    def __init__(self, *, get=None, mget=None, maxsize: Optional[int] = 4096, make_key=utils.make_key):
        super().__init__(get=get, mget=mget, make_key=make_key)
        self.lru = OrderedDict()
        self.maxsize = maxsize
        self.hits = 0
        self.misses = 0
        self.invalids = 0

    def __str__(self):
        return f'hits: {self.hits} misses: {self.misses} invalids: {self.invalids}'

    def _set_value(self, key, value):
        self.lru[key] = value
        if self.maxsize is not None and len(self.lru) > self.maxsize:
            self.lru.popitem(last=False)

    def mget(self, keys, *args, **kwargs) -> Sequence[T]:
        results = [None] * len(keys)
        missing_keys = []
        made_keys = []
        indexes = []
        for index, key in enumerate(keys):
            made_key = self._make_key(key, *args, **kwargs)
            value = self.lru.get(made_key, _NONE)
            if value is not _NONE:
                self.hits += 1
                results[index] = value
                if self.maxsize is not None:
                    self.lru.move_to_end(made_key)
            else:
                self.misses += 1
                missing_keys.append(key)
                made_keys.append(made_key)
                indexes.append(index)
        if missing_keys:
            version = self.invalids
            values, real_gets = super()._mget_stats(missing_keys, *args, **kwargs)
            for index, made_key, value, real_get in zip(indexes, made_keys, values, real_gets):
                assert not isinstance(value, (list, set, dict)), 'use tuple, frozenset, MappingProxyType instead'
                results[index] = value
                if version == self.invalids and real_get:
                    self._set_value(made_key, value)
        return results

    def listen(self, invalidator: Invalidator, group: str):
        @invalidator(group)
        def invalidate(key: str):
            self.invalids += 1
            if key:
                self.lru.pop(self._make_key(key), None)
            else:
                self.lru.clear()


class TtlCache(Cache[T]):
    def mget(self, keys, *args, **kwargs) -> Sequence[T]:
        results = [None] * len(keys)
        missing_keys = []
        made_keys = []
        indexes = []
        now = time.time()
        for index, key in enumerate(keys):
            made_key = self._make_key(key, *args, **kwargs)
            pair = self.lru.get(made_key, _NONE)
            if pair is not _NONE and pair.expire_at > now:
                self.hits += 1
                results[index] = pair.value
                if self.maxsize is not None:
                    self.lru.move_to_end(made_key)
            else:
                if pair is not _NONE:  # remove expired key
                    self.lru.pop(made_key)
                self.misses += 1
                missing_keys.append(key)
                made_keys.append(made_key)
                indexes.append(index)
        if missing_keys:
            version = self.invalids
            tuples, real_gets = super()._mget_stats(missing_keys, *args, **kwargs)
            for index, made_key, (value, expire), real_get in zip(indexes, made_keys, tuples, real_gets):
                assert not isinstance(value, (list, set, dict)), 'use tuple, frozenset, MappingProxyType instead'
                results[index] = value
                if version == self.invalids and real_get:
                    pair = _Pair(value, expire_at(expire))
                    self._set_value(made_key, pair)
        return results


class FullMixin(Generic[T]):
    invalids: int
    mget: Callable

    def __init__(self, *, get_values):
        self._get_values = get_values
        self._values = ()
        self._version = 0
        self._expire_at = float('-inf')

    @property
    def values(self) -> Sequence[T] | LazySequence[T]:
        if self._version == self.invalids and self._expire_at > time.time():
            return self._values
        return self._update_values()

    @singleflight
    def _update_values(self):
        version = self.invalids
        values, expire = self._get_values()  # invalidate events may happen simultaneously
        assert isinstance(values, (tuple, LazySequence))
        self._values = values
        self._expire_at = expire_at(expire)
        self._version = version
        return values

    def cached(self, func=None, *, maxsize=128, typed=False, get_expire=None):
        def decorator(f):
            @functools.lru_cache(maxsize, typed)
            def inner(*args, **kwargs):
                return f(*args, **kwargs)

            cache_version = 0
            cache_expire_at = float('-inf')

            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                nonlocal cache_version
                nonlocal cache_expire_at
                values = self.values
                if cache_version != self._version or cache_expire_at < time.time():
                    inner.cache_clear()
                    cache_version = self._version
                    assert not get_expire or not isinstance(values, LazySequence), 'DO NOT get expire of lazy sequence'
                    expire = get_expire(values) if get_expire else None
                    cache_expire_at = expire_at(expire)
                return inner(*args, **kwargs)

            wrapper.cache_info = inner.cache_info
            wrapper.cache_clear = inner.cache_clear
            return wrapper

        return decorator(func) if func else decorator


class FullCache(FullMixin[T], Cache[T]):
    def __init__(self, *, mget, maxsize: Optional[int] = 4096, make_key=utils.make_key, get_values):
        super(FullMixin, self).__init__(mget=mget, maxsize=maxsize, make_key=make_key)
        super().__init__(get_values=get_values)


class FullTtlCache(FullMixin[T], TtlCache[T]):
    def __init__(self, *, mget, maxsize: Optional[int] = 4096, make_key=utils.make_key, get_values):
        super(FullMixin, self).__init__(mget=mget, maxsize=maxsize, make_key=make_key)
        super().__init__(get_values=get_values)


def ttl_cache(expire, *, maxsize=128):
    def decorator(f):
        def get(key):
            args, *items = key
            return f(*args, **dict(items)), expire

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            key = (args, *kwargs.items())
            return cache.get(key)

        cache = TtlCache(get=get, maxsize=maxsize)
        wrapper.cache = cache
        return wrapper

    return decorator


class RedisCache(Singleflight[T]):
    def __init__(self, redis, *, get=None, mget=None, expire: timedelta, make_key, serialize=None, deserialize=None,
                 prefix='PLACEHOLDER:', try_interval=timedelta(milliseconds=50), try_times=10):
        super().__init__(mget=self._cached_mget, make_key=make_key)
        self.redis = redis  # type: Redis | RedisCluster
        self.raw_mget = utils.make_mget(get, mget)
        self.serialize = serialize
        self.deserialize = deserialize
        self.expire = expire
        self.prefix = prefix
        self.try_interval = try_interval
        self.try_times = try_times

    def _cached_mget(self, keys, *args, **kwargs):
        placeholder = self.prefix + str(uuid.uuid4())
        made_keys = []
        todo_indexes = []
        wait_indexes = []
        with self.redis.pipeline(transaction=False) as pipe:
            lock_time = max(self.try_interval * self.try_times, timedelta(seconds=5))
            for key in keys:
                made_key = self._make_key(key, *args, **kwargs)
                made_keys.append(made_key)
                pipe.set(made_key, placeholder, nx=True, px=lock_time, get=True)
            values = pipe.execute()
        for index, value in enumerate(values):
            if value is None:
                todo_indexes.append(index)
            elif value.startswith(self.prefix):
                wait_indexes.append(index)
            else:
                values[index] = self.deserialize(value) if self.deserialize else value
        if todo_indexes:
            new_values = self.raw_mget([keys[index] for index in todo_indexes], *args, **kwargs)
            with self.redis.pipeline(transaction=False) as pipe:
                script = Script(pipe)
                for index, value in zip(todo_indexes, new_values):
                    values[index] = value
                    data = self.serialize(value) if self.serialize else value
                    script.compare_set(made_keys[index], placeholder, data, self.expire)
                pipe.execute()
        try_times = 0
        while wait_indexes:
            try_times += 1
            gevent.sleep(self.try_interval.total_seconds())
            wait_keys = [made_keys[index] for index in wait_indexes]
            new_values = self.redis.mget_nonatomic(wait_keys) if isinstance(self.redis, RedisCluster) else \
                self.redis.mget(wait_keys)
            fail_indexes = []
            for index, value in zip(wait_indexes, new_values):
                if value is None or value.startswith(self.prefix):
                    fail_indexes.append(index)
                else:
                    values[index] = self.deserialize(value) if self.deserialize else value
            if not fail_indexes:  # all done
                break
            if try_times >= self.try_times:
                fail_keys = ', '.join(f'`{keys[index]}`' for index in fail_indexes)
                raise ValueError(f'{fail_keys} not resolve')
            wait_indexes = fail_indexes
        return values
