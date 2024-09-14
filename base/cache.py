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


class Cache(Generic[T]):
    placeholder = object()

    def __init__(self, *, get=None, mget=None, maxsize: Optional[int] = 4096, make_key=utils.make_key):
        self.singleflight = Singleflight(get=get, mget=mget, make_key=make_key)
        self.lru = OrderedDict()
        self.locks = {}  # https://redis.io/docs/manual/client-side-caching/#avoiding-race-conditions
        self.make_key = make_key
        self.maxsize = maxsize
        self.hits = 0
        self.misses = 0
        self.invalids = 0

    def __str__(self):
        return f'hits: {self.hits} misses: {self.misses} invalids: {self.invalids} ' \
               f'size: {len(self.lru)} maxsize: {self.maxsize}'

    def get(self, key, *args, **kwargs) -> T:
        value, = self.mget([key], *args, **kwargs)
        return value

    def _set_value(self, key, value):
        exists = key in self.lru
        self.lru[key] = value
        if self.maxsize is not None:
            if exists:  # size not changed
                self.lru.move_to_end(key)
            elif len(self.lru) > self.maxsize:
                self.lru.popitem(last=False)

    def mget(self, keys, *args, **kwargs) -> Sequence[T]:
        results = [self.placeholder] * len(keys)
        missing_keys = []
        made_keys = []
        indexes = []
        locked_keys = set()
        for index, key in enumerate(keys):
            made_key = self.make_key(key, *args, **kwargs)
            value = self.lru.get(made_key, self.placeholder)
            if value is not self.placeholder:
                self.hits += 1
                results[index] = value
                if self.maxsize is not None:
                    self.lru.move_to_end(made_key)
            else:
                self.misses += 1
                missing_keys.append(key)
                made_keys.append(made_key)
                indexes.append(index)
                if made_key not in self.locks:
                    self.locks[made_key] = True
                    locked_keys.add(made_key)
        if missing_keys:
            try:
                values = self.singleflight.mget(missing_keys, *args, **kwargs)
            finally:
                locked_keys = {k for k in locked_keys if self.locks.pop(k)}
            for index, made_key, value in zip(indexes, made_keys, values):
                assert not isinstance(value, (list, set, dict)), 'use tuple, frozenset, MappingProxyType instead'
                results[index] = value
                if made_key in locked_keys:
                    self._set_value(made_key, value)
        return results

    def listen(self, invalidator: Invalidator, group: str, convert: Callable = None):
        @invalidator(group)
        def invalidate(key: str):
            self.invalids += 1
            if not key:
                self.lru.clear()
                for made_key in self.locks:
                    self.locks[made_key] = False
                return
            if convert:
                key_or_keys = convert(key)
                made_keys = key_or_keys if isinstance(key_or_keys, (list, set)) else [key_or_keys]
            else:
                made_keys = [self.make_key(key)]
            for made_key in made_keys:
                self.lru.pop(made_key, None)
                if made_key in self.locks:
                    self.locks[made_key] = False


class TtlCache(Cache[T]):
    Pair = namedtuple('Pair', ['value', 'expire_at'])

    def mget(self, keys, *args, **kwargs) -> Sequence[T]:
        results = [self.placeholder] * len(keys)
        missing_keys = []
        made_keys = []
        indexes = []
        locked_keys = set()
        now = time.time()
        for index, key in enumerate(keys):
            made_key = self.make_key(key, *args, **kwargs)
            pair = self.lru.get(made_key, self.placeholder)
            if pair is not self.placeholder and pair.expire_at > now:
                self.hits += 1
                results[index] = pair.value
                if self.maxsize is not None:
                    self.lru.move_to_end(made_key)
            else:
                self.misses += 1
                missing_keys.append(key)
                made_keys.append(made_key)
                indexes.append(index)
                if made_key not in self.locks:
                    self.locks[made_key] = True
                    locked_keys.add(made_key)
        if missing_keys:
            try:
                tuples = self.singleflight.mget(missing_keys, *args, **kwargs)
            finally:
                locked_keys = {k for k in locked_keys if self.locks.pop(k)}
            for index, made_key, (value, expire) in zip(indexes, made_keys, tuples):
                assert not isinstance(value, (list, set, dict)), 'use tuple, frozenset, MappingProxyType instead'
                results[index] = value
                if made_key in locked_keys:
                    pair = TtlCache.Pair(value, expire_at(expire))
                    self._set_value(made_key, pair)
        return results


class FullMixin(Generic[T]):
    invalids: int
    mget: Callable

    def __init__(self, *, get_values):
        self._get_values = get_values
        self._version = 0
        self._values = []
        self._expire_at = float('-inf')

    @property
    @singleflight
    def values(self) -> Sequence[T] | LazySequence[T]:
        if self._version == self.invalids and self._expire_at > time.time():
            return self._values
        invalids = self.invalids
        values, expire = self._get_values()  # invalidate events may happen simultaneously
        self._values = values
        self._expire_at = expire_at(expire)
        self._version = invalids
        return values

    def cached(self, maxsize=128, typed=False, get_expire=None):
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

        return decorator


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

        cache = TtlCache(get=get, maxsize=maxsize)

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            key = (args, *kwargs.items())
            return cache.get(key)

        wrapper.cache = cache
        return wrapper

    return decorator


class RedisCache(Singleflight[T]):
    def __init__(self, redis: Redis | RedisCluster, slow_mget, make_key, serialize, deserialize, expire: timedelta,
                 prefix='PLACEHOLDER', timeout=timedelta(milliseconds=100), retry=3):
        super().__init__(mget=self._mget, make_key=make_key)
        self.redis = redis
        self.slow_mget = slow_mget
        self.make_key = make_key
        self.serialize = serialize
        self.deserialize = deserialize
        self.expire = expire
        self.prefix = prefix
        self.timeout = timeout
        self.retry = retry

    def _mget(self, keys, *args, **kwargs):
        placeholder = self.prefix + str(uuid.uuid4())
        made_keys = []
        todo_indexes = []
        wait_indexes = []
        with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                made_key = self.make_key(key, *args, **kwargs)
                made_keys.append(made_key)
                pipe.set(made_key, placeholder, nx=True, px=self.timeout * self.retry, get=True)
            values = pipe.execute()
        for index, value in enumerate(values):
            if value is None:
                todo_indexes.append(index)
            elif value.startswith(self.prefix):
                wait_indexes.append(index)
            else:
                values[index] = self.deserialize(value)
        if todo_indexes:
            new_values = self.slow_mget([keys[index] for index in todo_indexes], *args, **kwargs)
            with self.redis.pipeline(transaction=False) as pipe:
                script = Script(pipe)
                for index, value in zip(todo_indexes, new_values):
                    values[index] = value
                    script.compare_set(made_keys[index], placeholder, self.serialize(value), self.expire)
                pipe.execute()
        retry = 0
        while wait_indexes:
            gevent.sleep(self.timeout.total_seconds())
            mget_nonatomic = self.redis.mget_nonatomic if isinstance(self.redis, RedisCluster) else self.redis.mget
            new_values = mget_nonatomic(*[made_keys[index] for index in wait_indexes])
            fail_indexes = []
            for index, value in zip(wait_indexes, new_values):
                if value is None or value.startswith(self.prefix):
                    fail_indexes.append(index)
                else:
                    values[index] = self.deserialize(value)
            if not fail_indexes:
                break
            if retry >= self.retry:
                fail_keys = ', '.join(f'`{keys[index]}`' for index in fail_indexes)
                raise ValueError(f'{fail_keys} not resolve')
            wait_indexes = fail_indexes
            retry += 1
        return values
