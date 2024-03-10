# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from collections import namedtuple, OrderedDict
from typing import TypeVar, Optional, Generic, Callable, Sequence
from concurrent.futures import Future
import functools

from . import utils
from .single_flight import SingleFlight
from .invalidator import Invalidator

T = TypeVar('T')


def expire_at(expire):
    if expire is None or isinstance(expire, datetime):
        return expire
    if isinstance(expire, (int, float)):
        return None if expire < 0 else datetime.now() + timedelta(seconds=expire)
    if isinstance(expire, timedelta):
        return datetime.now() + expire
    raise ValueError(f'{expire} not valid')


class Cache(Generic[T]):
    placeholder = object()

    def __init__(self, *, get=None, mget=None, maxsize: Optional[int] = 4096, make_key=utils.make_key):
        self.single_flight = SingleFlight(get=get, mget=mget, make_key=make_key)
        self.lru = OrderedDict()
        self.locks = {}  # https://redis.io/docs/manual/client-side-caching/#avoiding-race-conditions
        self.make_key = make_key
        self.maxsize = maxsize
        self.full_cached = False
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
        replace = key in self.lru
        self.lru[key] = value
        if self.maxsize is not None:
            if replace:
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
                values = self.single_flight.mget(missing_keys, *args, **kwargs)
            finally:
                locked_keys = {k for k in locked_keys if self.locks.pop(k)}
            for index, made_key, value in zip(indexes, made_keys, values):
                results[index] = value
                if made_key in locked_keys:
                    self._set_value(made_key, value)
        return results

    def listen(self, invalidator: Invalidator, group: str, handler: Optional[Callable] = None):
        @invalidator.handler(group)
        def invalidate(key: str, *args, **kwargs):
            self.full_cached = False
            self.invalids += 1
            if not key:
                self.lru.clear()
                for key in self.locks:
                    self.locks[key] = False
                return
            if handler:
                key_or_keys = handler(key, *args, **kwargs)
                keys = key_or_keys if isinstance(key_or_keys, (list, set)) else [key_or_keys]
                for key in keys:
                    self.lru.pop(key, None)
                    if key in self.locks:
                        self.locks[key] = False
            else:
                key = self.make_key(key, *args, **kwargs)
                self.lru.pop(key, None)
                if key in self.locks:
                    self.locks[key] = False


class TTLCache(Cache[T]):
    Pair = namedtuple('Pair', ['value', 'expire_at'])

    def mget(self, keys, *args, **kwargs) -> Sequence[T]:
        results = [self.placeholder] * len(keys)
        missing_keys = []
        made_keys = []
        indexes = []
        locked_keys = set()
        for index, key in enumerate(keys):
            made_key = self.make_key(key, *args, **kwargs)
            pair = self.lru.get(made_key, self.placeholder)
            if pair is not self.placeholder and (pair.expire_at is None or pair.expire_at > datetime.now()):
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
                tuples = self.single_flight.mget(missing_keys, *args, **kwargs)
            finally:
                locked_keys = {k for k in locked_keys if self.locks.pop(k)}
            for index, made_key, (value, expire) in zip(indexes, made_keys, tuples):
                results[index] = value
                if made_key in locked_keys:
                    pair = TTLCache.Pair(value, expire_at(expire))
                    self._set_value(made_key, pair)
        return results


class FullMixin(Generic[T]):
    full_cached: bool
    mget: Callable

    def __init__(self, *, get_keys, get_expire=None):
        self.get_keys = get_keys
        self.get_expire = get_expire
        self._fut = None  # type: Optional[Future]
        self._version = 0
        self._values = []
        self._expire_at = None
        self.full_hits = 0
        self.full_misses = 0

    def __str__(self):
        return f'full_hits: {self.full_hits} full_misses: {self.full_misses} {super().__str__()}'

    @property
    def values(self) -> Sequence[T]:
        if self._fut:
            self.full_misses += 1
            return self._fut.result()
        if self.full_cached and (self._expire_at is None or self._expire_at > datetime.now()):
            self.full_hits += 1
            return self._values
        self.full_misses += 1
        self._fut = Future()
        self.full_cached = True  # BEFORE `get_keys` and `mget`, invalidate events may happen simultaneously
        try:
            keys = self.get_keys()
            self._values = self.mget(keys)
            if self.get_expire:
                expire = self.get_expire(self._values)
                self._expire_at = expire_at(expire)
            self._version += 1
            self._fut.set_result(self._values)
            return self._values
        except Exception as e:
            self._fut.set_exception(e)
            self.full_cached = False
            raise
        finally:
            self._fut = None

    def cached(self, maxsize=128, typed=False, get_expire=None):
        def decorator(f):
            @functools.lru_cache(maxsize, typed)
            def inner(*args, **kwargs):
                self.full_hits -= 1  # full_hits will +1 in f redundantly because values was accessed in wrapper
                return f(*args, **kwargs)

            cache_version = self._version
            cache_expire: Optional[datetime] = None

            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                nonlocal cache_version
                nonlocal cache_expire
                values = self.values  # update version if needed
                if cache_version != self._version or (cache_expire and cache_expire < datetime.now()):
                    inner.cache_clear()
                    cache_version = self._version
                    if get_expire:
                        expire = get_expire(values)
                        cache_expire = expire_at(expire)
                return inner(*args, **kwargs)

            wrapper.cache_info = inner.cache_info
            wrapper.cache_clear = inner.cache_clear
            return wrapper

        return decorator


class FullCache(FullMixin[T], Cache[T]):
    def __init__(self, *, mget, maxsize: Optional[int] = 4096, make_key=utils.make_key, get_keys, get_expire=None):
        super(FullMixin, self).__init__(mget=mget, maxsize=maxsize, make_key=make_key)
        super().__init__(get_keys=get_keys, get_expire=get_expire)


class FullTTLCache(FullMixin[T], TTLCache[T]):
    def __init__(self, *, mget, maxsize: Optional[int] = 4096, make_key=utils.make_key, get_keys, get_expire=None):
        super(FullMixin, self).__init__(mget=mget, maxsize=maxsize, make_key=make_key)
        super().__init__(get_keys=get_keys, get_expire=get_expire)


def ttl_cache(expire, *, maxsize=128):
    def decorator(f):
        def get(_, *args, **kwargs):
            return f(*args, **kwargs), expire

        cache = TTLCache(get=get, maxsize=maxsize)

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return cache.get(None, *args, **kwargs)

        wrapper.cache = cache
        return wrapper

    return decorator
