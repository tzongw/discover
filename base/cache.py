# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from collections import namedtuple, OrderedDict
from typing import TypeVar, Optional, Generic, Callable
from concurrent.futures import Future
import functools

from .utils import SingleFlight, make_key
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
    # placeholder to avoid race conditions, see https://redis.io/docs/manual/client-side-caching/
    placeholder = object()

    def __init__(self, *, get=None, mget=None, maxsize: Optional[int] = 8192):
        self.single_flight = SingleFlight(get=get, mget=mget)
        self.lru = OrderedDict()
        self.maxsize = maxsize
        self.full_cached = False

    def get(self, key, *args, **kwargs) -> Optional[T]:
        value, = self.mget([key], *args, **kwargs)
        return value

    def _set(self, key, value):
        self.lru[key] = value
        if self.maxsize is not None:
            self.lru.move_to_end(key)  # in case replace
            if len(self.lru) > self.maxsize:
                self.lru.popitem(last=False)

    def mget(self, keys, *args, **kwargs):
        results = []
        missed_keys = []
        indexes = []
        for index, key in enumerate(keys):
            made_key = make_key(key, *args, **kwargs)
            value = self.lru.get(made_key, self.placeholder)
            if value is not self.placeholder:
                results.append(value)
                if self.maxsize is not None:
                    self.lru.move_to_end(made_key)
            else:
                missed_keys.append(key)
                indexes.append(index)
                results.append(self.placeholder)
                self._set(made_key, self.placeholder)
        if missed_keys:
            values = self.single_flight.mget(missed_keys, *args, **kwargs)
            for key, value, index in zip(missed_keys, values, indexes):
                results[index] = value
                made_key = make_key(key, *args, **kwargs)
                if made_key in self.lru:
                    self.lru[made_key] = value
        return results

    def listen(self, invalidator: Invalidator, prefix: str, converter: Optional[Callable] = None):
        @invalidator.handler(prefix)
        def invalidate(key: str):
            self.full_cached = False
            if not key:
                self.lru.clear()
            elif self.lru:
                if converter:
                    key = converter(key)
                else:
                    key = key.split(invalidator.sep, maxsplit=1)[1]
                    key = type(next(iter(self.lru)))(key)
                self.lru.pop(key, None)


class TTLCache(Cache[T]):
    Pair = namedtuple('Pair', ['value', 'expire_at'])

    def mget(self, keys, *args, **kwargs):
        results = []
        missed_keys = []
        indexes = []
        for index, key in enumerate(keys):
            made_key = make_key(key, *args, **kwargs)
            pair = self.lru.get(made_key, self.placeholder)
            if pair is not self.placeholder and (pair.expire_at is None or pair.expire_at > datetime.now()):
                results.append(pair.value)
                if self.maxsize is not None:
                    self.lru.move_to_end(made_key)
            else:
                missed_keys.append(key)
                indexes.append(index)
                results.append(self.placeholder)
                self._set(made_key, self.placeholder)
        if missed_keys:
            tuples = self.single_flight.mget(missed_keys, *args, **kwargs)
            for key, (value, expire), index in zip(missed_keys, tuples, indexes):
                results[index] = value
                made_key = make_key(key, *args, **kwargs)
                if made_key in self.lru:
                    pair = TTLCache.Pair(value, expire_at(expire))
                    self.lru[made_key] = pair
        return results


class FullCache(Cache[T]):

    def __init__(self, *, mget, get_keys, get_expire=None):
        super().__init__(mget=mget, maxsize=None)
        self.get_keys = get_keys
        self.get_expire = get_expire
        self._fut = None  # type: Optional[Future]
        self._version = 0
        self._values = []
        self._expire_at = None

    @property
    def version(self):
        self.values()
        return self._version

    def values(self):
        if self._fut:
            return self._fut.result()
        if self.full_cached and (self._expire_at is None or self._expire_at > datetime.now()):
            return self._values
        self._fut = Future()
        self.full_cached = True
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

    def cached(self, maxsize=128, typed=False):
        def decorator(f):
            @functools.lru_cache(maxsize, typed)
            def inner(version, *args, **kwargs):
                return f(*args, **kwargs)

            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                return inner(self.version, *args, **kwargs)

            return wrapper

        return decorator
