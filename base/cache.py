# -*- coding: utf-8 -*-
import time
from collections import namedtuple, OrderedDict
from typing import TypeVar, Optional, Generic, Callable
from concurrent.futures import Future

from .utils import SingleFlight, make_key
from .invalidator import Invalidator

T = TypeVar('T')


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
        self.lru.move_to_end(key)  # in case replace
        if self.maxsize and len(self.lru) > self.maxsize:
            self.lru.popitem(last=False)

    def mget(self, keys, *args, **kwargs):
        results = {}
        new_keys = []
        for key in keys:
            made_key = make_key(key, *args, **kwargs)
            value = self.lru.get(made_key, self.placeholder)
            if value is not self.placeholder:
                self.lru.move_to_end(made_key)
                results[key] = value
            else:
                new_keys.append(key)
        if new_keys:
            for key in new_keys:
                made_key = make_key(key, *args, **kwargs)
                self._set(made_key, self.placeholder)
            values = self.single_flight.mget(new_keys, *args, **kwargs)
            for key, value in zip(new_keys, values):
                results[key] = value
                made_key = make_key(key, *args, **kwargs)
                if made_key in self.lru:
                    self.lru[made_key] = value
        return [results[key] for key in keys]

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
        results = {}
        new_keys = []
        for key in keys:
            made_key = make_key(key, *args, **kwargs)
            pair = self.lru.get(made_key, self.placeholder)
            if pair is not self.placeholder and (pair.expire_at is None or pair.expire_at > time.time()):
                self.lru.move_to_end(made_key)
                results[key] = pair.value
            else:
                new_keys.append(key)
        if new_keys:
            for key in new_keys:
                made_key = make_key(key, *args, **kwargs)
                self._set(made_key, self.placeholder)
            tuples = self.single_flight.mget(new_keys, *args, **kwargs)
            for key, (value, ttl) in zip(new_keys, tuples):
                results[key] = value
                made_key = make_key(key, *args, **kwargs)
                if made_key in self.lru:
                    pair = TTLCache.Pair(value, time.time() + ttl if ttl >= 0 else None)
                    self.lru[made_key] = pair
        return [results[key] for key in keys]


class FullCache(Cache[T]):

    def __init__(self, *, mget, get_keys):
        super().__init__(mget=mget, maxsize=None)
        self.get_keys = get_keys
        self.fut = None  # type: Optional[Future]
        self._version = 0

    @property
    def version(self):
        self.values()
        return self._version

    def values(self, *args, **kwargs):
        if self.fut:
            return self.fut.result()
        if self.full_cached:
            return self.lru.values()
        self.fut = Future()
        self.full_cached = True
        try:
            keys = self.get_keys()
            self.mget(keys, *args, **kwargs)
            values = self.lru.values()
            self._version += 1
            self.fut.set_result(values)
            return values
        except Exception as e:
            self.fut.set_exception(e)
            self.full_cached = False
            raise
        finally:
            self.fut = None
