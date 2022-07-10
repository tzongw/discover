# -*- coding: utf-8 -*-
import time
from collections import namedtuple, OrderedDict
from typing import TypeVar, Optional, Generic, Callable

from .utils import SingleFlight
from .invalidator import Invalidator

T = TypeVar('T')


class Cache(Generic[T]):
    # placeholder to avoid race conditions, see https://redis.io/docs/manual/client-side-caching/
    placeholder = object()

    def __init__(self, get=None, mget=None, maxsize=8192):
        self.single_flight = SingleFlight(get, mget)
        self.lru = OrderedDict()
        self.maxsize = maxsize

    def get(self, key, *args, **kwargs) -> Optional[T]:
        value, = self.mget([key], *args, **kwargs)
        return value

    def set(self, key, value):
        self.lru[key] = value
        self.lru.move_to_end(key)  # in case replace
        if self.maxsize and len(self.lru) > self.maxsize:
            self.lru.popitem(last=False)

    def mget(self, keys, *args, **kwargs):
        results = {}
        new_keys = []
        for key in keys:
            value = self.lru.get(key, self.placeholder)
            if value is not self.placeholder:
                self.lru.move_to_end(key)
                results[key] = value
            else:
                new_keys.append(key)
        if new_keys:
            for key in new_keys:
                self.set(key, self.placeholder)
            values = self.single_flight.mget(new_keys, *args, **kwargs)
            for key, value in zip(new_keys, values):
                results[key] = value
                if key in self.lru:
                    self.lru[key] = value
        return [results[key] for key in keys]

    def listen(self, invalidator: Invalidator, prefix: str, converter: Optional[Callable] = None):
        @invalidator.handler(prefix)
        def invalidate(key: str):
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
            pair = self.lru.get(key, self.placeholder)
            if pair is not self.placeholder and (pair.expire_at is None or pair.expire_at > time.time()):
                self.lru.move_to_end(key)
                results[key] = pair.value
            else:
                new_keys.append(key)
        if new_keys:
            for key in new_keys:
                self.set(key, self.placeholder)
            tuples = self.single_flight.mget(new_keys, *args, **kwargs)
            for key, (value, ttl) in zip(new_keys, tuples):
                results[key] = value
                if key in self.lru:
                    pair = TTLCache.Pair(value, time.time() + ttl if ttl >= 0 else None)
                    self.lru[key] = pair
        return [results[key] for key in keys]
