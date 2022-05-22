# -*- coding: utf-8 -*-
import time
from collections import namedtuple, OrderedDict
from typing import TypeVar, Optional, Generic

from .utils import SingleFlight
from .invalidator import Invalidator

T = TypeVar('T')


class Cache(Generic[T]):
    placeholder = object()

    def __init__(self, f, maxsize=8192):
        self.single_flight = SingleFlight(f)
        self.lru = OrderedDict()
        self.maxsize = maxsize

    def get(self, key, *args, **kwargs) -> Optional[T]:
        # placeholder to avoid race conditions, see https://redis.io/docs/manual/client-side-caching/
        value = self.lru.get(key, self.placeholder)
        if value is not self.placeholder:
            self.lru.move_to_end(key)
            return value
        self.set(key, self.placeholder)
        value = self.single_flight.get(key, *args, **kwargs)
        if key in self.lru:
            self.lru[key] = value
        return value

    def set(self, key, value):
        self.lru[key] = value
        self.lru.move_to_end(key)  # in case replace
        if self.maxsize and len(self.lru) > self.maxsize:
            self.lru.popitem(last=False)

    def listen(self, invalidator: Invalidator, prefix: str):
        @invalidator.handler(prefix)
        def invalidate(key: str):
            if not key:
                self.lru.clear()
            elif self.lru:
                key = key.split(invalidator.sep, maxsplit=1)[1]
                key = type(next(iter(self.lru)))(key)
                self.lru.pop(key, None)


class TTLCache(Cache[T]):
    Pair = namedtuple('Pair', ['value', 'expire_at'])

    def get(self, key, *args, **kwargs) -> Optional[T]:
        pair = self.lru.get(key, self.placeholder)
        if pair is not self.placeholder and (pair.expire_at is None or pair.expire_at > time.time()):
            self.lru.move_to_end(key)
            return pair.value
        self.set(key, self.placeholder)
        value, ttl = self.single_flight.get(key, *args, **kwargs)
        if key in self.lru:
            pair = TTLCache.Pair(value, time.time() + ttl if ttl >= 0 else None)
            self.lru[key] = pair
        return value
