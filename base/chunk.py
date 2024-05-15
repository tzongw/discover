# -*- coding: utf-8 -*-
import bisect
from itertools import islice
from typing import TypeVar, Generic, Iterator
from .singleflight import singleflight

T = TypeVar('T')


def batched(iterable, n):
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


class LazySequence(Generic[T]):
    def __init__(self, get_more):
        self._values = []
        self._get_more = get_more
        self._done = False

    @singleflight
    def _load(self):
        assert not self._done
        values = self._get_more()
        if not values:
            self._done = True
            return
        self._values += values

    def __iter__(self):
        return self.slice(0)

    def slice(self, pos) -> Iterator[T]:
        while True:
            while pos < len(self._values):
                yield self._values[pos]
                pos += 1
            if self._done:
                return
            self._load()

    def gt_slice(self, x, key=None):
        pos = 0
        while pos >= len(self._values) and not self._done:
            self._load()
            pos = bisect.bisect_right(self._values, x, lo=pos, key=key)
        return self.slice(pos)
