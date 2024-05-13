# -*- coding: utf-8 -*-
import bisect
from itertools import islice
from .singleflight import singleflight


def batched(iterable, n):
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


class LazySequence:
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

    def slice(self, start):
        while True:
            while start < len(self._values):
                yield self._values[start]
                start += 1
            if self._done:
                return
            self._load()

    def gt_slice(self, x, key=None):
        start = 0
        while start >= len(self._values) and not self._done:
            self._load()
            start = bisect.bisect_right(self._values, x, lo=start, key=key)
        return self.slice(start)
