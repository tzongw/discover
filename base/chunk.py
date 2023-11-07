# -*- coding: utf-8 -*-
from typing import Optional
import bisect
from itertools import islice
from concurrent.futures import Future


def batched(iterable, n):
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


class LazySequence:
    def __init__(self, get_more):
        self._values = []
        self._get_more = get_more
        self._cursor = None
        self._done = False
        self._fut = None  # type: Optional[Future]

    def _load(self):
        assert not self._done
        if self._fut:
            return self._fut.result()
        self._fut = Future()
        try:
            values, self._cursor = self._get_more(self._cursor)
            self._values += values
            if self._cursor is None:
                self._done = True
            self._fut.set_result(None)
        except Exception as e:
            self._fut.set_exception(e)
        finally:
            self._fut = None

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
