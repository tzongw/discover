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
    def __init__(self, iterable):
        self._values = []
        self._cursor = iter(iterable)
        self._done = False

    @property
    def done(self):
        return self._done

    @singleflight
    def _load(self):
        assert not self._done
        try:
            self._values += next(self._cursor)
        except StopIteration:
            self._done = True

    def __iter__(self):
        return iter(self._values) if self._done else self.slice(0)

    def slice(self, pos, load_times=None) -> Iterator[T]:
        while True:
            while pos < len(self._values):
                yield self._values[pos]
                pos += 1
            if self._done or load_times == 0:
                return
            if load_times is not None:
                load_times -= 1
            self._load()

    def gt_slice(self, x, key=None, load_times=None):
        pos = 0
        while True:
            pos = bisect.bisect_right(self._values, x, lo=pos, key=key)
            if pos < len(self._values) or self._done or load_times == 0:
                break
            if load_times is not None:
                load_times -= 1
            self._load()
        return self.slice(pos, load_times)


if __name__ == '__main__':
    def get_more():
        for c in batched(range(10), 3):
            print('loading', c)
            yield c


    lazy = LazySequence(get_more())
    for i in range(10):
        gt = next(iter(lazy.gt_slice(i)), None)
        print(i, gt)
