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


class LoadTimesError(Exception):
    pass


class LazySequence(Generic[T]):
    def __init__(self, get_more):
        self._values = []
        self._get_more = get_more
        self._done = False

    @singleflight
    def _load(self):
        assert not self._done
        values = self._get_more()
        if values is None:
            self._done = True
        else:
            self._values += values

    def __iter__(self):
        return iter(self._values) if self._done else self.slice()

    def slice(self, pos=0, load_times=3) -> Iterator[T]:
        while True:
            while pos < len(self._values):
                yield self._values[pos]
                pos += 1
            if self._done:
                return
            if load_times is not None:
                if load_times <= 0:
                    raise LoadTimesError
                load_times -= 1
            self._load()

    def gt_slice(self, x, key=None, load_times=3):
        pos = 0
        while True:
            pos = bisect.bisect_right(self._values, x, lo=pos, key=key)
            if pos < len(self._values) or self._done:
                break
            if load_times is not None:
                if load_times <= 0:
                    raise LoadTimesError
                load_times -= 1
            self._load()
        return self.slice(pos, load_times)


if __name__ == '__main__':
    def _get_more():
        global last_id
        if last_id >= 10:
            return
        values = range(last_id, min(last_id + 3, 10))
        last_id += 3
        print('loading', last_id, values)
        return values


    last_id = 0
    lazy = LazySequence(_get_more)
    for i in range(10):
        gt = next(iter(lazy.gt_slice(i)), None)
        print(i, gt)
