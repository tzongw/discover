# -*- coding: utf-8 -*-
from typing import Optional
from concurrent.futures import Future

sentinel = object()


class Chunk:
    def __init__(self, first, iterator, batch):
        self.next = first
        self.iterator = iterator
        self.batch = batch

    def __iter__(self):
        return self

    def __next__(self):
        if self.next is sentinel:
            raise StopIteration
        item = self.next
        self.batch -= 1
        if self.batch > 0:
            self.next = next(self.iterator, sentinel)
        else:
            self.next = sentinel
        return item


def chunks(iterable, batch):
    iterator = iter(iterable)
    while True:
        first = next(iterator, sentinel)
        if first is sentinel:
            return
        yield Chunk(first, iterator, batch)


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
