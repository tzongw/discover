# -*- coding: utf-8 -*-
from concurrent.futures import Future
from typing import Optional, Generator

_sentinel = object()


class _Chunk:
    def __init__(self, first, iterator, batch):
        self.next = first
        self.iterator = iterator
        self.batch = batch

    def __iter__(self):
        return self

    def __next__(self):
        if self.next is _sentinel:
            raise StopIteration
        item = self.next
        self.batch -= 1
        if self.batch > 0:
            self.next = next(self.iterator, _sentinel)
        else:
            self.next = _sentinel
        return item


def chunks(iterable, batch):
    iterator = iter(iterable)
    while True:
        first = next(iterator, _sentinel)
        if first is _sentinel:
            return
        yield _Chunk(first, iterator, batch)


class LazyValues:
    def __init__(self, generator: Generator):
        self._generator = generator
        self._values = []
        self._fut = None  # type: Optional[Future]
        self._done = False

    def __iter__(self):
        def lazy_values():
            index = 0
            while True:
                while index < len(self._values):
                    yield self._values[index]
                    index += 1
                if self._done:
                    return
                if self._fut:
                    self._fut.result()
                else:
                    self._fut = Future()
                    try:
                        for v in self._generator:
                            self._values.append(v)
                            break
                        else:
                            self._done = True
                        self._fut.set_result(None)
                        self._fut = None
                    except Exception as e:
                        self._fut.set_exception(e)
                        raise

        return lazy_values()
