# -*- coding: utf-8 -*-
import logging
import sys
from concurrent.futures import Future
from gevent import queue
import gevent
from typing import Callable


class _WorkItem:
    __slots__ = ["future", "fn", "args", "kwargs"]

    def __init__(self, fut: Future, fn, *args, **kwargs):
        self.future = fut
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if self.future.done():
            return
        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:
            logging.exception(f'')
            self.future.set_exception(exc)
        else:
            self.future.set_result(result)


class Executor:
    def __init__(self, max_workers=sys.maxsize, idle=600):
        self._max_workers = max_workers
        self._workers = 0
        self._available_workers = 0
        self._items = queue.Queue()
        self._idle = idle

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        assert callable(fn)
        fut = Future()
        item = _WorkItem(fut, fn, *args, **kwargs)
        self._items.put(item)
        logging.debug(f'+job {self}')
        self._adjust_workers()
        return fut

    def gather(self, *fns):
        return [fut.result() for fut in [self.submit(fn) for fn in fns]]

    def _adjust_workers(self):
        if len(self._items) > self._available_workers and self._workers < self._max_workers:
            self._workers += 1
            self._available_workers += 1
            gevent.spawn(self._worker)
            logging.debug(f'+worker {self}')

    def __str__(self):
        return f'pending: {len(self._items)} available: {self._available_workers} workers: {self._workers}'

    def _worker(self):
        try:
            while True:
                item = self._items.get(timeout=self._idle)  # type: _WorkItem
                self._available_workers -= 1
                logging.debug(f'get job {self}')
                item.run()
                self._available_workers += 1
                logging.debug(f'-job {self}')
        except queue.Empty:
            logging.debug(f'worker idle exit')
        finally:
            self._available_workers -= 1
            self._workers -= 1
            logging.debug(f'-worker {self}')
