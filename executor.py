# -*- coding: utf-8 -*-
import logging
import sys
from concurrent.futures import Future
from gevent import queue
import gevent


class _WorkItem:
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
    def __init__(self, max_workers=None, idle=3600):
        self._max_workers = max_workers or sys.maxsize
        self._workers = 0
        self._unfinished = 0
        self._items = queue.Queue()
        self._idle = idle

    def submit(self, fn, *args, **kwargs) -> Future:
        self._unfinished += 1
        self._adjust_workers()
        fut = Future()
        item = _WorkItem(fut, fn, *args, **kwargs)
        self._items.put(item)
        return fut

    def gather(self, *fns):
        return [fut.result() for fut in [self.submit(fn) for fn in fns]]

    def _adjust_workers(self):
        if self._unfinished > self._workers and self._workers < self._max_workers:
            self._workers += 1
            gevent.spawn(self._worker)

    def _worker(self):
        try:
            while True:
                item = self._items.get(timeout=self._idle)
                item.run()
                self._unfinished -= 1
        except queue.Empty:
            logging.info(f'worker idle exit')
        finally:
            self._workers -= 1
