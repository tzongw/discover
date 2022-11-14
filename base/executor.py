# -*- coding: utf-8 -*-
import logging
import time
from concurrent.futures import Future
from gevent import queue
import gevent
from typing import Callable
from functools import partial


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
    def __init__(self, max_workers=128, queue_size=None, idle=60, slow_log=1, name='unnamed'):
        self._max_workers = max_workers
        self._workers = 0
        self._unfinished = 0
        self._items = queue.Channel() if queue_size == 0 else queue.Queue(queue_size)
        self._idle = idle
        self._slow_log = slow_log
        self._name = name

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        assert callable(fn)
        self._unfinished += 1
        self._adjust_workers()
        fut = Future()
        item = _WorkItem(fut, fn, *args, **kwargs)
        start = time.time()
        self._items.put(item)
        if time.time() - start > self._slow_log:
            logging.warning(f'slow put {self} {item.fn} {item.args} {item.kwargs}')
        return fut

    def gather(self, *fns, block=True):
        futures = [self.submit(fn) for fn in fns]
        results = (fut.result() for fut in futures)
        return list(results) if block else results

    def map(self, fn: Callable, *args, block=True):
        return self.gather(*[partial(fn, arg) for arg in args], block=block)

    def _adjust_workers(self):
        if self._unfinished > self._workers and self._workers < self._max_workers:
            self._workers += 1
            gevent.spawn(self._worker)
            logging.debug(f'+worker {self}')

    def __str__(self):
        return f'{self._name}: unfinished: {self._unfinished}, workers: {self._workers}'

    def _worker(self):
        try:
            while True:
                item = self._items.get(timeout=self._idle)  # type: _WorkItem
                start = time.time()
                item.run()
                t = time.time() - start
                if t > self._slow_log:
                    fn = f'{item.fn.__module__}.{item.fn.__name__}'
                    logging.warning(f'slow task {t} {self} {fn} {item.args} {item.kwargs}')
                self._unfinished -= 1
        except queue.Empty:
            logging.debug(f'worker idle exit')
        finally:
            self._workers -= 1
            logging.debug(f'-worker {self}')
