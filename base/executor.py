# -*- coding: utf-8 -*-
import logging
import time
from concurrent.futures import Future
from weakref import WeakSet
from gevent import queue
import gevent
from typing import Callable
from functools import partial
from .utils import func_desc


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
            logging.exception(f'{self}')
            self.future.set_exception(exc)
        else:
            self.future.set_result(result)
        finally:
            self.future = None

    def __str__(self):
        return f'{func_desc(self.fn)}:{self.args}:{self.kwargs}'


class Executor:
    def __init__(self, max_workers=128, queue_size=None, idle=60, slow_log=3, name='unnamed'):
        self._max_workers = max_workers
        self._workers = 0
        self._unfinished = 0
        self._items = queue.Channel() if queue_size == 0 else queue.Queue(queue_size)
        self._idle = idle
        self._slow_log = slow_log
        self._name = name
        self._overload = False
        self._slow_count = 0

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        assert callable(fn)
        self._unfinished += 1
        self._adjust_workers()
        fut = Future()
        item = _WorkItem(fut, fn, *args, **kwargs)
        start = time.time()
        self._items.put(item)
        t = time.time() - start
        if t >= self._slow_log:
            self._slow_count += 1
            if self._slow_count <= 10:
                logging.warning(f'+ slow put {t} {self} {item}')
        elif self._slow_count > 0:
            logging.warning(f'- slow put {t} {self} {item}')
            self._slow_count = 0
        if not self._overload and self._items.qsize() >= self._max_workers * 2:
            self._overload = True
            logging.warning(f'+ overload {self} {item}')
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
            logging.debug(f'+ worker {self}')

    def __str__(self):
        return f'{self._name}: unfinished: {self._unfinished}, workers: {self._workers} queue: {self._items.qsize()}' \
               f' overload: {self._overload} slow_count: {self._slow_count}'

    def _worker(self):
        try:
            while True:
                item = self._items.get(block=self._idle > 0, timeout=self._idle)  # type: _WorkItem
                if self._overload and self._items.qsize() <= self._max_workers // 2:
                    self._overload = False
                    logging.warning(f'- overload {self} {item}')
                start = time.time()
                item.run()
                t = time.time() - start
                if t > self._slow_log:
                    logging.warning(f'+ slow task {t} {self} {item}')
                self._unfinished -= 1
        except queue.Empty:
            logging.debug(f'worker idle exit')
        finally:
            self._workers -= 1
            logging.debug(f'- worker {self}')


class WaitGroup(Executor):
    def __init__(self, max_workers=10, slow_log=600, name='wait_group'):
        super().__init__(max_workers, queue_size=max_workers, idle=0, slow_log=slow_log, name=name)
        self._futures = WeakSet()

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        fut = super().submit(fn, *args, **kwargs)
        self._futures.add(fut)
        return fut

    def join(self):
        while self._futures:
            fut = self._futures.pop()
            fut.exception()
