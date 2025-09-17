# -*- coding: utf-8 -*-
import logging
import time
from typing import Callable, Iterable
from functools import partial
from gevent import queue
from gevent.event import Event
from gevent.event import AsyncResult as Future
import gevent
from .utils import func_desc


class _WorkItem:
    __slots__ = ['future', 'fn', 'args', 'kwargs']

    def __init__(self, fut: Future, fn, args, kwargs):
        self.future = fut
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if self.future is None or self.future.done():
            return
        fut = self.future
        self.future = None
        try:
            result = self.fn(*self.args, **self.kwargs)
        except Exception as e:
            fut.set_exception(e)
            raise
        else:
            fut.set_result(result)

    def __str__(self):
        return f'fn: {func_desc(self.fn)} args: {self.args} kwargs: {self.kwargs}'


class Executor:
    def __init__(self, max_workers=128, queue_size=None, idle_time=60, slow_time=3, name='executor'):
        self._max_workers = max_workers
        self._workers = 0
        self._unfinished = 0
        self._done = Event()
        self._done.set()
        assert queue_size is None or queue_size > 0
        self._items = queue.Queue(queue_size)
        self._idle_time = idle_time
        self._slow_time = slow_time
        self._name = name
        self._overload = False

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        assert callable(fn)
        self._unfinished += 1
        self._done.clear()
        self._adjust_workers()
        fut = Future()
        item = _WorkItem(fut, fn, args, kwargs)
        self._items.put(item)
        if not self._overload and self._unfinished >= self._max_workers * 2:
            self._overload = True
            logging.warning(f'+ overload {self} {item}')
        return fut

    def join(self, timeout=None):
        return self._done.wait(timeout=timeout)

    def gather(self, fns: Iterable[Callable]):
        futures = [self.submit(fn) for fn in fns]
        return [fut.result() for fut in futures]

    def map(self, fn: Callable, args: Iterable):
        return self.gather([partial(fn, arg) for arg in args])

    def _adjust_workers(self):
        if self._workers < self._unfinished and self._workers < self._max_workers:
            self._workers += 1
            gevent.spawn(self._worker)
            logging.debug(f'+ worker {self}')

    def __str__(self):
        return f'{self._name}: unfinished: {self._unfinished}, workers: {self._workers} queue: {self._items.qsize()}' \
               f' overload: {self._overload}'

    def _worker(self):
        try:
            while True:
                item = self._items.get(timeout=self._idle_time)  # type: _WorkItem
                start = time.monotonic()
                try:
                    item.run()
                except Exception:
                    logging.exception(f'run error {self} {item}')
                t = time.monotonic() - start
                if t > self._slow_time:
                    logging.warning(f'+ slow task {t} {self} {item}')
                self._unfinished -= 1
                if self._unfinished == 0:
                    self._done.set()
                if self._overload and self._unfinished <= self._max_workers:
                    self._overload = False
                    logging.warning(f'- overload {self} {item}')
        except queue.Empty:
            logging.debug(f'worker idle exit')
        finally:
            self._workers -= 1
            logging.debug(f'- worker {self}')
            self._adjust_workers()  # race


class WaitGroup(Executor):
    def __init__(self, max_workers=10, queue_size=1, idle_time=5, slow_time=20, name='wait_group'):
        super().__init__(max_workers, queue_size, idle_time, slow_time, name)
