# -*- coding: utf-8 -*-
from __future__ import annotations
import time
import heapq
import threading
from datetime import timedelta, datetime
from typing import List, Union, Optional, Callable
import gevent
from .utils import LogSuppress, func_desc
from .executor import Executor


class Handle:
    __slots__ = ['when', 'callback']

    def __init__(self, callback: Callable, when):
        assert callback is not None
        self.callback = callback
        self.when = when

    def cancel(self):
        self.callback = None

    @property
    def cancelled(self):
        return self.callback is None

    def __lt__(self, other: Handle):
        return self.when < other.when

    def __call__(self, *args, **kwargs):
        if cb := self.callback:
            cb(*args, **kwargs)

    def __str__(self):
        return f'handle: {func_desc(self.callback)} at: {datetime.fromtimestamp(self.when)}'


class Scheduler:
    def __init__(self, executor=None):
        self._cond = threading.Condition()
        self._executor = executor or Executor(name='scheduler')
        self._handles = []  # type: List[Handle]
        gevent.spawn(self._run)

    def call_later(self, callback: Callable, delay: Union[float, timedelta]) -> Handle:
        if isinstance(delay, timedelta):
            delay = delay.total_seconds()
        return self.call_at(callback, time.time() + delay)

    def call_at(self, callback: Callable, when: Union[float, datetime]) -> Handle:
        assert callable(callback)
        if isinstance(when, datetime):
            when = when.timestamp()
        with self._cond:
            handle = Handle(callback, when)
            heapq.heappush(self._handles, handle)
            if self._handles[0] is handle:  # wakeup
                self._cond.notify()
            return handle

    def _run(self):
        with self._cond:
            while True:
                now = time.time()
                while self._handles and self._handles[0].when <= now:
                    handle = heapq.heappop(self._handles)  # type: Handle
                    if not handle.cancelled:
                        self._executor.submit(handle)
                timeout = self._handles[0].when - now if self._handles else None
                self._cond.wait(timeout)

    def __call__(self, period: Union[float, timedelta]):
        def decorator(f):
            pc = PeriodicCallback(self, f, period)
            f.stop = pc.stop
            return f

        return decorator


class PeriodicCallback:
    __slots__ = ['_scheduler', '_callback', '_period', '_handle']

    def __init__(self, scheduler: Scheduler, callback: Callable, period: Union[float, timedelta]):
        if isinstance(period, timedelta):
            period = period.total_seconds()
        assert callable(callback) and period > 0
        self._scheduler = scheduler
        self._callback = callback
        self._period = period
        self._handle = None  # type: Optional[Handle]
        self._schedule_next()

    def _run(self):
        if cb := self._callback:
            with LogSuppress():
                cb()
        if self._handle:
            self._schedule_next()

    def _schedule_next(self):
        self._handle = self._scheduler.call_later(self._run, self._period)

    def stop(self):
        self._callback = None
        if self._handle:
            self._handle.cancel()
            self._handle = None
