# -*- coding: utf-8 -*-
from __future__ import annotations
import time
import threading
from typing import List
import gevent
from .executor import Executor
import heapq
from typing import Optional, Callable
from .utils import LogSuppress


class Handle:
    __slots__ = ["when", "callback"]

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


class Schedule:
    def __init__(self, executor=None):
        self._cond = threading.Condition()
        self._executor = executor or Executor(name='schedule')
        self._handles = []  # type: List[Handle]
        gevent.spawn(self._run)

    def call_later(self, callback: Callable, delay) -> Handle:
        return self.call_at(callback, time.time() + delay)

    def call_at(self, callback: Callable, at) -> Handle:
        assert callable(callback)
        with self._cond:
            handle = Handle(callback, at)
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
                        self._executor.submit(handle.callback)
                timeout = (self._handles[0].when - now) if self._handles else None
                self._cond.wait(timeout)


class PeriodicCallback:
    __slots__ = ["_schedule", "_callback", "_period", "_handle"]

    def __init__(self, schedule: Schedule, callback: Callable, period):
        assert callable(callback) and period > 0
        self._schedule = schedule
        self._callback = callback
        self._period = period
        self._handle = None  # type: Optional[Handle]
        self.start()

    def _run(self):
        with LogSuppress(Exception):
            self._callback()
        if self._handle:
            self._schedule_next()

    def _schedule_next(self):
        self._handle = self._schedule.call_later(self._run, self._period)

    def start(self):
        if self._handle is None:
            self._schedule_next()

    def stop(self):
        if self._handle:
            self._handle.cancel()
            self._handle = None
