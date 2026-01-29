# -*- coding: utf-8 -*-
from __future__ import annotations
import time
import heapq
import functools
from datetime import timedelta, datetime
from typing import List, Union, Callable
import gevent
from gevent.event import Event
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


class Proxy:
    __slots__ = ['_handle']

    def __init__(self, handle: Handle):
        self._handle = handle

    def reset(self, handle: Handle):
        self._handle = handle

    def cancel(self):
        self._handle.cancel()

    @property
    def cancelled(self):
        return self._handle.cancelled


class Scheduler:
    def __init__(self):
        self._event = Event()
        self._executor = Executor(name='scheduler')
        self._handles = []  # type: List[Handle]
        self._stopped = False
        gevent.spawn(self._run)

    def join(self, timeout=None):
        self._stopped = True
        return self._executor.join(timeout)

    def call_later(self, callback: Callable, delay: Union[float, timedelta]) -> Handle:
        if isinstance(delay, timedelta):
            delay = delay.total_seconds()
        return self.call_at(callback, time.time() + delay)

    def call_at(self, callback: Callable, when: Union[float, datetime]) -> Handle:
        assert callable(callback)
        if isinstance(when, datetime):
            when = when.timestamp()
        handle = Handle(callback, when)
        heapq.heappush(self._handles, handle)
        if self._handles[0] is handle:  # wakeup
            self._event.set()
        return handle

    def call_repeat(self, callback: Callable, interval: Union[float, timedelta]) -> Handle | Proxy:
        @functools.wraps(callback)
        def run():
            with LogSuppress():
                callback()
            if not proxy.cancelled:
                proxy.reset(self.call_later(run, interval))

        proxy = Proxy(self.call_later(run, interval))
        return proxy

    def _run(self):
        while not self._stopped:
            now = time.time()
            while self._handles and self._handles[0].when <= now:
                handle = heapq.heappop(self._handles)  # type: Handle
                if not handle.cancelled:
                    self._executor.submit(handle)
            timeout = self._handles[0].when - now if self._handles else None
            self._event.clear()
            self._event.wait(timeout)

    def __call__(self, interval: Union[float, timedelta]):
        def decorator(f):
            self.call_repeat(f, interval)
            return f

        return decorator
