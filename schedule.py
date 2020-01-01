# -*- coding: utf-8 -*-
from __future__ import annotations
import socket
import time
import threading
from typing import List
import gevent
from utils import LogSuppress
from executor import Executor
import heapq

MAXIMUM_TIMEOUT = 3600


class Handle:
    def __init__(self, callback, when):
        self.callback = callback
        self.when = when
        self.cancelled = False

    def cancel(self):
        self.cancelled = True

    def __lt__(self, other: Handle):
        return self.when < other.when


class Schedule:
    def __init__(self):
        self._stopped = False
        self._cond = threading.Condition()
        self._executor = Executor(max_workers=128)
        self._handles = []  # type: List[Handle]
        gevent.spawn(self._run)

    def call_later(self, callback, delay) -> Handle:
        return self.call_at(callback, time.time() + delay)

    def call_at(self, callback, at) -> Handle:
        with self._cond:
            handle = Handle(callback, at)
            heapq.heappush(self._handles, handle)
            if self._handles[0] is handle:  # wakeup
                self._cond.notify()
            return handle

    def _run(self):
        while True:
            with self._cond:
                timeout = None
                if self._handles:
                    when = self._handles[0].when
                    timeout = min(max(0, when - time.time()), MAXIMUM_TIMEOUT) + 0.01
                    timeout += 0.01  # sleep a little more
                self._cond.wait(timeout)
                now = time.time()
                while self._handles:
                    handle = self._handles[0]
                    if handle.when > now:
                        break
                    handle = heapq.heappop(self._handles)  # type: Handle
                    if not handle.cancelled:
                        self._executor.submit(handle.callback)
