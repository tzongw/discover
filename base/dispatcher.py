# -*- coding: utf-8 -*-
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta, datetime
from calendar import MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY
from typing import Iterable, Union
from binascii import crc32
from .executor import Executor
from .utils import variadic_args

WEEKDAY = [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY]
WEEKEND = [SATURDAY, SUNDAY]


class Manager:
    def __init__(self):
        self._handlers = {}

    def handle(self, key, *args, **kwargs):
        return self._handlers[key](*args, **kwargs)

    def __call__(self, key):
        def decorator(f):
            assert key not in self._handlers
            self._handlers[key] = f
            return f

        return decorator


class Dispatcher:
    def __init__(self, executor: Executor):
        self._handlers = defaultdict(list)
        self._executor = executor

    def keys(self):
        return self._handlers.keys()

    def dispatch(self, key, *args, **kwargs):
        handlers = self._handlers.get(key) or []
        for handle in handlers:
            self._executor.submit(handle, *args, **kwargs)

    def signal(self, event):
        cls = event.__class__
        self.dispatch(cls, event)

    def __call__(self, key):
        def decorator(f):
            self._handlers[key].append(variadic_args(f))
            return f

        return decorator


@dataclass(frozen=True)
class _Crontab:
    year: Union[None, int, Iterable]
    month: Union[None, int, Iterable]
    day: Union[None, int, Iterable]
    hour: Union[None, int, Iterable]
    minute: Union[None, int, Iterable]
    second: Union[None, int, Iterable]
    weekday: Union[None, int, Iterable]

    def __contains__(self, cron: '_Crontab'):
        return all(pattern is None or (value == pattern if isinstance(pattern, int) else value in pattern) for
                   value, pattern in zip(cron.__dict__.values(), self.__dict__.values()))


class TimeDispatcher(Dispatcher):
    def __init__(self, executor: Executor):
        super().__init__(executor)
        self._periodic_handlers = []
        self._crontab_handlers = []

    def dispatch_tick(self, ts: int):
        dt = datetime.fromtimestamp(ts)
        for period, remain, handle in self._periodic_handlers:
            if ts % period == remain:
                self._executor.submit(handle, dt)
        now = _Crontab(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.weekday())
        for cron, handle in self._crontab_handlers:
            if now in cron:
                self._executor.submit(handle, dt)

    def periodic(self, period: [int, timedelta]):
        if isinstance(period, timedelta):
            period = int(period.total_seconds())
        assert isinstance(period, int) and period > 0

        def decorator(f):
            # scatter handlers with same period, `remain` consistent across different processes
            remain = crc32(f.__name__.encode()) % period
            self._periodic_handlers.append([period, remain, variadic_args(f)])
            return f

        return decorator

    def crontab(self, *, year=None, month=None, day=None, hour=None, minute=None, second=None, weekday=None):
        cron = _Crontab(year, month, day, hour, minute, second, weekday)
        for value in cron.__dict__.values():
            assert value is None or isinstance(value, int) or -1 not in value  # supports `in`

        def decorator(f):
            self._crontab_handlers.append([cron, variadic_args(f)])
            return f

        return decorator
