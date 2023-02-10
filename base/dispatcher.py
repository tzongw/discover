# -*- coding: utf-8 -*-
from collections import defaultdict
from datetime import timedelta

from .executor import Executor
from .utils import var_args


class Dispatcher:
    def __init__(self, sep=None, executor=None):
        self.handlers = defaultdict(list)
        self.sep = sep
        self._executor = executor or Executor(name='dispatch')

    def dispatch(self, key, *args, **kwargs):
        if self.sep and isinstance(key, str):
            key = key.split(self.sep, maxsplit=1)[0]
        handlers = self.handlers.get(key) or []
        for handle in handlers:
            self._executor.submit(handle, *args, **kwargs)

    def signal(self, event):
        cls = event.__class__
        self.dispatch(cls, event)

    def handler(self, key):
        def decorator(f):
            self.handlers[key].append(var_args(f))
            return f

        return decorator


class TimeDispatcher(Dispatcher):
    def __init__(self, executor=None):
        super().__init__(executor=executor or Executor(name='mod_dispatch'))

    def dispatch(self, ts, *args, **kwargs):
        for factor, handles in self.handlers.items():
            if ts % factor:
                continue
            for handle in handles:
                self._executor.submit(handle, *args, **kwargs)

    def handler(self, factor: [int, timedelta]):
        if isinstance(factor, timedelta):
            factor = int(factor.total_seconds())
        assert isinstance(factor, int) and factor > 0
        return super().handler(factor)
