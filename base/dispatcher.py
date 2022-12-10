# -*- coding: utf-8 -*-
from collections import defaultdict

from base import Executor
from base.utils import var_args


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
