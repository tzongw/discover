import itertools
from random import shuffle
from datetime import datetime
from typing import Callable
from .singleflight import singleflight


class BalanceProxy:
    def __init__(self, targets):
        targets = list(targets)
        shuffle(targets)
        self._iter = itertools.cycle(targets)

    def pick(self):
        return next(self._iter)

    def __getattr__(self, name):
        target = self.pick()
        return getattr(target, name)


class MigratingProxy:
    def __int__(self, new, old, start_time: datetime):
        self._new = new
        self._old = old
        self._start_time = start_time

    def __getattr__(self, name):
        target = self._new if datetime.now() >= self.start_time else self._old
        return getattr(target, name)


class LazyProxy:
    def __init__(self, create: Callable):
        self._target = None
        self._create = singleflight(create)

    def __getattr__(self, name):
        if self._create:
            self._target = self._create()
            self._create = None
        return getattr(self._target, name)
