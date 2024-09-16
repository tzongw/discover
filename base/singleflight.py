# -*- coding: utf-8 -*-
import functools
from concurrent.futures import Future
from typing import TypeVar, Generic, Sequence
from . import utils

T = TypeVar('T')


class Singleflight(Generic[T]):
    def __init__(self, *, get=None, mget=None, make_key=utils.make_key):
        self.make_key = make_key
        self._mget = utils.make_mget(get, mget)
        self._futures = {}  # type: dict[any, Future]

    def get(self, key, *args, **kwargs) -> T:
        value, = self.mget([key], *args, **kwargs)
        return value

    def mget(self, keys, *args, **kwargs) -> Sequence[T]:
        futures = []
        missing_keys = []
        made_keys = []
        for key in keys:
            made_key = self.make_key(key, *args, **kwargs)
            if made_key in self._futures:
                futures.append(self._futures[made_key])
            else:
                fut = Future()
                self._futures[made_key] = fut
                futures.append(fut)
                missing_keys.append(key)
                made_keys.append(made_key)
        if missing_keys:
            try:
                values = self._mget(missing_keys, *args, **kwargs)
                assert len(missing_keys) == len(values)
            except Exception as e:
                for made_key in made_keys:
                    fut = self._futures.pop(made_key)
                    fut.set_exception(e)
                raise
            else:
                for made_key, value in zip(made_keys, values):
                    fut = self._futures.pop(made_key)
                    fut.set_result(value)
        return [fut.result() for fut in futures]


def singleflight(f):
    def get(key):
        args, *items = key
        return f(*args, **dict(items))

    sf = Singleflight(get=get)

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        key = (args, *kwargs.items())
        return sf.get(key)

    return wrapper
