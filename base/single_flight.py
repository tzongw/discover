# -*- coding: utf-8 -*-
from concurrent.futures import Future
from .utils import make_key


class SingleFlight:
    def __init__(self, *, get=None, mget=None):
        assert get or mget
        if mget is None:
            # simulate mget to reuse code
            def mget(keys, *args, **kwargs):
                assert len(keys) == 1
                return [get(key, *args, **kwargs) for key in keys]
        self._mget = mget
        self._futures = {}  # type: dict[any, Future]

    def get(self, key, *args, **kwargs):
        value, = self.mget([key], *args, **kwargs)
        return value

    def mget(self, keys, *args, **kwargs):
        futures = []
        missed_keys = []
        for key in keys:
            made_key = make_key(key, *args, **kwargs)
            if made_key in self._futures:
                futures.append(self._futures[made_key])
            else:
                fut = Future()
                self._futures[made_key] = fut
                futures.append(fut)
                missed_keys.append(key)
        if missed_keys:
            try:
                values = self._mget(missed_keys, *args, **kwargs)
                assert len(missed_keys) == len(values)
                for key, value in zip(missed_keys, values):
                    made_key = make_key(key, *args, **kwargs)
                    fut = self._futures.pop(made_key)
                    fut.set_result(value)
            except Exception as e:
                for key in missed_keys:
                    made_key = make_key(key, *args, **kwargs)
                    fut = self._futures.pop(made_key)
                    fut.set_exception(e)
                raise
        return [fut.result() for fut in futures]
