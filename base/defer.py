# -*- coding: utf-8 -*-
import functools
import contextlib
from enum import Flag, auto
from typing import Callable
from gevent.local import local

_local = local()
_exception = object()


class Result(Flag):
    TRUE = auto()
    FALSE = auto()
    EXCEPTION = auto()


def deferrable(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not hasattr(_local, 'stacks'):
            _local.stacks = []
        stacks = _local.stacks
        with contextlib.ExitStack() as stack:
            stacks.append(stack)
            stack.callback(stacks.pop)
            _local.result = _exception
            result = f(*args, **kwargs)
            _local.result = result
            return result

    return wrapper


def defer(f: Callable, *args, **kwargs):
    assert callable(f)
    stack = _local.stacks[-1]
    stack.callback(f, *args, **kwargs)


def defer_if(flags: Result, f: Callable, *args, **kwargs):
    def inner():
        result = _local.result
        if result is _exception:
            flag = Result.EXCEPTION
        elif result:
            flag = Result.TRUE
        else:
            flag = Result.FALSE
        if flag & flags:
            f(*args, **kwargs)

    defer(inner)
