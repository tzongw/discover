import contextlib
import logging
import socket
from typing import Callable
from inspect import signature, Parameter
from functools import lru_cache, wraps
from typing import Type, Union
from redis import Redis
from werkzeug.routing import BaseConverter
from random import choice
from collections import defaultdict
import gevent
from pydantic import BaseModel


class LogSuppress(contextlib.suppress):
    def __exit__(self, exctype, excinst, exctb):
        suppress = super().__exit__(exctype, excinst, exctb)
        if suppress:
            logging.exception(f'')
        return suppress


class Addr:
    def __init__(self, value: str):
        host, port = value.rsplit(':', maxsplit=1)
        self.host = host
        self.port = int(port)

    def __str__(self):
        return f'{self.host}:{self.port}'

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.host == other.host and self.port == other.port

    def __hash__(self):
        return hash(str(self))


@lru_cache()
def ip_address(ipv6=False):
    with socket.socket(socket.AF_INET6 if ipv6 else socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(('8.8.8.8', 9))
        return sock.getsockname()[0]


def var_args(f: Callable):
    params = signature(f).parameters

    @wraps(f)
    def wrapper(*args, **kwargs):
        if not any(p.kind == Parameter.VAR_POSITIONAL for p in params.values()):
            args = args[:len(params)]
        if not any(p.kind == Parameter.VAR_KEYWORD for p in params.values()):
            kwargs = {k: v for k, v in kwargs.items() if k in params}
        f(*args, **kwargs)

    return wrapper


class ListConverter(BaseConverter):
    def __init__(self, map, type=str, sep=','):
        super().__init__(map)
        self.type = type
        self.sep = sep

    def to_python(self, value):
        return [self.type(v) for v in value.split(self.sep)]

    def to_url(self, value):
        return self.sep.join([str(v) for v in value])


class Proxy:
    def __init__(self, *targets):
        self._targets = targets

    def __getattr__(self, name):
        return getattr(choice(self._targets), name)


_kw_mark = object()


def make_key(key, *args, **kwargs):
    if not args and not kwargs:
        return key
    return key, *args, _kw_mark, *kwargs.items()


def stream_name(message_or_cls: Union[BaseModel, Type[BaseModel]]) -> str:
    cls = message_or_cls.__class__ if isinstance(message_or_cls, BaseModel) else message_or_cls
    return f'stream:{cls.__name__}'


def timer_name(message_or_cls: Union[BaseModel, Type[BaseModel]]) -> str:
    cls = message_or_cls.__class__ if isinstance(message_or_cls, BaseModel) else message_or_cls
    return f'timer:{cls.__name__}'


def func_desc(func):
    try:
        return f'{func.__module__}.{func.__name__}'
    except AttributeError:
        return str(func)


def run_in_thread(fn, *args, **kwargs):
    pool = gevent.get_hub().threadpool
    result = pool.spawn(fn, *args, **kwargs).get()
    return result


class DefaultDict(defaultdict):
    def __missing__(self, key):
        if not self.default_factory:
            return super().__missing__(key)
        value = self.default_factory(key)
        self[key] = value
        return value


def zpop_by_score(redis: Redis, key, start, stop, limit=None):
    kwargs = {'offset': 0, 'num': limit} if limit else {}
    members = {member: score for member, score in
               redis.zrange(key, start, stop, byscore=True, withscores=True, **kwargs)}
    with redis.pipeline() as pipe:
        for member in members:
            pipe.zrem(key, member)
        misses = []
        for member, removed in zip(members, pipe.execute()):
            if not removed:
                misses.append(member)
    for member in misses:
        members.pop(member)
    return members
