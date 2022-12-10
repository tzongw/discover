import contextlib
import logging
import socket
from typing import Callable
from inspect import signature, Parameter
from functools import lru_cache, wraps
from typing import TypeVar, Optional, Type, Union
from redis import Redis
from redis.client import Pipeline
from google.protobuf.message import Message
from google.protobuf.json_format import ParseDict, MessageToDict
from werkzeug.routing import BaseConverter
from random import choice
from concurrent.futures import Future
from collections import defaultdict
import gevent


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


M = TypeVar('M', bound=Message)


class Parser:
    def __init__(self, redis: Redis):
        self._redis = redis
        redis.response_callbacks['HGETALL'] = self.hgetall_callback

    @staticmethod
    def hgetall_callback(response, converter=None):
        response = Redis.RESPONSE_CALLBACKS['HGETALL'](response)
        return converter(response) if converter else response

    def hset(self, name: str, message: Message, expire=None):  # embedded message not work
        mapping = MessageToDict(message)
        if expire is None:
            self._redis.hset(name, mapping=mapping)
        else:
            if isinstance(self._redis, Pipeline):
                self._redis.hset(name, mapping=mapping)
                self._redis.expire(name, expire)
            else:
                with self._redis.pipeline() as pipe:
                    pipe.hset(name, mapping=mapping)
                    pipe.expire(name, expire)
                    pipe.execute()

    def hget(self, name: str, message: M, return_none=False) -> Optional[M]:
        def converter(mapping):
            return ParseDict(mapping, message, ignore_unknown_fields=True) \
                if mapping or not return_none else None

        return self._redis.execute_command('HGETALL', name, converter=converter)


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


def stream_name(message_or_cls: Union[Message, Type[Message]]) -> str:
    cls = message_or_cls.__class__ if isinstance(message_or_cls, Message) else message_or_cls
    return f'stream:{cls.__name__}'


def timer_name(message_or_cls: Union[Message, Type[Message]]) -> str:
    cls = message_or_cls.__class__ if isinstance(message_or_cls, Message) else message_or_cls
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
