import contextlib
import logging
import socket
import string
from binascii import crc32
from collections import defaultdict
from datetime import timedelta
from functools import lru_cache, wraps
from inspect import signature, Parameter
from typing import Callable, Type, Union

from pydantic import BaseModel
from redis import Redis, RedisCluster
from redis.exceptions import LockError
from redis.lock import Lock
from yaml import safe_dump


class LogSuppress(contextlib.suppress):
    def __init__(self, *exceptions, log_level=logging.ERROR):
        if not exceptions:
            exceptions = [Exception]
        super().__init__(*exceptions)
        self.log = {logging.INFO: logging.info, logging.WARNING: logging.warning, logging.ERROR: logging.error}.get(
            log_level)

    def __exit__(self, exctype, excinst, exctb):
        suppress = super().__exit__(exctype, excinst, exctb)
        if suppress:
            self.log(f'suppressed: {excinst}', exc_info=True)
        return suppress


class Addr:
    def __init__(self, value: str):
        host, port = value.rsplit(':', maxsplit=1)
        self.host = host or '127.0.0.1'
        self.port = int(port)

    def __str__(self):
        return f'{self.host}:{self.port}'

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.host == other.host and self.port == other.port

    def __hash__(self):
        return hash(str(self))


@lru_cache(maxsize=None)
def ip_address(ipv6=False):
    with socket.socket(socket.AF_INET6 if ipv6 else socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(('8.8.8.8', 9))
        return sock.getsockname()[0]


def var_args(f: Callable):
    params = signature(f).parameters
    var_positional = any(p.kind == Parameter.VAR_POSITIONAL for p in params.values())
    var_keyword = any(p.kind == Parameter.VAR_KEYWORD for p in params.values())

    @wraps(f)
    def wrapper(*args, **kwargs):
        if not var_positional:
            args = args[:len(params)]
        if not var_keyword:
            kwargs = {k: v for k, v in kwargs.items() if k in params}
        return f(*args, **kwargs)

    return wrapper


_kw_mark = object()


def make_key(key, *args, **kwargs):
    if not args and not kwargs:
        return key
    return key, *args, _kw_mark, *kwargs.items()


def stream_name(message_or_cls: Union[BaseModel, Type[BaseModel]]) -> str:
    cls = message_or_cls.__class__ if isinstance(message_or_cls, BaseModel) else message_or_cls
    return f'stream:{cls.__name__}'


def func_desc(func):
    try:
        return f'{func.__module__}.{func.__name__}'
    except AttributeError:
        return str(func)


class DefaultDict(defaultdict):
    def __missing__(self, key):
        if not self.default_factory:
            return super().__missing__(key)
        value = self.default_factory(key)
        self[key] = value
        return value


class CaseDict(dict):
    def __getitem__(self, item):
        if isinstance(item, str):
            item = ''.join(s[0].upper() + s[1:] for s in item.split('-'))
        return super().__getitem__(item)


def base62(n):
    assert n >= 0
    charset = string.ascii_letters + string.digits
    base = len(charset)
    chars = []
    while True:
        n, r = divmod(n, base)
        chars.append(charset[r])
        if n == 0:
            break
    return ''.join(chars[::-1])


def redis_name(redis: Union[Redis, RedisCluster]):
    if isinstance(redis, RedisCluster):
        return redis.nodes_manager.default_node.name
    else:
        kwargs = redis.get_connection_kwargs()
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 6379)
        db = kwargs.get('db', 0)
        return f'{host}:{port}/{db}'


def stable_hash(o):
    s = safe_dump(o)
    return crc32(s.encode())


class Exclusion:
    def __init__(self, redis: Union[Redis, RedisCluster]):
        self.redis = redis

    def __call__(self, timeout: timedelta, *names):
        def decorator(f):
            path = func_desc(f)
            assert '<' not in path, 'CAN NOT be lambda or local function'
            assert not path.startswith('__main__'), '__main__ is different in another process'
            params = signature(f).parameters
            indexes = {name: index for name in names for index, param in enumerate(params.values()) if
                       param.name == name}
            assert len(indexes) == len(names)

            @wraps(f)
            def wrapper(*args, **kwargs):
                keys = {name: args[index] if index < len(args) else kwargs[name] for name, index in indexes.items()}
                key = f'exclusion:{path}:{stable_hash(keys)}'
                with contextlib.suppress(LockError), Lock(self.redis, key, timeout.total_seconds(), blocking=False):
                    f(*args, **kwargs)

            return wrapper

        return decorator
