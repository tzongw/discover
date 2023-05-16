import contextlib
import logging
import socket
import string
import uuid
from datetime import timedelta
from typing import Callable
from inspect import signature, Parameter
from functools import lru_cache, wraps
from typing import Type, Union
from redis import Redis, RedisCluster
from redis.lock import Lock, LockError
from werkzeug.routing import BaseConverter
from random import choice, shuffle
from collections import defaultdict
import gevent
from pydantic import BaseModel
from gevent.local import local


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
        return f(*args, **kwargs)

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
    with redis.pipeline(transaction=False) as pipe:
        for member in members:
            pipe.zrem(key, member)
        misses = []
        for member, removed in zip(members, pipe.execute()):
            if not removed:
                misses.append(member)
    for member in misses:
        members.pop(member)
    return members


class Semaphore:
    def __init__(self, redis: Redis, name, value: int, timeout=timedelta(minutes=1)):
        self.redis = redis
        self.names = [f'{{{name}}}_{i}' for i in range(value)]
        self.timeout = timeout
        self.local = local()
        self.lua_release = redis.register_script(Lock.LUA_RELEASE_SCRIPT)
        self.lua_reacquire = redis.register_script(Lock.LUA_REACQUIRE_SCRIPT)

    def __enter__(self):
        token = str(uuid.uuid4())
        names = [name for name, value in zip(self.names, self.redis.mget(self.names)) if value is None]
        shuffle(names)
        for name in names:
            if self.redis.set(name, token, nx=True, px=self.timeout):
                self.local.name = name
                self.local.token = token
                return self
        raise LockError('Unable to acquire lock')

    def __exit__(self, exctype, excinst, exctb):
        name, token = self.local.name, self.local.token
        self.local.name = self.local.token = None
        self.lua_release(keys=[name], args=[token])

    def reacquire(self):
        timeout = int(self.timeout.total_seconds() * 1000)
        name, token = self.local.name, self.local.token
        if name and token and self.lua_reacquire(keys=[name], args=[token, timeout]):
            return
        raise LockError('Lock not owned')

    def exists(self):
        return self.redis.exists(*self.names)


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


class Stocks:
    def __init__(self, redis: Union[Redis, RedisCluster]):
        self.redis = redis

    def reset(self, key, total=0, expire=None):
        assert total >= 0
        self.redis.bitfield(key).set(fmt='u32', offset=0, value=total).execute()
        if expire is not None:
            self.redis.expire(key, expire)

    def incrby(self, key, total):
        assert total >= 0
        self.redis.bitfield(key).incrby(fmt='u32', offset=0, increment=total).execute()

    def try_lock(self, key, hint=None) -> bool:
        bitfield = self.redis.bitfield(key, default_overflow='FAIL')
        return bitfield.incrby(fmt='u32', offset=0, increment=-1).execute()[0] is not None
