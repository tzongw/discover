import os
import fcntl
import logging
import socket
import string
import bisect
import hashlib
import contextlib
from binascii import crc32
from random import choice
from collections import defaultdict, namedtuple
from functools import lru_cache, wraps, total_ordering
from inspect import signature, Parameter
from typing import Callable, Type, Union
from pydantic import BaseModel
from redis import Redis, RedisCluster
import gevent


class LogSuppress(contextlib.suppress):
    def __init__(self, *exceptions):
        if not exceptions:
            exceptions = [Exception]
        super().__init__(*exceptions)

    def __exit__(self, exc_type, exc_val, exc_tb):
        suppress = super().__exit__(exc_type, exc_val, exc_tb)
        if suppress:
            logging.exception('suppressed')
        return suppress


class Addr:
    def __init__(self, value: str):
        host, port = value.rsplit(':', maxsplit=1)
        self.host = host or '127.0.0.1'
        self.port = int(port)

    def __str__(self):
        return f'{self.host}:{self.port}'

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.host == other.host and self.port == other.port

    def __hash__(self):
        return hash(str(self))


@total_ordering
class Version:
    def __init__(self, value='0.0.0'):
        version = [int(s) for s in value.split('.')]
        if len(version) != 3 or not all(0 <= v <= 65535 for v in version):
            raise ValueError(value)
        self.value = value
        self.int = sum(v << 16 * i for i, v in enumerate(reversed(version)))

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.int == other.int

    def __lt__(self, other):
        return self.int < other.int

    def __hash__(self):
        return self.int


_Node = namedtuple('Node', ['hash', 'value'])


class CHash:
    """consistent hash"""

    def __init__(self, values, replicas=10):
        ring = []
        for value in values:
            for replica in range(replicas):
                ring.append(_Node(crc32(f'{value}_{replica}'.encode()), value))
        ring.sort()
        self._ring = ring

    def __call__(self, key: str):
        node = _Node(crc32(key.encode()), self._ring[0].value)
        i = bisect.bisect(self._ring, node)
        if i >= len(self._ring):
            i = 0
        return self._ring[i].value


@lru_cache(maxsize=None)
def ip_address(ipv6=False):
    with socket.socket(socket.AF_INET6 if ipv6 else socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(('8.8.8.8', 9))
        return sock.getsockname()[0]


def variadic_args(f: Callable):
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
    return key, args, _kw_mark, *kwargs.items()


def make_mget(get=None, mget=None):
    assert get or mget
    if mget is None:  # simulate mget to reuse code
        def mget(keys, *args, **kwargs):
            assert len(keys) == 1
            return [get(key, *args, **kwargs) for key in keys]
    return mget


def stream_name(message_or_cls: Union[BaseModel, Type[BaseModel]]) -> str:
    cls = message_or_cls.__class__ if isinstance(message_or_cls, BaseModel) else message_or_cls
    return f'stream:{cls.__name__}'


def func_desc(func):
    try:
        return f'{func.__module__}.{func.__name__}'
    except AttributeError:
        return str(func)


def native_worker(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        pool = gevent.get_hub().threadpool
        return pool.spawn(f, *args, **kwargs).get()

    return wrapper


class DefaultDict(defaultdict):
    def __missing__(self, key):
        if not self.default_factory:
            return super().__missing__(key)
        value = self.default_factory(key)
        self[key] = value
        return value


class PascalCaseDict(dict):
    def __getitem__(self, item):
        if isinstance(item, str):
            item = item.replace('_', '-')
            item = ''.join(s[0].upper() + s[1:] for s in item.split('-'))
        return super().__getitem__(item)


class base62:
    charset = string.digits + string.ascii_uppercase + string.ascii_lowercase
    base = 62
    mapping = {c: index for index, c in enumerate(charset)}

    @classmethod
    def encode(cls, n):
        assert n >= 0
        chars = []
        while True:
            n, r = divmod(n, cls.base)
            chars.append(cls.charset[r])
            if n == 0:
                break
        return ''.join(chars[::-1])

    @classmethod
    def decode(cls, s):
        n = 0
        for c in s:
            n = n * cls.base + cls.mapping[c]
        return n


class base36(base62):
    charset = string.digits + string.ascii_uppercase
    base = 36
    mapping = {c: index for index, c in enumerate(charset)}


def redis_name(redis: Union[Redis, RedisCluster]):
    if isinstance(redis, RedisCluster):
        return redis.nodes_manager.default_node.name
    else:
        kwargs = redis.get_connection_kwargs()
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 6379)
        db = kwargs.get('db', 0)
        return f'{host}:{port}/{db}'


def create_redis(addr: str):
    if ',' in addr:
        addr = choice(addr.split(','))
        return RedisCluster.from_url(f'redis://{addr}', decode_responses=True)
    else:
        proto = 'unix' if os.path.exists(addr) else 'redis'
        return Redis.from_url(f'{proto}://{addr}', decode_responses=True)


def string_hash(s: str):
    digest = hashlib.md5(s.encode()).digest()
    return int.from_bytes(digest[:8], signed=True)


def salt_hash(value, *, salt):
    return hashlib.sha1(f'{salt}{value}'.encode()).hexdigest()


def flock(path):
    f = open(path, 'a+')
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.seek(0)
        pid = f.readline()
        f.close()
        raise BlockingIOError(f'locking by {pid}')
    except Exception:
        f.close()
        raise
    f.seek(0)
    f.truncate(0)
    f.write(str(os.getpid()))
    f.flush()
    return f


def diff_dict(after: dict, before: dict):
    diff = {}
    for key in after.keys() | before.keys():  # type: str
        if key.startswith('_'):  # ignore
            continue
        va = after.get(key)
        vb = before.get(key)
        if va == vb:
            continue
        if isinstance(va, dict) and isinstance(vb, dict):
            diff[key] = diff_dict(va, vb)
        else:
            diff[key] = va
    return diff


def apply_diff(origin: dict, diff: dict):
    for key, value in diff.items():
        if value is None:
            origin.pop(key)
            continue
        vo = origin.get(key)
        if isinstance(vo, dict) and isinstance(value, dict):
            apply_diff(vo, value)
        else:
            origin[key] = value
