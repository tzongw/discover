import contextlib
import logging
import socket
from functools import lru_cache
import sys
from .executor import Executor
from redis import Redis
from redis.client import Pipeline
from google.protobuf.message import Message
from google.protobuf.json_format import ParseDict, MessageToDict
from werkzeug.routing import BaseConverter
from random import choice
from concurrent.futures import Future
from collections import defaultdict
import gevent
from boltons.cacheutils import LRU


class LogSuppress(contextlib.suppress):
    def __exit__(self, exctype, excinst, exctb):
        if excinst:
            logging.exception(f'')
        return super().__exit__(exctype, excinst, exctb)


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


wildcard = '' if sys.platform == 'darwin' else '*'


class Dispatcher:
    def __init__(self, sep=None, executor=None):
        self._handlers = defaultdict(list)
        self._sep = sep
        self._executor = executor or Executor(name='dispatch')

    def dispatch(self, key, *args, **kwargs):
        if self._sep and isinstance(key, str):
            key = key.split(self._sep, maxsplit=1)[0]
        handlers = self._handlers.get(key) or []
        for handle in handlers:
            self._executor.submit(handle, *args, **kwargs)

    def signal(self, event):
        cls = event.__class__
        self.dispatch(cls, event)

    def handler(self, key):
        def decorator(f):
            self._handlers[key].append(f)
            return f

        return decorator

    @property
    def handlers(self):
        return self._handlers


class Parser:
    def __init__(self, redis: Redis):
        self._redis = redis
        if redis.response_callbacks['HGETALL'] is Redis.RESPONSE_CALLBACKS['HGETALL']:
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

    def hget(self, name: str, message: Message, return_none=False):
        def converter(mapping):
            return ParseDict(mapping, message, ignore_unknown_fields=True) \
                if mapping or not return_none else None

        return self._redis.execute_command('HGETALL', name, converter=converter)


class ListConverter(BaseConverter):
    def __init__(self, map, type='str', sep=','):
        super().__init__(map)
        self.type = eval(type)
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


class SingleFlight:
    def __init__(self, f):
        self._f = f
        self._futures = {}

    def get(self, key, *args, **kwargs):
        if key in self._futures:
            return self._futures[key].result()
        fut = Future()
        self._futures[key] = fut
        try:
            r = self._f(key, *args, **kwargs)
            fut.set_result(r)
            return r
        except Exception as e:
            fut.set_exception(e)
            raise
        finally:
            self._futures.pop(key)


class Cache:
    def __init__(self, f, maxsize=8192):
        self.single_flight = SingleFlight(f)
        self.lru = LRU(max_size=maxsize)

    def get(self, key, *args, **kwargs):
        if key in self.lru:
            return self.lru[key]
        r = self.single_flight.get(key, *args, **kwargs)
        self.lru[key] = r
        return r


def stream_name(message: Message) -> str:
    return f'stream:{message.__class__.__name__}'


def run_in_thread(fn, *args, **kwargs):
    pool = gevent.get_hub().threadpool
    result = pool.spawn(fn, *args, **kwargs).get()
    return result
