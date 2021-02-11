import contextlib
import logging
import socket
from functools import lru_cache
import sys
from redis import Redis
from redis.client import Pipeline
from google.protobuf.message import Message
from google.protobuf.json_format import ParseDict, MessageToDict
from werkzeug.routing import BaseConverter
from random import choice
from copy import copy


class LogSuppress(contextlib.suppress):
    def __exit__(self, exctype, excinst, exctb):
        if excinst:
            logging.exception(f'')
        return super().__exit__(exctype, excinst, exctb)


@lru_cache()
def ip_address():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(('8.8.8.8', 9))
        return sock.getsockname()[0]


wildcard = '' if sys.platform == 'darwin' else '*'


class Dispatcher:
    def __init__(self, sep=None):
        self._handlers = {}
        self._sep = sep

    def dispatch(self, key: str, *args, **kwargs):
        if self._sep is not None:
            key = key.split(self._sep, maxsplit=1)[0]
        handler = self._handlers.get(key)
        if handler:
            with LogSuppress(Exception):
                return handler(*args, **kwargs)
        else:
            logging.warning(f'not handle {args} {kwargs}')

    def handler(self, key: str):
        def decorator(f):
            assert key not in self._handlers
            self._handlers[key] = f
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
                with self._redis.pipeline(transaction=True) as pipe:
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
