from __future__ import annotations

import dataclasses
import json
import uuid
from datetime import datetime, date, timedelta
from random import shuffle
from typing import Any, Callable, Optional, Self, Union

from flask.app import DefaultJSONProvider, Flask
from gevent.local import local
from mongoengine import EmbeddedDocument, DoesNotExist, FloatField
from pydantic import BaseModel
from redis import Redis, RedisCluster
from redis.lock import Lock
from redis.exceptions import LockError
from werkzeug.routing import BaseConverter

from .invalidator import Invalidator


class ListConverter(BaseConverter):
    def __init__(self, map, type=str, sep=','):
        super().__init__(map)
        self.type = type
        self.sep = sep

    def to_python(self, value):
        return [self.type(v) for v in value.split(self.sep)]

    def to_url(self, value):
        return self.sep.join([str(v) for v in value])


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, GetterMixin):
            return o.to_dict()
        elif isinstance(o, BaseModel):
            return o.dict()
        elif isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(o, date):
            return o.strftime('%Y-%m-%d')
        elif isinstance(o, timedelta):
            return o.total_seconds()
        elif isinstance(o, EmbeddedDocument):
            return o.to_mongo().to_dict()
        return super().default(o)


class JSONProvider(DefaultJSONProvider):
    def dumps(self, obj, cls=JSONEncoder, default=None, **kwargs) -> str:
        return super().dumps(obj, cls=cls, default=default, **kwargs)


def make_response(app, rv):
    if rv is None:
        rv = {}
    elif isinstance(rv, GetterMixin):
        rv = rv.to_dict()
    elif isinstance(rv, BaseModel):
        rv = rv.dict()
    elif dataclasses.is_dataclass(rv):
        rv = dataclasses.asdict(rv)
    return Flask.make_response(app, rv)


class GetterMixin:
    id: Any
    objects: Callable
    _fields: dict
    _data: dict
    __include__ = None

    @classmethod
    def mget(cls, keys, *, only=()) -> list[Optional[Self]]:
        if not keys:
            return []
        query = {f'{cls.id.name}__in': keys}
        mapping = {o.id: o for o in cls.objects(**query).only(*only).limit(len(keys))}
        return [mapping.get(cls.id.to_python(k)) for k in keys]

    @classmethod
    def get(cls, key, *, ensure=False, default=False, only=()) -> Optional[Self]:
        value = cls.mget([key], only=only)[0]
        if value is None:
            if ensure:
                raise DoesNotExist(f'`{cls.__name__}` `{key}` does not exist')
            if default:
                value = cls(**{cls.id.name: cls.id.to_python(key)})
        return value

    def to_dict(self, include=(), exclude=None):
        if exclude is not None:
            assert not include, '`include`, `exclude` are mutually exclusive'
            include = [field for field in self._fields if field not in exclude and not field.startswith('_')]
        elif not include:
            include = self.__include__
        return {k: v for k, v in self._data.items() if k in include}


class CacheMixin(GetterMixin):
    @classmethod
    def make_key(cls, key, *_, **__):
        return cls.id.to_python(key)  # ignore only

    @classmethod
    def mget(cls, keys, *_, **__) -> list[Optional[Self]]:
        return super().mget(keys)  # ignore only

    def invalidate(self, invalidator: Invalidator):
        invalidator.publish(self.__class__.__name__, self.id)

    @staticmethod
    def fields_expire(*fields):
        def get_expire(values):
            now = datetime.now()
            expires = [doc[field] for doc in values for field in fields if doc[field] >= now]
            return min(expires) if expires else None

        return get_expire


class FlashCacheMixin(CacheMixin):
    @classmethod
    def mget(cls, keys, *_, **__) -> list[Optional[Self]]:
        return [(value, timedelta(seconds=1)) for value in super().mget(keys)]


class Semaphore:
    def __init__(self, redis: Union[Redis, RedisCluster], name, value: int, timeout=timedelta(minutes=1)):
        self.redis = redis
        self.names = [f'{name}_{i}' for i in range(value)]
        self.timeout = timeout
        self.local = local()
        self.lua_release = redis.register_script(Lock.LUA_RELEASE_SCRIPT)
        self.lua_reacquire = redis.register_script(Lock.LUA_REACQUIRE_SCRIPT)

    def __enter__(self):
        assert not self.local.__dict__, 'recursive lock'
        token = str(uuid.uuid4())
        shuffle(self.names)
        for name in self.names:
            if self.redis.set(name, token, nx=True, px=self.timeout):
                self.local.name = name
                self.local.token = token
                return self
        raise LockError('Unable to acquire lock')

    def __exit__(self, exctype, excinst, exctb):
        keys = [self.local.name]
        args = [self.local.token]
        del self.local.name
        del self.local.token
        self.lua_release(keys=keys, args=args)

    def reacquire(self):
        timeout = int(self.timeout.total_seconds() * 1000)
        name, token = self.local.name, self.local.token
        if self.lua_reacquire(keys=[name], args=[token, timeout]):
            return
        raise LockError('Lock not owned')

    def acquired(self):
        return sum(self.redis.exists(*self.names))


class Stocks:
    def __init__(self, redis: Union[Redis, RedisCluster]):
        self.redis = redis

    def reset(self, key, total=0, expire=None):
        assert total >= 0
        with self.redis.pipeline(transaction=False) as pipe:
            pipe.bitfield(key).set(fmt='u32', offset=0, value=total).execute()
            if expire is not None:
                pipe.expire(key, expire)
            pipe.execute()

    def get(self, key):
        return self.mget([key])

    def mget(self, keys):
        with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.bitfield(key).get(fmt='u32', offset=0).execute()
            return [values[0] for values in pipe.execute()]

    def incrby(self, key, total):
        assert total >= 0
        return self.redis.bitfield(key).incrby(fmt='u32', offset=0, increment=total).execute()[0]

    def try_lock(self, key, hint=None) -> bool:
        bitfield = self.redis.bitfield(key, default_overflow='FAIL')
        return bitfield.incrby(fmt='u32', offset=0, increment=-1).execute()[0] is not None


class TimeDeltaField(FloatField):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        if isinstance(min_value, timedelta):
            min_value = min_value.total_seconds()
        if isinstance(max_value, timedelta):
            max_value = max_value.total_seconds()
        super().__init__(min_value, max_value, **kwargs)

    def prepare_query_value(self, op, value):
        value = self.to_mongo(value)
        return super().prepare_query_value(op, value)

    def to_mongo(self, value):
        return value.total_seconds() if isinstance(value, timedelta) else super().to_python(value)  # yes, to_python

    def to_python(self, value):
        value = super().to_python(value)
        return timedelta(seconds=value) if isinstance(value, float) else value
