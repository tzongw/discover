import contextlib
import dataclasses
import json
import uuid
from binascii import crc32
from datetime import datetime, date, timedelta
from functools import wraps
from inspect import signature
from random import choice
from collections import defaultdict
from typing import Any, Callable, Optional, Self, Union, Iterable
from types import MappingProxyType
from flask.app import DefaultJSONProvider, Flask
from gevent.hub import Hub
from gevent.local import local
from gevent import getcurrent
from mongoengine import EmbeddedDocument, FloatField
from pymongo.results import BulkWriteResult
from sqlalchemy import and_, DateTime, Date
from pydantic import BaseModel
from redis import Redis, RedisCluster
from redis.lock import Lock
from redis.exceptions import LockError
from werkzeug.routing import BaseConverter
from .utils import base62, diff_dict
from .invalidator import Invalidator
from .snowflake import extract_datetime


class DoesNotExist(Exception):
    pass


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
        if isinstance(o, (DocumentMixin, TableMixin)):
            return o.to_dict()
        elif isinstance(o, BaseModel):
            return o.dict()
        elif dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        elif isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(o, date):
            return o.strftime('%Y-%m-%d')
        elif isinstance(o, timedelta):
            return o.total_seconds()
        elif isinstance(o, EmbeddedDocument):
            return o.to_mongo().to_dict()
        elif isinstance(o, (set, frozenset)):
            return list(o)
        elif isinstance(o, MappingProxyType):
            return o.copy()
        return super().default(o)


class JSONProvider(DefaultJSONProvider):
    def dumps(self, obj, cls=JSONEncoder, default=None, **kwargs) -> str:
        return super().dumps(obj, cls=cls, default=default, **kwargs)


def make_response(app, rv):
    if rv is None:
        rv = {}
    elif isinstance(rv, (DocumentMixin, TableMixin)):
        rv = rv.to_dict()
    elif isinstance(rv, BaseModel):
        rv = rv.dict()
    elif dataclasses.is_dataclass(rv):
        rv = dataclasses.asdict(rv)
    elif isinstance(rv, list):
        raise ValueError('return dict instead')
    return Flask.make_response(app, rv)


class DocumentMixin:
    id: Any
    objects: Callable
    _get_collection: Callable
    _fields: dict
    _data: dict
    __include__ = ()
    __exclude__ = ()

    @classmethod
    def mget(cls, keys) -> list[Optional[Self]]:
        if not keys:
            return []
        query = {f'{cls.id.name}__in': keys}
        mapping = {o.id: o for o in cls.objects(**query)}
        return [mapping.get(cls.id.to_python(k)) for k in keys]

    @classmethod
    def get(cls, key, *, ensure=False, default=False) -> Optional[Self]:
        value = cls.mget([key])[0]
        if value is None:
            if default:
                value = cls(**{cls.id.name: cls.id.to_python(key)})
            elif ensure:
                raise DoesNotExist(f'`{cls.__name__}` `{key}` does not exist')
        return value

    def to_dict(self, include=(), exclude=None):
        if exclude is not None:
            assert not include, '`include`, `exclude` are mutually exclusive'
            include = self.__include__ + tuple(
                field for field in self._fields if field not in exclude and field not in self.__include__)
        elif not include:
            include = self.__include__
        d = {k: v for k, v in self._data.items() if k in include and k not in self.__exclude__}
        if 'create_time' in include and 'create_time' not in d:
            d['create_time'] = extract_datetime(self.id)
        return d

    def diff(self, origin: Self = None):
        after = self._data
        before = origin._data if origin else {self.__class__.id.name: self.id}
        return diff_dict(after, before)

    def bulk_write(self, requests, ordered=True, **kwargs) -> BulkWriteResult:
        return self._get_collection().bulk_write(requests, ordered, **kwargs)

    @classmethod
    def batch_range(cls, field, *, start, stop, batch=1000, query=None):
        if not isinstance(field, str):
            field = field.name
        asc = start < stop
        order_by = field if asc else '-' + field
        seen_ids = []
        if query is None:
            query = {}
        while True:
            range_query = {f'{field}__gte': start, f'{field}__lt': stop, f'{cls.id.name}__nin': seen_ids} if asc else \
                {f'{field}__lte': start, f'{field}__gt': stop, f'{cls.id.name}__nin': seen_ids}
            docs = list(cls.objects(**query, **range_query).order_by(order_by).limit(batch))
            if not docs:
                return
            last = docs[-1][field]
            if last != start:
                seen_ids = []
                start = last
            for doc in reversed(docs):
                if doc[field] != last:
                    break
                seen_ids.append(doc.id)
            yield docs


class CacheMixin(DocumentMixin):
    @classmethod
    def make_key(cls, key):
        return cls.id.to_python(key)

    def invalidate(self, invalidator: Invalidator):
        invalidator.publish(self.__class__.__name__, self.id)

    @staticmethod
    def fields_expire(*fields):
        def get_expire(values):
            now = datetime.now()
            expires = [doc[field] for doc in values for field in fields if doc[field] >= now]
            return min(expires) if expires else None

        return get_expire


class TtlCacheMixin(CacheMixin):
    __cache_ttl__ = timedelta(seconds=1)

    @classmethod
    def mget(cls, keys) -> list[Optional[Self]]:
        return [(value, cls.__cache_ttl__) for value in super().mget(keys)]


class RedisCacheMixin(CacheMixin):
    __fields_version__: str = None

    @classmethod
    def make_key(cls, key):
        v = cls.__fields_version__
        if v is None:
            v = cls.__fields_version__ = base62.encode(crc32(' '.join(cls._fields).encode()))
        return f'{cls.__name__}:{v}:{cls.id.to_python(key)}'

    def invalidate(self, invalidator: Invalidator):
        key = self.make_key(self.id)
        invalidator.redis.delete(key)


class Semaphore:
    def __init__(self, redis: Union[Redis, RedisCluster], name, value: int, timeout=timedelta(minutes=1)):
        self.redis = redis
        self.keys = [f'semaphore:{name}:{i}' for i in range(value)]
        self.timeout = timeout
        self.local = local()
        self.lua_release = redis.register_script(Lock.LUA_RELEASE_SCRIPT)
        self.lua_reacquire = redis.register_script(Lock.LUA_REACQUIRE_SCRIPT)

    def __enter__(self):
        assert not self.local.__dict__, 'recursive lock'
        token = str(uuid.uuid4())
        keys = self.keys
        while keys:
            key = choice(keys)
            if self.redis.set(key, token, nx=True, px=self.timeout):
                self.local.key = key
                self.local.token = token
                return self
            values = self.redis.mget_nonatomic(keys) if isinstance(self.redis, RedisCluster) else self.redis.mget(keys)
            keys = [key for key, value in zip(keys, values) if value is None]
        raise LockError('Unable to acquire lock')

    def __exit__(self, exctype, excinst, exctb):
        keys, args = [self.local.key], [self.local.token]
        del self.local.key, self.local.token
        self.lua_release(keys=keys, args=args)

    def reacquire(self):
        timeout = int(self.timeout.total_seconds() * 1000)
        key, token = self.local.key, self.local.token
        if self.lua_reacquire(keys=[key], args=[token, timeout]):
            return
        raise LockError('Lock not owned')


class Inventory:
    def __init__(self, redis: Union[Redis, RedisCluster]):
        self.redis = redis

    def get(self, key, hint=None):
        return self.mget([key], hint)[0]

    def mget(self, keys, hint=None):
        with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.bitfield(key).get(fmt='u32', offset=0).execute()
            return [values[0] for values in pipe.execute()]

    def reset(self, key, value=0, expire=None):
        assert value >= 0
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.bitfield(key).set(fmt='u32', offset=0, value=value).execute()
            if expire is not None:
                pipe.expire(key, expire)
            pipe.execute()

    def incrby(self, key, increment):
        assert increment >= 0
        return self.redis.bitfield(key).incrby(fmt='u32', offset=0, increment=increment).execute()[0]

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

    def validate(self, value):
        value = self.to_mongo(value)
        return super().validate(value)


class Exclusion:
    def __init__(self, redis: Union[Redis, RedisCluster]):
        self.redis = redis

    def __call__(self, pattern: str, timeout=timedelta(minutes=1)):
        def decorator(f):
            params = signature(f).parameters
            names = {index: param.name for index, param in enumerate(params.values())}

            @wraps(f)
            def wrapper(*args, **kwargs):
                values = {names[index]: value for index, value in enumerate(args)}
                key = pattern.format(*args, **values, **kwargs)
                with contextlib.suppress(LockError), Lock(self.redis, key, timeout.total_seconds(), blocking=False):
                    f(*args, **kwargs)

            return wrapper

        return decorator


class TableMixin:
    Session: Callable
    __table__: Any
    __include__ = ()
    __exclude__ = ()

    @classmethod
    def mget(cls, keys) -> list[Optional[Self]]:
        if not keys:
            return []
        pk = cls.__table__.primary_key.columns[0]
        with cls.Session() as session:
            objects = session.query(cls).filter(pk.in_(keys)).all()
            mapping = {getattr(o, pk.name): o for o in objects}
            return [mapping.get(pk.type.python_type(k)) for k in keys]

    @classmethod
    def get(cls, key, *, ensure=False, default=False) -> Optional[Self]:
        value = cls.mget([key])[0]
        if value is None:
            if default:
                pk = cls.__table__.primary_key.columns[0]
                value = cls(**{pk.name: pk.type.python_type(key)})
            elif ensure:
                raise DoesNotExist(f'`{cls.__name__}` `{key}` does not exist')
        return value

    def to_dict(self, include=(), exclude=None):
        if exclude is not None:
            assert not include, '`include`, `exclude` are mutually exclusive'
            columns = self.__table__.columns
            include = self.__include__ + tuple(
                c.name for c in columns if c.name not in exclude and c.name not in self.__include__)
        elif not include:
            include = self.__include__
        d = {k: v for k, v in self.__dict__.items() if k in include and k not in self.__exclude__}
        if 'create_time' in include and 'create_time' not in d:
            pk = self.__table__.primary_key.columns[0]
            d['create_time'] = extract_datetime(getattr(self, pk.name))
        return d

    def diff(self, origin: Self = None):
        after = self.__dict__
        pk = self.__table__.primary_key.columns[0]
        before = origin.__dict__ if origin else {pk.name: getattr(self, pk.name)}
        return diff_dict(after, before)

    @classmethod
    def batch_range(cls, column, *, start, stop, batch=1000, query=()):
        if isinstance(column, str):
            col = getattr(cls, column)
        else:
            col, column = column, column.name
        pk = cls.__table__.primary_key.columns[0]
        asc = start < stop
        order_by = col.asc() if asc else col.desc()
        seen_ids = []
        while True:
            with cls.Session() as session:
                range_query = [col >= start, col < stop, pk.not_in(seen_ids)] if asc else \
                    [col <= start, col > stop, pk.not_in(seen_ids)]
                rows = session.query(cls).filter(*query, *range_query).order_by(order_by).limit(batch).all()
            if not rows:
                return
            last = getattr(rows[-1], column)
            if last != start:
                seen_ids = []
                start = last
            for row in reversed(rows):
                if getattr(row, column) != last:
                    break
                seen_ids.append(getattr(row, pk.name))
            yield rows


class SqlCacheMixin(TableMixin):
    @classmethod
    def make_key(cls, key):
        pk = cls.__table__.primary_key.columns[0]
        return pk.type.python_type(key)

    def invalidate(self, invalidator: Invalidator):
        pk = self.__table__.primary_key.columns[0]
        invalidator.publish(self.__class__.__name__, getattr(self, pk.name))

    @staticmethod
    def columns_expire(*columns):
        def get_expire(values):
            now = datetime.now()
            expires = [getattr(row, column) for row in values for column in columns if getattr(row, column) >= now]
            return min(expires) if expires else None

        return get_expire


def build_order_by(tb, keys):
    if not keys:
        pk = tb.__table__.primary_key.columns[0]
        return [pk.desc()]
    order_by = []
    for key in keys:  # type: str
        asc = True
        if key.startswith('-'):
            asc = False
            key = key[1:]
        column = getattr(tb, key)
        order_by.append(column.asc() if asc else column.desc())
    return order_by


def build_condition(tb, params: dict):
    conditions = []
    for key, value in params.items():
        segments = key.split('__', maxsplit=1)
        key = segments[0]
        op = segments[1] if len(segments) > 1 else 'eq'
        column = getattr(tb, key)
        if op == 'eq':
            conditions.append(column == value)
        elif op == 'ne':
            conditions.append(column != value)
        elif op == 'gt':
            conditions.append(column > value)
        elif op == 'gte':
            conditions.append(column >= value)
        elif op == 'lt':
            conditions.append(column < value)
        elif op == 'lte':
            conditions.append(column <= value)
        elif op == 'in':
            conditions.append(column.in_(value.split(',')))
        elif op == 'nin':
            conditions.append(column.not_in(value.split(',')))
        elif op == 'null':
            conditions.append(column.is_(None))
        elif op == 'not_null':
            conditions.append(column.is_not(None))
        else:
            raise ValueError(f'`{op}` unrecognized operator')
    return and_(*conditions)


def convert_type(tb, params: dict):
    for key, value in params.items():
        column = getattr(tb, key)
        if isinstance(column.type, DateTime):
            format = '%Y-%m-%d %H:%M:%S.%f' if '.' in value else '%Y-%m-%d %H:%M:%S'
            params[key] = datetime.strptime(value, format)
        elif isinstance(column.type, Date):
            params[key] = datetime.strptime(value, '%Y-%m-%d').date()


def build_operation(tb, params: dict):
    operation = {}
    for key, value in params.items():
        segments = key.split('__', maxsplit=1)
        key = segments[-1]
        op = segments[0] if len(segments) > 1 else 'set'
        column = getattr(tb, key)
        if op == 'set':
            operation[key] = value
        elif op == 'unset':
            operation[key] = None
        elif op == 'inc':
            operation[key] = column + value
        elif op == 'dec':
            operation[key] = column - value
        else:
            raise ValueError(f'`{op}` unrecognized operator')
    return operation


class SwitchTracer:
    def __init__(self):
        self._tracing = {}

    def enable(self):
        Hub.settrace(self._trace)

    def __enter__(self):
        g = getcurrent()
        self._tracing[g] = False
        return self

    def __exit__(self, exctype, excinst, exctb):
        g = getcurrent()
        self._tracing.pop(g)

    def is_switched(self):
        g = getcurrent()
        return self._tracing[g]

    def _trace(self, event, args):
        if event != 'switch':
            return
        g = args[0]
        if g in self._tracing:
            self._tracing[g] = True


class UvCache:
    def __init__(self):
        self._cache = defaultdict(set)

    def cache(self, uid, views: Iterable) -> int:
        for view in views:
            self._cache[view].add(uid)
        return len(self._cache)

    def pick(self) -> dict[Any, set]:
        cache = self._cache
        self._cache = defaultdict(set)
        return cache


class PvCache:
    def __init__(self):
        self._cache = defaultdict(int)

    def cache(self, views: Iterable) -> int:
        for view in views:
            self._cache[view] += 1
        return len(self._cache)

    def pick(self) -> dict[Any, int]:
        cache = self._cache
        self._cache = defaultdict(int)
        return cache

    def get(self, view) -> int:
        return self._cache.get(view, 0)


class LogCache:
    def __init__(self):
        self._cache = []

    def cache(self, log) -> int:
        self._cache.append(log)
        return len(self._cache)

    def pick(self) -> list:
        cache = self._cache
        self._cache = []
        return cache
