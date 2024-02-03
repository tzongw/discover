from __future__ import annotations

import dataclasses
import json
from datetime import datetime, date, timedelta
from typing import Any, Callable, Optional, Self

from flask.app import DefaultJSONProvider, Flask
from mongoengine import EmbeddedDocument, DoesNotExist
from pydantic import BaseModel
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

    def to_dict(self, include=None, exclude=None):
        if exclude is not None:
            assert not include, '`include`, `exclude` are mutually exclusive'
            include = [field for field in self._fields if field not in exclude and not field.startswith('_')]
        if include is None:
            include = self.__include__
        assert include, 'NO specified fields'
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
