# -*- coding: utf-8 -*-
from __future__ import annotations
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum, auto
from typing import Callable, Any, TypeVar, Generic, Optional, Union, Type
from pymongo import monitoring
from mongoengine import Document, IntField, StringField, connect, DoesNotExist, DateTimeField, FloatField, EnumField, \
    EmbeddedDocument, ListField, EmbeddedDocumentListField
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import BigInteger
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from base.utils import CaseDict
from config import options
import const
from base import FullCache, Cache
from common.shared import invalidator, id_generator

echo = {'debug': 'debug', 'info': True}.get(options.logging, False)
engine = create_engine('sqlite:///db.sqlite3', echo=echo)
Session = sessionmaker(engine)
Base = declarative_base()


@dataclass
class Account(Base):
    id: int

    __tablename__ = "accounts"
    id = Column(BigInteger, primary_key=True)
    username = Column(String(40), unique=True, nullable=False)
    hashed = Column(String(40), nullable=False)


Base.metadata.create_all(engine)

T = TypeVar('T')


class GetterMixin(Generic[T]):
    id: Any
    objects: Callable
    _fields: dict
    _data: dict
    __include__ = None

    @classmethod
    def mget(cls, keys, *, only=()) -> list[Optional[T]]:
        if not keys:
            return []
        query = {f'{cls.id.name}__in': keys}
        mapping = {o.id: o for o in cls.objects(**query).only(*only).limit(len(keys))}
        return [mapping.get(cls.id.to_python(k)) for k in keys]

    @classmethod
    def get(cls, key, *, ensure=True, only=()) -> Optional[T]:
        value = cls.mget([key], only=only)[0]
        if value is None and ensure:
            raise DoesNotExist(f'document `{key}` does not exist')
        return value

    def to_dict(self, include=None, exclude=None):
        if exclude is not None:
            assert not include, '`include`, `exclude` are mutually exclusive'
            include = [field for field in self._fields if field not in exclude and not field.startswith('_')]
        if include is None:
            include = self.__include__
        assert include, 'NO specified fields'
        return {k: v for k, v in self._data.items() if k in include}


class CacheMixin(GetterMixin[T]):
    @classmethod
    def make_key(cls, key, *_, **__):
        return cls.id.to_python(key)  # ignore only

    @classmethod
    def mget(cls, keys, *_, **__) -> list[Optional[T]]:
        return super().mget(keys)  # ignore only

    def invalidate(self):
        invalidator.publish(self.__class__.__name__, self.id)

    @staticmethod
    def fields_expire(*fields):
        def get_expire(values):
            now = datetime.now()
            expires = [doc[field] for doc in values for field in fields if doc[field] >= now]
            return min(expires) if expires else None

        return get_expire


collections: dict[str, Union[Type[Document], Type[CacheMixin]]] = CaseDict()


def collection(coll):
    assert coll.__name__ not in collections
    collections[coll.__name__] = coll
    return coll


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


class CRUD(StrEnum):
    CREATE = auto()
    READ = auto()
    UPDATE = auto()
    DELETE = auto()


class Privilege(EmbeddedDocument):
    coll = StringField(required=True)
    ops = ListField(EnumField(CRUD), required=True)

    def can_access(self, coll, op: CRUD):
        return coll == self.coll and op in self.ops


@collection
class Role(Document, CacheMixin['Role']):
    id = StringField(primary_key=True)
    privileges = EmbeddedDocumentListField(Privilege, required=True)

    def can_access(self, coll, op: CRUD):
        return any(privilege.can_access(coll, op) for privilege in self.privileges)


cache: Cache[Role] = Cache(mget=Role.mget, make_key=Role.make_key, maxsize=None)
cache.listen(invalidator, Role.__name__)
Role.mget = cache.mget


@collection
class Profile(Document, CacheMixin['Profile']):
    __include__ = ['name', 'addr']
    meta = {'strict': False}

    id = IntField(primary_key=True, default=id_generator.gen)
    name = StringField(default='')
    addr = StringField(default='')
    rank = IntField()
    expire = DateTimeField()
    roles = ListField(StringField())

    def can_access(self, coll, op: CRUD):
        return any(role.can_access(coll, op) for role in Role.mget(self.roles) if role)


full_cache: FullCache[Profile] = FullCache(mget=Profile.mget, make_key=Profile.make_key,
                                           get_keys=lambda: Profile.objects.distinct('id'))
full_cache.listen(invalidator, Profile.__name__)
Profile.mget = full_cache.mget


@full_cache.cached(get_expire=Profile.fields_expire('expire'))
def valid_profiles():
    now = datetime.now()
    return [profile for profile in full_cache.values if profile.expire > now]


@collection
class Setting(Document, CacheMixin['Setting']):
    meta = {'strict': False, 'allow_inheritance': True}

    id = StringField(primary_key=True)


cache: Cache[Setting] = Cache(mget=Setting.mget, make_key=Setting.make_key, maxsize=None)
cache.listen(invalidator, Setting.__name__)
Setting.mget = cache.mget


class Status(StrEnum):
    OK = auto()
    ERROR = auto()


@collection
class TokenSetting(Setting):
    expire = TimeDeltaField(default=timedelta(hours=1), max_value=timedelta(days=1))
    status = EnumField(Status, default=Status.OK)

    @classmethod
    def get(cls, key=None, *, ensure=False, only=()) -> TokenSetting:
        assert key is None or key == cls.__name__, 'key is NOT support'
        return super().get(cls.__name__, ensure=ensure, only=only) or cls(id=cls.__name__)


cache.listen(invalidator, TokenSetting.__name__)


class CommandLogger(monitoring.CommandListener):
    def started(self, event):
        if event.command:
            logging.debug('Command {0.command} with request id '
                          '{0.request_id} started on server '
                          '{0.connection_id}'.format(event))

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


if host := options.mongo:
    if options.env is const.Environment.DEV:
        monitoring.register(CommandLogger())
    connect(host=host)
