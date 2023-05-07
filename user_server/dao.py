# -*- coding: utf-8 -*-
from __future__ import annotations
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Any, TypeVar, Generic, Optional, Union, Type
from pymongo import monitoring
from mongoengine import Document, IntField, StringField, connect, DoesNotExist, DateTimeField
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import BigInteger
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
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
    def mget(cls, keys) -> list[Optional[T]]:
        query = {f'{cls.id.name}__in': keys}
        mapping = {o.id: o for o in cls.objects(**query)}
        return [mapping.get(cls.id.to_python(k)) for k in keys]

    @classmethod
    def get(cls, key, ensure_exists=True) -> Optional[T]:
        value = cls.mget([key])[0]
        if value is None and ensure_exists:
            raise DoesNotExist(f'document {key} not exists')
        return value

    def to_dict(self, include=None, exclude=None):
        if exclude is not None:
            assert not include, '`include`, `exclude` are mutually exclusive'
            include = [field for field in self._fields if field not in exclude]
        if include is None:
            include = self.__include__
        return {k: v for k, v in self._data.items() if k in include} if include else self._data


class CacheMixin:
    id: Any

    def invalidate(self):
        key = f'{self.__class__.__name__}:{self.id}'
        invalidator.publish(key)

    @staticmethod
    def default_expire(*keys):
        def get_expire(values):
            now = datetime.now()
            expires = [value[key] for value in values for key in keys if value[key] >= now]
            return min(expires) if expires else None

        return get_expire


collections: dict[str, Union[Type[Document], Type[GetterMixin], Type[CacheMixin]]] = {}


def collection(coll):
    collections[coll.__name__] = coll
    return coll


@collection
class Profile(Document, GetterMixin['Profile'], CacheMixin):
    __include__ = ['name', 'addr']
    meta = {'strict': False}

    id = IntField(primary_key=True, default=id_generator.gen)
    name = StringField(default='')
    addr = StringField(default='')
    rank = IntField()
    expire = DateTimeField()


cache: FullCache[Profile] = FullCache(mget=Profile.mget, make_key=Profile.id.to_python,
                                      get_keys=lambda: Profile.objects(expire__gt=datetime.now()).distinct(
                                          Profile.id.name))
cache.listen(invalidator, Profile.__name__)
Profile.mget = cache.mget


@cache.cached(get_expire=Profile.default_expire('expire'))
def valid_profiles():
    now = datetime.now()
    return [profile for profile in cache.values if profile.expire > now]


@collection
class Setting(Document, GetterMixin['Setting'], CacheMixin):
    meta = {'strict': False, 'allow_inheritance': True}

    id = StringField(primary_key=True)


cache: Cache[Setting] = Cache(mget=Setting.mget, make_key=Setting.id.to_python, maxsize=None)
cache.listen(invalidator, Setting.__name__)
Setting.mget = cache.mget


@collection
class TokenSetting(Setting):
    expire = IntField(default=3600)

    @classmethod
    def get(cls, key=None, ensure_exists=False) -> TokenSetting:
        assert key is None or key == cls.__name__, 'key is NOT support'
        return super().get(cls.__name__, ensure_exists) or cls(id=cls.__name__)


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
    if options.env == const.Environment.DEV:
        monitoring.register(CommandLogger())
    connect(host=host)
