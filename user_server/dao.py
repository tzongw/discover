# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass
from typing import Callable, Any, TypeVar, Generic, Optional
from pymongo import monitoring
from mongoengine import Document, IntField, StringField, connect, DoesNotExist
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import BigInteger
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import options
import const
from base import FullCache
from common.shared import invalidator

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


collections = {}


def collection(coll):
    collections[coll.__name__] = coll
    return coll


@collection
class Profile(Document, GetterMixin['Profile'], CacheMixin):
    __include__ = ['name', 'addr']
    meta = {'strict': False}

    id = IntField(primary_key=True)
    name = StringField(default='')
    addr = StringField(default='')
    rank = IntField(required=True)


cache: FullCache[Profile] = FullCache(mget=Profile.mget, make_key=Profile.id.to_python,
                                      get_keys=lambda: Profile.objects.distinct(Profile.id.name))
cache.listen(invalidator, Profile.__name__)
Profile.mget = cache.mget


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
