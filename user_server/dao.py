# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass
from typing import Callable, Any, TypeVar, Generic, Optional
from pymongo import monitoring
from mongoengine import Document, IntField, StringField, connect
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
    _data: dict
    __fields__ = None

    @classmethod
    def mget(cls, keys) -> list[Optional[T]]:
        query = {f'{cls.id.name}__in': keys}
        mapping = {o.id: o for o in cls.objects(**query)}
        return [mapping.get(cls.id.to_python(k)) for k in keys]

    @classmethod
    def get(cls, key, ensure_exists=True) -> Optional[T]:
        value = cls.mget([key])[0]
        if value is None and ensure_exists:
            raise KeyError(f'document {key} not exists')
        return value

    def to_dict(self, fields=None):
        if fields is None:
            fields = self.__fields__
        return {k: v for k, v in self._data.items() if k in fields} if fields else self._data


class CacheMixin:
    _class_name: str
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
    __fields__ = ['name', 'addr']

    id = IntField(primary_key=True)
    name = StringField(default='')
    addr = StringField(default='')


cache = FullCache(mget=Profile.mget, make_key=Profile.id.to_python, get_keys=lambda: Profile.objects.distinct('id'))
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
