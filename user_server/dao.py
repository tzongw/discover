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
from base import Cache
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


class CacheMixin:
    _class_name: str
    id: Any

    @classmethod
    def key_prefix(cls):
        return cls._class_name

    def invalidate(self):
        key = f'{self.key_prefix()}:{self.id}'
        invalidator.publish(key)


class Profile(Document, GetterMixin['Profile'], CacheMixin):
    id = IntField(primary_key=True)
    name = StringField(default='')
    addr = StringField(default='')


cache = Cache(mget=Profile.mget, make_key=Profile.id.to_python)
cache.listen(invalidator, Profile.key_prefix())
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
