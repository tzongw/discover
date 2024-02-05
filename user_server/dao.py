# -*- coding: utf-8 -*-
from __future__ import annotations
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Union, Type, Self
from mongoengine import Document, IntField, StringField, connect, DateTimeField, EnumField, \
    EmbeddedDocument, ListField, EmbeddedDocumentListField, BooleanField
from pymongo import monitoring
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import const
from base import FullCache, Cache
from base.utils import CaseDict
from base.misc import CacheMixin, TimeDeltaField
from config import options
from shared import invalidator, id_generator

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

collections: dict[str, Union[Type[Document], Type[CacheMixin]]] = CaseDict()


def collection(coll):
    assert coll.__name__ not in collections
    collections[coll.__name__] = coll
    return coll


class CRUD(StrEnum):
    CREATE = 'create'
    READ = 'read'
    UPDATE = 'update'
    DELETE = 'delete'


class Privilege(EmbeddedDocument):
    meta = {'strict': False}

    coll = StringField(required=True)
    ops = ListField(EnumField(CRUD), required=True)

    def can_access(self, coll, op: CRUD):
        return coll == self.coll and op in self.ops


@collection
class Role(Document, CacheMixin):
    meta = {'strict': False}

    id = StringField(primary_key=True)
    admin = BooleanField(default=False)
    privileges = EmbeddedDocumentListField(Privilege, required=True)

    def can_access(self, coll, op: CRUD):
        return self.admin or any(privilege.can_access(coll, op) for privilege in self.privileges)


cache: Cache[Role] = Cache(mget=Role.mget, make_key=Role.make_key, maxsize=None)
cache.listen(invalidator, Role.__name__)
Role.mget = cache.mget


@collection
class Profile(Document, CacheMixin):
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
class Setting(Document, CacheMixin):
    meta = {'strict': False, 'allow_inheritance': True}

    id = StringField(primary_key=True)

    @classmethod
    def get(cls, key=None, *, ensure=False, default=True, only=()) -> Self:
        assert key is None or key == cls.__name__, 'key is NOT support'
        return super().get(cls.__name__, ensure=ensure, default=default, only=only)


cache: Cache[Setting] = Cache(mget=Setting.mget, make_key=Setting.make_key, maxsize=None)
cache.listen(invalidator, Setting.__name__)
Setting.mget = cache.mget


class Status(StrEnum):
    OK = 'ok'
    ERROR = 'error'


@collection
class TokenSetting(Setting):
    expire = TimeDeltaField(default=timedelta(hours=1), max_value=timedelta(days=1))
    status = EnumField(Status, default=Status.OK)


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
