# -*- coding: utf-8 -*-
from __future__ import annotations
import time
import logging
from enum import StrEnum
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Union, Type, Self, Optional
from contextlib import contextmanager, ExitStack
from gevent import threading
from mongoengine import Document, IntField, StringField, connect, DateTimeField, EnumField, \
    EmbeddedDocument, ListField, EmbeddedDocumentListField, BooleanField
from pymongo import monitoring
from sqlalchemy import Integer
from sqlalchemy import Column, Index
from sqlalchemy import String, DateTime
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.inspection import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import const
from base import FullCache, Cache
from base.chunk import LazySequence
from base.utils import PascalCaseDict, log_if_slow
from base.misc import CacheMixin, TimeDeltaField, DoesNotExist
from config import options
from shared import invalidator, id_generator


class SessionMaker(sessionmaker):
    @contextmanager
    def transaction(self):
        with tx_lock, ExitStack() as stack, self() as session, session.begin():
            session.connection().exec_driver_sql('BEGIN IMMEDIATE')
            start_time = time.time()
            stack.callback(log_if_slow, start_time, tx_timeout, 'slow transaction')
            yield session


tx_timeout = 0.1
tx_lock = threading.RLock()
echo = {'debug': 'debug', 'info': True}.get(options.logging, False)
engine = create_engine('sqlite:///db.sqlite3', echo=echo, connect_args={'isolation_level': None, 'timeout': tx_timeout})
Session = SessionMaker(engine, expire_on_commit=False)
Base = declarative_base()


@event.listens_for(engine, 'connect')
def sqlite_connect(conn, rec):
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode = WAL')
    cur.execute('PRAGMA synchronous = NORMAL')
    cur.close()


class BaseModel(Base):
    __abstract__ = True

    @classmethod
    def mget(cls, keys, only=()) -> list[Optional[Self]]:
        if not keys:
            return []
        pk = inspect(cls).primary_key[0]
        if not only:
            only = [cls]
        with Session() as session:
            objects = session.query(*only).filter(pk.in_(keys)).all()
            mapping = {getattr(o, pk.name): o for o in objects}
            return [mapping.get(k) for k in keys]

    @classmethod
    def get(cls, key, *, ensure=False, default=False, only=()) -> Optional[Self]:
        value = cls.mget([key], only=only)[0]
        if value is None:
            if default:
                pk = inspect(cls).primary_key[0]
                value = cls(**{pk.name: pk.type.python_type(key)})
            elif ensure:
                raise DoesNotExist(f'`{cls.__name__}` `{key}` does not exist')
        return value


@dataclass
class Account(BaseModel):
    id: int

    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True)
    username = Column(String(40), unique=True, nullable=False)
    hashed = Column(String(40), nullable=False)
    last_active = Column(DateTime, nullable=False, default=datetime.now)

    Index('idx_last_active', last_active)


collections: dict[str, Union[Type[Document], Type[CacheMixin]]] = PascalCaseDict()


def collection(coll):
    assert coll.__name__ not in collections
    collections[coll.__name__] = coll
    return coll


class CRUD(StrEnum):
    CREATE = 'create'
    READ = 'read'
    UPDATE = 'update'
    DELETE = 'delete'


class Permission(EmbeddedDocument):
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
    permissions = EmbeddedDocumentListField(Permission, required=True)

    def can_access(self, coll, op: CRUD):
        return self.admin or any(permission.can_access(coll, op) for permission in self.permissions)


cache: Cache[Role] = Cache(mget=Role.mget, make_key=Role.make_key, maxsize=None)
cache.listen(invalidator, Role.__name__)
Role.mget = cache.mget


@collection
class Profile(Document, CacheMixin):
    __include__ = ['name', 'addr', 'create_time']
    meta = {'strict': False}

    id = IntField(primary_key=True, default=id_generator.gen)
    name = StringField(default='')
    addr = StringField(default='')
    rank = IntField()
    expire = DateTimeField()
    roles = ListField(StringField())

    def can_access(self, coll, op: CRUD):
        return any(role.can_access(coll, op) for role in Role.mget(self.roles) if role)


def get_all_profiles():
    def get_more():
        nonlocal last_id
        ids = [p.id for p in Profile.objects(id__gt=last_id).only('id').order_by('id').limit(100)]
        if not ids:
            return
        values = full_cache.mget(ids)
        last_id = ids[-1]
        return values

    last_id = 0
    lazy = LazySequence(get_more)
    return lazy, None


full_cache: FullCache[Profile] = FullCache(mget=Profile.mget, make_key=Profile.make_key, get_values=get_all_profiles)
full_cache.listen(invalidator, Profile.__name__)
Profile.mget = full_cache.mget


@full_cache.cached()
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
