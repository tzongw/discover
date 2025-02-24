# -*- coding: utf-8 -*-
from __future__ import annotations
import time
import logging
from enum import StrEnum, IntEnum
from datetime import datetime, timedelta
from typing import Type, Self
from contextlib import contextmanager
from gevent import threading
from mongoengine import Document, IntField, StringField, connect, DateTimeField, EnumField, \
    EmbeddedDocument, ListField, EmbeddedDocumentListField, BooleanField
from pymongo import monitoring
from sqlalchemy import Integer, JSON
from sqlalchemy import Column, Index
from sqlalchemy import String, DateTime
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import const
from base import FullCache, Cache
from base.chunk import LazySequence
from base.utils import PascalCaseDict, log_if_slow
from base.misc import GetterMixin, CacheMixin, TimeDeltaField, SqlGetterMixin
from config import options
from shared import invalidator, id_generator
from models import QueueConfig, SmsConfig


class SessionMaker(sessionmaker):
    @contextmanager
    def transaction(self):
        with tx_lock, self() as session, session.begin():
            session.connection().exec_driver_sql('BEGIN IMMEDIATE')
            start_time = time.time()
            yield session
            log_if_slow(start_time, tx_timeout, 'slow transaction')


tx_timeout = 0.1
tx_lock = threading.RLock()
echo = options.env is const.Environment.DEV
engine = create_engine('sqlite:///db.sqlite3', echo=echo, connect_args={'isolation_level': None, 'timeout': tx_timeout})
Session = SessionMaker(engine, expire_on_commit=False)


@event.listens_for(engine, 'connect')
def sqlite_connect(conn, rec):
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode = WAL')
    cur.execute('PRAGMA synchronous = NORMAL')
    cur.close()


@event.listens_for(engine, 'before_cursor_execute')
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    context.query_start_time = time.time()


@event.listens_for(engine, 'after_cursor_execute')
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    start_time = context.query_start_time
    message = f'slow query: {statement} parameters: {parameters}'
    log_if_slow(start_time, options.slow_log, message)


Base = declarative_base()


class BaseModel(Base):
    __abstract__ = True
    Session = Session

    def __init__(self, **kwargs):
        for column in self.__table__.columns:
            if column.name in kwargs or column.default is None:
                continue
            arg = column.default.arg
            kwargs[column.name] = arg(self) if callable(arg) else arg
        super().__init__(**kwargs)


tables: dict[str, Type[BaseModel | SqlGetterMixin]] = {}


def table(tb):
    assert tb.__tablename__ not in tables
    tables[tb.__tablename__] = tb
    return tb


@table
class Account(BaseModel, SqlGetterMixin):
    __tablename__ = 'accounts'
    __include__ = ('id', 'create_time', 'age', 'last_active')
    __exclude__ = ('hashed',)

    id = Column(Integer, primary_key=True, default=id_generator.gen)
    username = Column(String(40), unique=True, nullable=False)
    hashed = Column(String(40), nullable=False)
    age = Column(Integer, nullable=False, default=20, server_default='20')
    last_active = Column(DateTime, nullable=False, default=datetime.now)

    Index('idx_last_active', last_active)


class ConfigKey(IntEnum):
    QUEUE = 1
    SMS = 2


config_models = {
    ConfigKey.QUEUE: QueueConfig,
    ConfigKey.SMS: SmsConfig,
}

ConfigModels = QueueConfig | SmsConfig


class Config(BaseModel, SqlGetterMixin):
    __tablename__ = 'configs'

    id = Column(Integer, primary_key=True)
    value = Column(JSON, nullable=False)
    update_time = Column(DateTime, nullable=False, default=datetime.now)

    @classmethod
    def mget(cls, keys) -> list[ConfigModels]:
        values = []
        for key, config in zip(keys, super().mget(keys)):
            model = config_models[key]
            value = model.parse_obj(config.value) if config else model()
            values.append(value)
        return values

    @classmethod
    def get(cls, key, *, ensure=False, default=True) -> ConfigModels:
        return super().get(key, ensure=ensure, default=default)


collections: dict[str, Type[Document | GetterMixin]] = PascalCaseDict()


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
    __include__ = ('name', 'addr', 'create_time')
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
    def get(cls, key=None, *, ensure=False, default=True) -> Self:
        assert key is None or key == cls.__name__, 'key is NOT support'
        return super().get(cls.__name__, ensure=ensure, default=default)


cache: Cache[Setting] = Cache(mget=Setting.mget, make_key=Setting.make_key, maxsize=None)
cache.listen(invalidator, Setting.__name__)
Setting.mget = cache.mget


class Status(StrEnum):
    OK = 'ok'
    ERROR = 'error'


@collection
class TokenSetting(Setting):
    __include__ = ('expire', 'status')
    expire = TimeDeltaField(default=timedelta(hours=1), max_value=timedelta(days=1))
    status = EnumField(Status, default=Status.OK)


cache.listen(invalidator, TokenSetting.__name__)


class CommandLogger(monitoring.CommandListener):
    def started(self, event):
        if event.command:
            logging.info('Command {0.command} with request id '
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
