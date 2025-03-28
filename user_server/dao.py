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
    EmbeddedDocument, ListField, EmbeddedDocumentListField, BooleanField, DictField, DynamicField
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
from base.utils import PascalCaseDict, log_if_slow, apply_diff
from base.misc import GetterMixin, CacheMixin, TimeDeltaField, SqlGetterMixin, SqlCacheMixin
from config import options
from shared import invalidator, id_generator
from models import QueueConfig, SmsConfig, ConfigModels


class SessionMaker(sessionmaker):
    def __init__(self, bind, *, tx_slow=0.1, **kwargs):
        super().__init__(bind, **kwargs)
        self.tx_slow = tx_slow
        self.tx_lock = threading.RLock()

    @contextmanager
    def transaction(self):
        with self.tx_lock, self() as session, session.begin():
            session.connection().exec_driver_sql('BEGIN IMMEDIATE')
            start_time = time.time()
            yield session
            log_if_slow(start_time, self.tx_slow, 'slow transaction')


echo = options.env == const.Environment.DEV
engine = create_engine('sqlite:///db.sqlite3', echo=echo, connect_args={'isolation_level': None, 'timeout': 0.1})
Session = SessionMaker(engine, expire_on_commit=False)


@event.listens_for(engine, 'connect')
def sqlite_connect(conn, rec):
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode = WAL')
    cur.execute('PRAGMA synchronous = NORMAL')
    cur.close()


if slow_log := options.slow_log:
    @event.listens_for(engine, 'before_cursor_execute')
    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        context.query_start_time = time.time()


    @event.listens_for(engine, 'after_cursor_execute')
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        start_time = context.query_start_time
        message = f'slow query: {statement} parameters: {parameters}'
        log_if_slow(start_time, slow_log, message)

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


class RowChange(BaseModel):
    __tablename__ = 'row_changes'

    id = Column(Integer, primary_key=True, default=id_generator.gen)
    table_name = Column(String, nullable=False)
    row_id = Column(Integer, nullable=False)
    diff = Column(JSON, nullable=False)

    Index('idx_row_id', row_id)
    Index('idx_table_name', table_name)

    @classmethod
    def snapshot(cls, row_id, change_id):
        with cls.Session() as session:
            changes = session.query(cls).filter(cls.row_id == row_id, cls.id <= change_id).order_by(cls.id.asc()).all()
        assert changes and changes[-1].id == change_id, 'CAN NOT find snapshot'
        snapshot = {}
        for change in changes:
            apply_diff(snapshot, change.diff)
        return snapshot


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


@table
class Config(BaseModel, SqlCacheMixin):
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
        assert default
        return super().get(key, ensure=ensure, default=default)


def get_all_configs():
    return tuple(config_cache.mget(ConfigKey)), None


config_cache = FullCache[ConfigModels](mget=Config.mget, maxsize=None, make_key=Config.make_key,
                                       get_values=get_all_configs)
config_cache.listen(invalidator, Config.__name__)
Config.mget = config_cache.mget

collections: dict[str, Type[Document | GetterMixin]] = PascalCaseDict()


def collection(coll):
    assert coll.__name__ not in collections
    collections[coll.__name__] = coll
    return coll


class Change(Document):
    meta = {
        'strict': False,
        'indexes': [
            {'fields': ['doc_id', 'id']},
            {'fields': ['coll_name', 'id']},
        ]
    }

    id = IntField(primary_key=True, default=id_generator.gen)
    coll_name = StringField(required=True)
    doc_id = DynamicField(required=True)
    diff = DictField(required=True)

    @classmethod
    def snapshot(cls, doc_id, change_id):
        changes = list(cls.objects(doc_id=doc_id, id__lte=change_id).order_by('id'))
        assert changes and changes[-1].id == change_id, 'CAN NOT find snapshot'
        snapshot = {}
        for change in changes:
            apply_diff(snapshot, change.diff)
        return snapshot


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


role_cache = Cache[Role](mget=Role.mget, make_key=Role.make_key, maxsize=None)
role_cache.listen(invalidator, Role.__name__)
Role.mget = role_cache.mget


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
        values = profile_cache.mget(ids)
        last_id = ids[-1]
        return values

    last_id = 0
    lazy = LazySequence(get_more)
    return lazy, None


profile_cache = FullCache[Profile](mget=Profile.mget, make_key=Profile.make_key, get_values=get_all_profiles)
profile_cache.listen(invalidator, Profile.__name__)
Profile.mget = profile_cache.mget


@profile_cache.cached()
def valid_profiles():
    now = datetime.now()
    return [profile for profile in profile_cache.values if profile.expire > now]


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
    def started(self, ev):
        if ev.command:
            logging.info(f'Command {ev.command} with request id {ev.request_id} started on server {ev.connection_id}')

    def succeeded(self, ev):
        pass

    def failed(self, ev):
        pass


if host := options.mongo:
    if options.env == const.Environment.DEV:
        monitoring.register(CommandLogger())
    connect(host=host)
