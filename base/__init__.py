# -*- coding: utf-8 -*-
from datetime import timedelta, datetime, date
from redis.connection import Encoder
from redis._parsers.commands import CommandsParser
from yaml.representer import SafeRepresenter
from yaml.constructor import SafeConstructor
from pydantic import BaseModel
from .utils import LogSuppress, Addr, ip_address, stream_name, Base62, func_desc
from .misc import ListConverter, Exclusion
from .singleflight import Singleflight, singleflight, once
from .parser import create_parser, Parser
from .snowflake import extract_datetime, from_datetime, IdGenerator
from .chunk import batched
from .defer import deferrable, defer_if, defer
from .pool import Pool
from .thrift_pool import ThriftPool
from .executor import Executor, WaitGroup
from .dispatcher import Dispatcher, TimeDispatcher
from .scheduler import Scheduler
from .invalidator import create_invalidator, Invalidator
from .mq import Receiver, Publisher
from .registry import Registry
from .service import Service
from .timer import Timer
from .unique import UniqueId
from .cache import Cache, TtlCache, FullCache, FullTtlCache
from .task import AsyncTask, HeavyTask
from .poller import Poller
from .redis_script import Script
from .ztimer import ZTimer


def _represent_timedelta(self, data):
    return self.represent_scalar('!timedelta', str(data.total_seconds()))


def _construct_timedelta(self, node):
    seconds = float(self.construct_scalar(node))
    return timedelta(seconds=seconds)


SafeRepresenter.add_representer(timedelta, _represent_timedelta)
SafeConstructor.add_constructor('!timedelta', _construct_timedelta)


def _get_moveable_keys(self, redis_conn, *args):
    cmd = args[0]
    if cmd == 'FCALL':
        keys_count = int(args[2])
        return args[3:3 + keys_count]
    elif cmd == 'XREADGROUP':
        streams_index = args.index(b'STREAMS', 4) + 1
        streams_count = (len(args) - streams_index) // 2
        return args[streams_index:streams_index + streams_count]
    elif cmd == 'MIGRATE':
        return redis_conn.execute_command('COMMAND GETKEYS', *args)
    raise NotImplementedError(f'unrecognized command {cmd}')


def _encode(self: Encoder, value):
    if isinstance(value, BaseModel):
        value = value.json(exclude_defaults=True)
    elif isinstance(value, datetime):
        value = value.strftime('%Y-%m-%d %H:%M:%S.%f')
    elif isinstance(value, date):
        value = value.strftime('%Y-%m-%d')
    elif isinstance(value, timedelta):
        value = value.total_seconds()
    return _orig_encode(self, value)


CommandsParser._get_moveable_keys = _get_moveable_keys
_orig_encode = Encoder.encode
Encoder.encode = _encode
