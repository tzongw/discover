# -*- coding: utf-8 -*-
from yaml.representer import SafeRepresenter
from yaml.constructor import SafeConstructor
from datetime import timedelta
from .utils import LogSuppress, Addr, ip_address, Parser, ListConverter, Proxy, SingleFlight, stream_name, \
    run_in_thread
from .snowflake import extract_datetime, from_datetime, IdGenerator
from .chunk import chunks
from .defer import deferrable, defer_if, defer
from .pool import Pool
from .thrift_pool import ThriftPool
from .executor import Executor
from .dispatcher import Dispatcher
from .schedule import Schedule
from .invalidator import Invalidator
from .mq import Receiver, Publisher
from .registry import Registry
from .service import Service
from .timer import Timer
from .unique import UniqueId
from .cache import Cache, TTLCache, FullCache, FullTTLCache
from .task import AsyncTask, HeavyTask


def represent_timedelta(self, data):
    return self.represent_scalar('!timedelta', str(data.total_seconds()))


def construct_timedelta(self, node):
    seconds = float(self.construct_scalar(node))
    return timedelta(seconds=seconds)


SafeRepresenter.add_representer(timedelta, represent_timedelta)
SafeConstructor.add_constructor('!timedelta', construct_timedelta)
