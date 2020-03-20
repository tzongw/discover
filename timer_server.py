# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import const
from tornado.options import options, define, parse_command_line
import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import gevent
from generated.service import timer
import common
from typing import Dict
from redis.client import Pipeline
from utils import LogSuppress
from redis import Redis
import utils
from schedule import Handle, PeriodicCallback, Schedule

define("host", utils.ip_address(), str, "listen host")
define("rpc_port", 0, int, "rpc port")

parse_command_line()


class Handler:
    _PREFIX = 'timer'
    _KEY = 'key'
    _SERVICE = 'service'
    _DATA = 'data'
    _DEADLINE = 'deadline'  # one shot
    _INTERVAL = 'interval'  # repeat
    _HANDLE = 'handle'

    def __init__(self, redis: Redis, schedule: Schedule):
        self._redis = redis
        self._schedule = schedule
        self._timers = {}  # type: Dict[str, dict]

    @classmethod
    def key(cls, key):
        return f'{cls._PREFIX}:{key}'

    def _fire_timer(self, key, service_name, data):
        pass

    def call_at(self, key, service_name, data, deadline):
        self.remove_timer(key)  # remove first
        redis_key = self.key(key)
        timer = {self._KEY: key,
                 self._SERVICE: service_name,
                 self._DATA: data,
                 self._DEADLINE: deadline,
                 }
        self._redis.hmset(redis_key, timer)

        def callback():
            self._fire_timer(key, service_name, data)
            self.remove_timer(key)

        timer[self._HANDLE] = self._schedule.call_at(callback, deadline)

    def call_repeat(self, key, service_name, data, interval):
        self.remove_timer(key)  # remove first
        redis_key = self.key(key)
        timer = {self._KEY: key,
                 self._SERVICE: service_name,
                 self._DATA: data,
                 self._INTERVAL: interval,
                 }
        self._redis.hmset(redis_key, timer)

        def callback():
            self._fire_timer(key, service_name, data)

        timer[self._HANDLE] = PeriodicCallback(self._schedule, callback, interval).start()

    def remove_timer(self, key):
        redis_key = self.key(key)
        self._redis.delete(redis_key)
        timer = self._timers.pop(key, None)
        if timer:
            handle = timer[self._HANDLE]
            if isinstance(handle, Handle):
                handle.cancel()
            elif isinstance(handle, PeriodicCallback):
                handle.stop()


def main():
    handler = Handler(common.redis, common.schedule)
    processor = timer.Processor(handler)
    transport = TSocket.TServerSocket(utils.addr_wildchar, options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    def register():
        options.rpc_port = transport.handle.getsockname()[1]
        logging.info(f'Starting the server {options.host}:{options.rpc_port} ...')
        common.registry.start({const.RPC_TIMER: f'{options.host}:{options.rpc_port}'})

    gevent.spawn_later(0.1, register)
    server.serve()


if __name__ == '__main__':
    main()
