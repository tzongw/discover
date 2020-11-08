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
from service import timer
import common
from typing import Dict
from registry import Registry
from redis import Redis
import utils
from schedule import Handle, PeriodicCallback, Schedule
from service_pools import ServicePools
from service import timeout
from setproctitle import setproctitle
from utils import LogSuppress

define("host", utils.ip_address(), str, "public host")
define("rpc_port", 0, int, "rpc port")

parse_command_line()

app_name = const.APP_TIMER


class Handler:
    _PREFIX = 'timer'
    _KEY = 'key'
    _SERVICE = 'service'
    _DATA = 'data'
    _DEADLINE = 'deadline'  # one shot
    _INTERVAL = 'interval'  # repeat
    _HANDLE = 'handle'

    def __init__(self, redis: Redis, schedule: Schedule, registry: Registry):
        self._redis = redis
        self._schedule = schedule
        self._registry = registry
        self._timers = {}  # type: Dict[str, dict]
        self._services = {}  # type: Dict[str, ServicePools]

    def load_timers(self):
        full_keys = set(self._redis.scan_iter(match=f'{self._PREFIX}:*'))
        for full_key in full_keys:
            with LogSuppress(Exception):
                key, service_name, data, deadline, interval = self._redis.hmget(full_key, self._KEY, self._SERVICE,
                                                                                self._DATA, self._DEADLINE,
                                                                                self._INTERVAL)
                if deadline:
                    self.call_at(key, service_name, data, float(deadline))
                elif interval:
                    self.call_repeat(key, service_name, data, float(interval))
                else:
                    logging.error(f'invalid timer: {key} {service_name} {data}')

    @classmethod
    def _full_key(cls, key, service_name):
        return f'{cls._PREFIX}:{service_name}:{key}'

    def _fire_timer(self, key, service_name, data):
        logging.info(f'{key} {service_name} {data}')
        service = self._services.get(service_name)
        if not service:
            service = ServicePools(self._registry, service_name)
            self._services[service_name] = service
        with service.connection() as conn:
            client = timeout.Client(conn)
            client.timeout(key, data)

    def call_at(self, key, service_name, data, deadline):
        logging.info(f'{key} {service_name} {data} {deadline}')
        self._delete_timer(key, service_name)  # delete previous
        full_key = self._full_key(key, service_name)
        timer = {self._KEY: key,
                 self._SERVICE: service_name,
                 self._DATA: data,
                 self._DEADLINE: deadline,
                 }
        self._redis.hset(full_key, mapping=timer)
        self._timers[full_key] = timer

        def callback():
            self.remove_timer(key, service_name)
            self._fire_timer(key, service_name, data)

        timer[self._HANDLE] = self._schedule.call_at(callback, deadline)

    def call_repeat(self, key, service_name, data, interval):
        assert interval > 0
        logging.info(f'{key} {service_name} {data} {interval}')
        self._delete_timer(key, service_name)  # delete previous
        full_key = self._full_key(key, service_name)
        timer = {self._KEY: key,
                 self._SERVICE: service_name,
                 self._DATA: data,
                 self._INTERVAL: interval,
                 }
        self._redis.hset(full_key, mapping=timer)
        self._timers[full_key] = timer

        def callback():
            self._fire_timer(key, service_name, data)

        timer[self._HANDLE] = PeriodicCallback(self._schedule, callback, interval).start()

    def remove_timer(self, key, service_name):
        logging.info(f'{key} {service_name}')
        full_key = self._full_key(key, service_name)
        self._redis.delete(full_key)
        self._delete_timer(key, service_name)

    def _delete_timer(self, key, service_name):
        full_key = self._full_key(key, service_name)
        timer = self._timers.pop(full_key, None)
        if timer:
            logging.warning(f'delete {key} {service_name}')
            handle = timer[self._HANDLE]
            if isinstance(handle, Handle):
                handle.cancel()
            elif isinstance(handle, PeriodicCallback):
                handle.stop()


def main():
    handler = Handler(common.redis, common.schedule, common.registry)
    processor = timer.Processor(handler)
    transport = TSocket.TServerSocket(utils.wildcard, options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    def register():
        options.rpc_port = transport.handle.getsockname()[1]
        logging.info(f'Starting the server {options.host}:{options.rpc_port} ...')
        common.registry.start({const.RPC_TIMER: f'{options.host}:{options.rpc_port}'})
        setproctitle(f'{app_name}-{options.host}:{options.rpc_port}')
        handler.load_timers()

    gevent.spawn_later(0.1, register)
    server.serve()


if __name__ == '__main__':
    main()
