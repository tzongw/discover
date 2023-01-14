# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import const
import shared
import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import gevent
from service import timer
from typing import Dict
from base.registry import Registry
from redis import Redis
from base.schedule import Handle, PeriodicCallback, Schedule
from base.service_pools import ServicePools
from service import timeout
from setproctitle import setproctitle
from base.utils import LogSuppress
import time


class Handler:
    _PREFIX = 'TIMER'
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
        full_keys = set(self._redis.scan_iter(match=f'{self._PREFIX}:*', count=100))
        for full_key in full_keys:
            with LogSuppress(Exception):
                key, service_name, data, deadline, interval = self._redis.hmget(full_key, self._KEY, self._SERVICE,
                                                                                self._DATA, self._DEADLINE,
                                                                                self._INTERVAL)
                if deadline:
                    self.call_later(key, service_name, data, float(deadline) - time.time())
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

    def call_later(self, key, service_name, data, delay):
        deadline = time.time() + delay
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

        timer[self._HANDLE] = PeriodicCallback(self._schedule, callback, interval)

    def remove_timer(self, key, service_name):
        logging.info(f'{key} {service_name}')
        full_key = self._full_key(key, service_name)
        self._redis.delete(full_key)
        self._delete_timer(key, service_name)

    def _delete_timer(self, key, service_name):
        full_key = self._full_key(key, service_name)
        timer = self._timers.pop(full_key, None)
        if timer:
            logging.info(f'delete {key} {service_name}')
            handle = timer[self._HANDLE]
            if isinstance(handle, Handle):
                handle.cancel()
            elif isinstance(handle, PeriodicCallback):
                handle.stop()


def rpc_serve(handler):
    processor = timer.Processor(handler)
    transport = TSocket.TServerSocket(port=options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    g = gevent.spawn(server.serve)
    gevent.sleep(0.1)
    if not options.rpc_port:
        options.rpc_port = transport.handle.getsockname()[1]
    logging.info(f'Starting the server {options.rpc_address} ...')
    return g


def main():
    logging.info(f'{shared.app_name} app id: {shared.app_id}')
    handler = Handler(shared.redis, shared.schedule, shared.registry)
    g = rpc_serve(handler)
    setproctitle(f'{shared.app_name}-{shared.app_id}-{options.rpc_port}')
    shared.registry.start()
    shared.init_main()
    shared.registry.register({const.RPC_TIMER: f'{options.rpc_address}'})
    handler.load_timers()
    gevent.joinall([g], raise_error=True)


if __name__ == '__main__':
    with LogSuppress(Exception):
        main()
