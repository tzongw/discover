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
from service.timer import Processor
from service.timeout import Client
from typing import Dict, Callable
from base.schedule import PeriodicCallback
from base.service import Service
from setproctitle import setproctitle
from base import LogSuppress
import time
from pydantic import BaseModel
from dataclasses import dataclass


class TimerInfo(BaseModel):
    key: str
    service: str
    data: str
    deadline = 0.0
    interval = 0.0


@dataclass
class Timer:
    info: TimerInfo
    cancel: Callable


class Handler:
    _PREFIX = 'TIMER'

    def __init__(self):
        self._timers = {}  # type: Dict[str, Timer]
        self._services = {}  # type: Dict[str, Service]

    def load_timers(self):
        full_keys = set(shared.redis.scan_iter(match=f'{self._PREFIX}:*', count=100))
        for full_key in full_keys:
            with LogSuppress():
                timer_info = shared.parser.get(full_key, TimerInfo)
                if timer_info.deadline:
                    self.call_later(timer_info.key, timer_info.service, timer_info.data,
                                    timer_info.deadline - time.time())
                elif timer_info.interval:
                    self.call_repeat(timer_info.key, timer_info.service, timer_info.data, timer_info.interval)
                else:
                    logging.error(f'invalid timer: {timer_info}')

    @classmethod
    def _full_key(cls, key, service_name):
        return f'{cls._PREFIX}:{service_name}:{key}'

    def _fire_timer(self, key, service_name, data):
        logging.debug(f'{key} {service_name}')
        service = self._services.get(service_name)
        if not service:
            service = Service(shared.registry, service_name)
            self._services[service_name] = service
        with service.connection() as conn:
            client = Client(conn)
            client.timeout(key, data)

    def call_later(self, key, service_name, data, delay):
        logging.info(f'{key} {service_name} {delay}')
        full_key = self._full_key(key, service_name)
        deadline = time.time() + delay
        timer_info = TimerInfo(key=key, service=service_name, data=data, deadline=deadline)
        px = max(int(delay * 1000), 1)
        shared.parser.set(full_key, timer_info, px=px)

        def callback():
            self._delete_timer(full_key)
            self._fire_timer(key, service_name, data)

        handle = shared.schedule.call_at(callback, deadline)
        self._delete_timer(full_key)
        self._timers[full_key] = Timer(info=timer_info, cancel=handle.cancel)

    def call_repeat(self, key, service_name, data, interval):
        assert interval > 0
        logging.info(f'{key} {service_name} {interval}')
        full_key = self._full_key(key, service_name)
        timer_info = TimerInfo(key=key, service=service_name, data=data, interval=interval)
        shared.parser.set(full_key, timer_info)

        def callback():
            self._fire_timer(key, service_name, data)

        pc = PeriodicCallback(shared.schedule, callback, interval)
        self._delete_timer(full_key)
        self._timers[full_key] = Timer(info=timer_info, cancel=pc.stop)

    def remove_timer(self, key, service_name):
        logging.info(f'{key} {service_name}')
        full_key = self._full_key(key, service_name)
        shared.redis.delete(full_key)
        self._delete_timer(full_key)

    def _delete_timer(self, full_key):
        if timer := self._timers.pop(full_key, None):
            logging.info(f'delete {full_key}')
            timer.cancel()


def rpc_serve(handler):
    processor = Processor(handler)
    transport = TSocket.TServerSocket(port=options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    g = gevent.spawn(server.serve)
    if not options.rpc_port:
        gevent.sleep(0.01)
        options.rpc_port = transport.handle.getsockname()[1]
    logging.info(f'Starting the server {options.rpc_address} ...')
    return g


def main():
    logging.info(f'{shared.app_name} app id: {shared.app_id}')
    handler = Handler()
    g = rpc_serve(handler)
    setproctitle(f'{shared.app_name}-{shared.app_id}-{options.rpc_port}')
    shared.registry.start()
    shared.init_main()
    shared.registry.register({const.RPC_TIMER: f'{options.rpc_address}'})
    handler.load_timers()
    gevent.joinall([g], raise_error=True)


if __name__ == '__main__':
    with LogSuppress():
        main()
