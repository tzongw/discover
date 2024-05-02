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
from base.scheduler import PeriodicCallback
from base.service import Service
from setproctitle import setproctitle
from base import LogSuppress
from base.utils import DefaultDict
import time
from pydantic import BaseModel
from dataclasses import dataclass
from common.shared import spawn_worker


class Info(BaseModel):
    key: str
    service: str
    data: str
    addr: str
    deadline = 0.0
    interval = 0.0


@dataclass
class Timer:
    info: Info
    cancel: Callable


class Handler:
    _PREFIX = 'TIMER'

    def __init__(self):
        self._timers = {}  # type: Dict[str, Timer]
        self._services = DefaultDict(
            lambda service_name: Service(shared.registry, service_name))  # type: Dict[str, Service]

    def load_timers(self):
        cursor = '0'
        while cursor != 0:
            cursor, full_keys = shared.redis.scan(cursor=cursor, match=f'{self._PREFIX}:*', count=100)
            if not full_keys:
                continue
            for info in shared.parser.mget_nonatomic(full_keys, Info):
                with LogSuppress():
                    if info.addr != options.rpc_address or self._full_key(info.key, info.service) in self._timers:
                        continue
                    if info.deadline:
                        self.call_later(info.key, info.service, info.data, info.deadline - time.time())
                    elif info.interval:
                        self.call_repeat(info.key, info.service, info.data, info.interval)
                    else:
                        logging.error(f'invalid timer: {info}')

    @classmethod
    def _full_key(cls, key, service_name):
        return f'{cls._PREFIX}:{service_name}:{key}'

    def _fire_timer(self, key, service_name, data):
        logging.debug(f'{key} {service_name}')
        service = self._services[service_name]
        addr = None if key == const.TICK_TIMER else service.address(hint=key)
        with service.connection(addr) as conn:
            client = Client(conn)
            client.timeout(key, data)

    def call_later(self, key, service_name, data, delay):
        logging.debug(f'{key} {service_name} {delay}')
        full_key = self._full_key(key, service_name)
        deadline = time.time() + delay
        info = Info(key=key, service=service_name, data=data, addr=options.rpc_address, deadline=deadline)
        px = max(int(delay * 1000), 1)
        old_info = shared.parser.set(full_key, info, px=px, get=True)
        if old_info and old_info.addr != options.rpc_address:
            self._rpc_delete(old_info)

        def callback():
            self._delete_timer(key, service_name)
            self._fire_timer(key, service_name, data)

        handle = shared.scheduler.call_at(callback, deadline)
        self._delete_timer(key, service_name)
        self._timers[full_key] = Timer(info=info, cancel=handle.cancel)

    def call_repeat(self, key, service_name, data, interval):
        assert interval > 0
        logging.debug(f'{key} {service_name} {interval}')
        full_key = self._full_key(key, service_name)
        info = Info(key=key, service=service_name, data=data, addr=options.rpc_address, interval=interval)
        old_info = shared.parser.set(full_key, info, get=True)
        if old_info and old_info.addr != options.rpc_address:
            self._rpc_delete(old_info)

        def callback():
            self._fire_timer(key, service_name, data)

        pc = PeriodicCallback(shared.scheduler, callback, interval)
        self._delete_timer(key, service_name)
        self._timers[full_key] = Timer(info=info, cancel=pc.stop)

    def remove_timer(self, key, service_name):
        logging.debug(f'{key} {service_name}')
        full_key = self._full_key(key, service_name)
        info = shared.parser.getdel(full_key, Info)
        if info and info.addr != options.rpc_address:
            self._rpc_delete(info)
        self._delete_timer(key, service_name)

    def _delete_timer(self, key, service_name):
        full_key = self._full_key(key, service_name)
        if timer := self._timers.pop(full_key, None):
            logging.debug(f'delete {full_key}')
            timer.cancel()

    @staticmethod
    def _rpc_delete(info: Info):
        logging.debug(f'{info.key} {info.service} {info.addr}')
        with shared.timer_service.client(info.addr) as client:
            # noinspection PyProtectedMember
            client._delete_timer(info.key, info.service)

    def _do_retreat(self, addr):
        logging.info(f'retreat worker {addr} start')
        while self._timers and addr in shared.timer_service.addresses():
            full_key, timer = self._timers.popitem()
            logging.debug(f'retreating timer: {full_key}')
            timer.cancel()
            info = timer.info
            with LogSuppress():
                shared.redis.delete(full_key)
                with shared.timer_service.client(addr) as client:
                    if info.deadline:
                        client.call_later(info.key, info.service, info.data, info.deadline - time.time())
                    elif info.interval:
                        client.call_repeat(info.key, info.service, info.data, info.interval)
                    else:
                        logging.error(f'invalid timer: {info}')
        logging.info(f'retreat worker {addr} done')

    def retreat_timers(self):
        if not self._timers:
            return
        addresses = [addr for addr in shared.timer_service.addresses() if addr != options.rpc_address]
        if not addresses:
            logging.error(f'can not retreat timers: {len(self._timers)}')
            return
        logging.info(f'retreat timers: {len(self._timers)}')
        for addr in addresses:
            spawn_worker(self._do_retreat, addr)


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
    workers = [rpc_serve(handler)]
    setproctitle(f'{shared.app_name}-{shared.app_id}-{options.rpc_port}')
    workers += shared.registry.start()
    shared.init_main()
    shared.registry.register({const.RPC_TIMER: f'{options.rpc_address}'})
    handler.load_timers()
    shared.at_exit(handler.retreat_timers)
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    with LogSuppress():
        main()
