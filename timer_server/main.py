# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import time
import const
import atexit
import shared
import logging
from typing import Dict, Callable
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from setproctitle import setproctitle
import gevent
from service.timer import Processor
from service.timeout import Client
from base.scheduler import PeriodicCallback
from base.service import Service
from base import LogSuppress, batched
from base.utils import DefaultDict
from pydantic import BaseModel
from dataclasses import dataclass


class Info(BaseModel):
    service: str
    key: str
    data: str
    addr: str
    deadline: float = None
    interval: float = None


@dataclass
class Timer:
    info: Info
    cancel: Callable


class Handler:
    _PREFIX = 'TIMER'

    def __init__(self):
        self._timers = {}  # type: Dict[str, Timer]
        self._services = DefaultDict(
            lambda name: Service(shared.registry, name))  # type: Dict[str, Service]

    def load_timers(self):
        for full_keys in batched(shared.redis.scan_iter(match=f'{self._PREFIX}:*', count=1000), 1000):
            for info in shared.parser.mget_nonatomic(full_keys, Info):
                if info is None or info.addr != options.rpc_address or \
                        self._full_key(info.service, info.key) in self._timers:
                    continue
                if info.deadline is not None:
                    self.call_later(info.service, info.key, info.data, info.deadline - time.time())
                elif info.interval is not None:
                    self.call_repeat(info.service, info.key, info.data, info.interval)
                else:
                    logging.error(f'invalid timer: {info}')

    @classmethod
    def _full_key(cls, service, key):
        return f'{cls._PREFIX}:{service}:{key}'

    def _fire_timer(self, service, key, data):
        logging.debug(f'{service} {key}')
        service = self._services[service]
        addr = None if key == const.TICK_TIMER else service.address(hint=key)
        with service.connection(addr) as conn:
            client = Client(conn)
            client.timeout(key, data)

    def call_later(self, service, key, data, delay):
        logging.debug(f'{service} {key} {delay}')
        full_key = self._full_key(service, key)
        deadline = time.time() + delay
        info = Info(service=service, key=key, data=data, addr=options.rpc_address, deadline=deadline)
        px = max(int(delay * 1000), 1)
        old_info = shared.parser.set(full_key, info, px=px, get=True)
        if old_info and old_info.addr != options.rpc_address:
            self._rpc_delete(old_info)

        def callback():
            self._delete_timer(service, key)
            self._fire_timer(service, key, data)

        handle = shared.scheduler.call_at(callback, deadline)
        self._delete_timer(service, key)
        self._timers[full_key] = Timer(info=info, cancel=handle.cancel)

    def call_repeat(self, service, key, data, interval):
        assert interval > 0
        logging.debug(f'{service} {key} {interval}')
        full_key = self._full_key(service, key)
        info = Info(service=service, key=key, data=data, addr=options.rpc_address, interval=interval)
        old_info = shared.parser.set(full_key, info, get=True)
        if old_info and old_info.addr != options.rpc_address:
            self._rpc_delete(old_info)

        def callback():
            self._fire_timer(service, key, data)

        pc = PeriodicCallback(shared.scheduler, callback, interval)
        self._delete_timer(service, key)
        self._timers[full_key] = Timer(info=info, cancel=pc.stop)

    def remove_timer(self, service, key):
        logging.debug(f'{service} {key}')
        full_key = self._full_key(service, key)
        info = shared.parser.getdel(full_key, Info)
        if info and info.addr != options.rpc_address:
            self._rpc_delete(info)
        self._delete_timer(service, key)

    def _delete_timer(self, service, key):
        full_key = self._full_key(service, key)
        if timer := self._timers.pop(full_key, None):
            logging.debug(f'delete {full_key}')
            timer.cancel()

    @staticmethod
    def _rpc_delete(info: Info):
        logging.debug(f'{info.service} {info.key} {info.addr}')
        with shared.timer_service.client(info.addr) as client:
            # noinspection PyProtectedMember
            client._delete_timer(info.service, info.key)

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
                    if info.deadline is not None:
                        client.call_later(info.service, info.key, info.data, info.deadline - time.time())
                    elif info.interval is not None:
                        client.call_repeat(info.service, info.key, info.data, info.interval)
                    else:
                        logging.error(f'invalid timer: {info}')
        logging.info(f'retreat worker {addr} done')

    def retreat_timers(self):
        if not self._timers:
            return
        addresses = [addr for addr in shared.timer_service.addresses() if addr != options.rpc_address]
        if not addresses:
            logging.error(f'CAN NOT retreat timers: {len(self._timers)}')
            return
        logging.info(f'retreat timers: {len(self._timers)}')
        workers = [gevent.spawn(self._do_retreat, addr) for addr in addresses]
        gevent.joinall(workers)


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
    atexit.register(handler.retreat_timers)  # double check
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    with LogSuppress():
        main()
