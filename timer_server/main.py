# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import time
import logging
import contextlib
from typing import Dict
from dataclasses import dataclass
import gevent
from gevent.lock import RLock
from redis import RedisCluster
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from setproctitle import setproctitle
from pydantic import BaseModel
from service.timer import Processor
from service.timeout import Client
from base.scheduler import Handle
from base.service import Service
from base import LogSuppress, JoinGroup
from base.utils import DefaultDict, Base62
from base.chunk import batched
import const
import shared


class Info(BaseModel):
    uniq_id: str
    service: str
    key: str
    data: str
    addr: str
    deadline: float | None = None
    interval: float | None = None

    def __str__(self) -> str:
        return f'{self.uniq_id = } {self.service = } {self.key = } {self.addr = }'


@dataclass(frozen=True)
class Timer:
    info: Info
    handle: Handle


class Handler:
    _PREFIX = 'TIMER'

    def __init__(self):
        self._timers = {}  # type: Dict[str, Timer]
        self._services = DefaultDict(
            lambda name: Service(shared.registry, name, options.host))  # type: Dict[str, Service]
        self._locks = {}  # type: Dict[str, list]  # full_key -> [RLock, refcount]
        self._migrating = False
        self._no_peer_logged = False

    def load_timers(self):
        group = JoinGroup(slow_time=1, name='loading_timers')
        for full_keys in batched(shared.redis.scan_iter(match=f'{self._PREFIX}:*', count=100), 100):
            values = shared.redis.mget_nonatomic(full_keys) if isinstance(shared.redis, RedisCluster) else \
                shared.redis.mget(full_keys)
            for info_data in values:
                if info_data is None:
                    continue
                info = Info.model_validate_json(info_data)
                if info.addr != options.rpc_address:
                    continue
                group.submit(self._create_timer, info, info_data)
        group.join()

    def _create_timer(self, info, info_data):
        if info.deadline is not None:
            self.call_later(info.service, info.key, info.data, info.deadline - time.time(),
                            doing_info=info, info_data=info_data)
        elif info.interval is not None:
            self.call_repeat(info.service, info.key, info.data, info.interval,
                             doing_info=info, info_data=info_data)
        else:
            logging.error(f'invalid timer: {info}')

    @classmethod
    def _full_key(cls, service, key):
        return f'{cls._PREFIX}:{service}:{key}'

    @contextlib.contextmanager
    def _key_lock(self, full_key):
        entry = self._locks.get(full_key)
        if entry is None:
            entry = [RLock(), 0]
            self._locks[full_key] = entry
        entry[1] += 1  # count holders + waiters before any yield
        entry[0].acquire()  # reentrant per greenlet; acquire before try so release always paired
        try:
            yield
        finally:
            entry[0].release()
            entry[1] -= 1
            if entry[1] == 0:
                del self._locks[full_key]

    def _fire_timer(self, service, key, data):
        logging.debug(f'{service} {key}')
        service = self._services[service]
        addr = None if key == const.TICK_TIMER else service.address(hint=key)
        with service.connection(addr) as conn:
            client = Client(conn)
            client.timeout(key, data)

    def call_later(self, service, key, data, delay, *, doing_info=None, info_data=None):
        logging.debug(f'{service} {key} {delay}')
        full_key = self._full_key(service, key)
        deadline = time.time() + delay
        px = max(int(delay * 1000), 1)
        uniq_id = Base62.encode(shared.snowflake.gen())
        with self._key_lock(full_key):
            if info := doing_info:
                info.uniq_id = uniq_id
                info.addr = options.rpc_address
                if not shared.redis.set(full_key, info, ifeq=info_data, px=px):
                    logging.info(f'timer claim failed: {info}')
                    return
            else:
                info = Info(uniq_id=uniq_id, service=service, key=key, data=data, addr=options.rpc_address,
                            deadline=deadline)
                old_info = shared.parser.set(full_key, info, px=px, get=True)
                if old_info and old_info.addr != options.rpc_address:
                    self._rpc_delete(old_info)

            def callback():
                if self._delete_timer(service, key, info.uniq_id):  # fire only if still owned
                    self._fire_timer(service, key, data)

            self._delete_timer(service, key)
            handle = shared.scheduler.call_at(callback, deadline)
            self._timers[full_key] = Timer(info=info, handle=handle)

    def call_repeat(self, service, key, data, interval, *, doing_info=None, info_data=None):
        assert interval > 0
        logging.debug(f'{service} {key} {interval}')
        full_key = self._full_key(service, key)
        uniq_id = Base62.encode(shared.snowflake.gen())
        with self._key_lock(full_key):
            if info := doing_info:
                info.uniq_id = uniq_id
                info.addr = options.rpc_address
                if not shared.redis.set(full_key, info, ifeq=info_data):
                    logging.info(f'timer claim failed: {info}')
                    return
            else:
                info = Info(uniq_id=uniq_id, service=service, key=key, data=data, addr=options.rpc_address,
                            interval=interval)
                old_info = shared.parser.set(full_key, info, get=True)
                if old_info and old_info.addr != options.rpc_address:
                    self._rpc_delete(old_info)
            self._delete_timer(service, key)
            handle = shared.scheduler.call_repeat(lambda: self._fire_timer(service, key, data), interval)
            self._timers[full_key] = Timer(info=info, handle=handle)

    def remove_timer(self, service, key):
        logging.debug(f'{service} {key}')
        full_key = self._full_key(service, key)
        with self._key_lock(full_key):
            self._delete_timer(service, key)
            info = shared.parser.getdel(full_key, Info)
            if info and info.addr != options.rpc_address:
                self._rpc_delete(info)

    def _delete_timer(self, service, key, uniq_id=None):
        full_key = self._full_key(service, key)
        with self._key_lock(full_key):
            timer = self._timers.get(full_key)
            if timer and (uniq_id is None or timer.info.uniq_id == uniq_id):
                logging.debug(f'delete {full_key}')
                self._timers.pop(full_key)
                timer.handle.cancel()
                return True
        return False

    @staticmethod
    def _rpc_delete(info: Info):
        logging.debug(f'{info}')
        with shared.timer_service.client(info.addr) as client:
            # noinspection PyProtectedMember
            client._delete_timer(info.service, info.key, info.uniq_id)

    def _migrate_timer(self, info_data):
        info = Info.model_validate_json(info_data)
        self._create_timer(info, info_data)

    def _do_migrate(self, addr):
        logging.info(f'migrate worker {addr} start')
        while self._timers and addr in shared.timer_service.addresses():
            full_key, timer = self._timers.popitem()
            logging.debug(f'migrating timer: {full_key}')
            timer.handle.cancel()
            info = timer.info
            with LogSuppress():
                info_data = shared.redis.get(full_key)
                if info_data is None:
                    logging.info(f'migrating timer changed: {full_key}')
                    continue
                if Info.model_validate_json(info_data).uniq_id != info.uniq_id:
                    logging.info(f'migrating timer changed: {full_key}')
                    continue
                with shared.timer_service.client(addr) as client:
                    # noinspection PyProtectedMember
                    client._migrate_timer(info_data)
        logging.info(f'migrate worker {addr} done')

    def migrate_timers(self):
        if self._migrating or not self._timers:
            return
        addresses = [addr for addr in shared.timer_service.addresses() if addr != options.rpc_address]
        if not addresses:
            if not self._no_peer_logged:
                self._no_peer_logged = True
                logging.error(f'no peer found, CAN NOT migrate timers: {len(self._timers)}')
            return
        logging.info(f'migrate timers: {len(self._timers)}')
        self._migrating = True
        workers = [gevent.spawn(self._do_migrate, addr) for addr in addresses]
        gevent.joinall(workers)
        self._migrating = False


def rpc_serve(handler):
    processor = Processor(handler)
    transport = TSocket.TServerSocket(port=options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    g = gevent.spawn(server.serve)
    if not options.rpc_port:
        while not transport.handle:
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
    handler.load_timers()
    shared.registry.register({shared.rpc_service: f'{options.rpc_address}'})
    shared.to_exit(handler.migrate_timers)
    shared.to_exit(lambda: shared.scheduler.call_repeat(handler.migrate_timers, interval=0.1))
    shared.at_exit(handler.migrate_timers)  # final check
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logging.exception('')
        exit(1)
