import logging
from collections import defaultdict
from typing import Set, DefaultDict, Dict
import gevent
from redis import Redis

from .utils import LogSuppress


class Registry:
    _PREFIX = 'service'
    _INTERVAL = 3
    _TTL = 3 * _INTERVAL
    COOLDOWN = _TTL + _INTERVAL

    @classmethod
    def _key_prefix(cls, name):
        return f'{cls._PREFIX}:{name}'

    @classmethod
    def _full_key(cls, name, address):
        return f'{cls._key_prefix(name)}:{address}'

    @classmethod
    def _unpack(cls, key: str):
        assert key.startswith(cls._PREFIX)
        _, name, address = key.split(sep=':', maxsplit=2)
        return name, address

    def __init__(self, redis: Redis):
        self._redis = redis
        self._services = {}  # type: Dict[str, str]
        self._stopped = False
        self._addresses = defaultdict(set)  # type: DefaultDict[str, Set[str]]
        self._callbacks = []

    def add_callback(self, cb):
        self._callbacks.append(cb)

    def start(self):
        logging.info(f'start')
        self._refresh()
        gevent.spawn(self._run)

    def stop(self):
        logging.info(f'stop {self._services}')
        self._stopped = True
        self._unregister()

    def register(self, services):
        logging.info(f'register {services}')
        self._services.update(services)
        self._unregister()  # remove first in case process restarting

    def _unregister(self):
        if not self._services:
            return
        keys = [self._full_key(name, address) for name, address in self._services.items()]
        self._redis.delete(*keys)
        self._redis.publish(self._PREFIX, 'unregister')

    def addresses(self, name) -> Set[str]:  # constant
        return self._addresses.get(name) or set()

    def _refresh(self):
        keys = set(self._redis.scan_iter(match=f'{self._PREFIX}:*', count=100))
        addresses = defaultdict(set)
        for key in keys:
            with LogSuppress():
                name, address = self._unpack(key)
                addresses[name].add(address)
        if addresses != self._addresses:
            logging.info(f'{self._addresses} -> {addresses}')
            self._addresses = addresses
            for cb in self._callbacks:
                with LogSuppress():
                    cb()

    def _run(self):
        sub = None
        while True:
            try:
                if self._services and not self._stopped:
                    with self._redis.pipeline() as pipe:
                        for name, address in self._services.items():
                            key = self._full_key(name, address)
                            pipe.set(key, '', ex=self._TTL, get=True)
                        values = pipe.execute()
                    if self._stopped:  # race
                        self._unregister()
                    elif any(v is None for v in values):
                        logging.info(f'publish {self._services}')
                        self._redis.publish(self._PREFIX, 'register')
                if not sub:
                    sub = self._redis.pubsub()
                    sub.subscribe(self._PREFIX)
                self._refresh()
                msg = sub.get_message(ignore_subscribe_messages=True, timeout=self._INTERVAL)
                if msg is not None:
                    logging.info(f'got {msg}')
            except Exception:
                logging.exception(f'')
                sub = None
                gevent.sleep(self._INTERVAL)
