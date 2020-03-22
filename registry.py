import logging
from collections import defaultdict
from typing import Set, DefaultDict

import gevent
from redis import Redis

import const
from utils import LogSuppress


class Registry:
    _PREFIX = 'service'
    _REFRESH_INTERVAL = 3
    _TTL = const.MISS_TIMES * _REFRESH_INTERVAL
    COOL_DOWN = _TTL + _REFRESH_INTERVAL

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
        self._services = {}  # type: Map[str, str]
        self._stopped = False
        self._addresses = defaultdict(set)  # type: DefaultDict[str, Set[str]]
        self._callbacks = []

    def add_callback(self, cb):
        self._callbacks.append(cb)

    def start(self, services):
        logging.info(f'start')
        self._services.update(services)
        self._unregister()  # in case process restart
        self._refresh()
        gevent.spawn_later(1, self._run)  # wait unregister publish & socket listen

    def stop(self):
        logging.info(f'stop')
        self._stopped = True
        self._unregister()

    def _unregister(self):
        if not self._services:
            return
        keys = [self._full_key(name, address) for name, address in self._services.items()]
        self._redis.delete(*keys)
        self._redis.publish(self._PREFIX, 'unregister')

    def addresses(self, name) -> Set[str]:  # constant
        return self._addresses.get(name) or set()

    def _refresh(self):
        keys = set(self._redis.scan_iter(match=f'{self._PREFIX}*'))
        addresses = defaultdict(set)
        for key in keys:
            with LogSuppress(Exception):
                name, address = self._unpack(key)
                addresses[name].add(address)
        if addresses != self._addresses:
            logging.warning(f'{self._addresses} -> {addresses}')
            self._addresses = addresses
            for cb in self._callbacks:
                cb()

    def _run(self):
        published = False
        sub = None
        while not self._stopped:
            try:
                if self._services:
                    with self._redis.pipeline(transaction=False) as pipe:
                        for name, address in self._services.items():
                            key = self._full_key(name, address)
                            pipe.set(key, '', self._TTL)
                        pipe.execute()
                    if self._stopped:  # race
                        self._unregister()
                        return
                    if not published:
                        logging.info(f'publish {self._services}')
                        self._redis.publish(self._PREFIX, 'register')
                        published = True
                if not sub:
                    sub = self._redis.pubsub()
                    sub.subscribe(self._PREFIX)
                self._refresh()
                msg = sub.get_message(ignore_subscribe_messages=True, timeout=self._REFRESH_INTERVAL)
                if msg is not None:
                    logging.info(f'got {msg}')
            except Exception:
                logging.exception(f'')
                sub = None
                gevent.sleep(self._REFRESH_INTERVAL)
