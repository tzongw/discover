import logging
import time
from collections import defaultdict
from typing import Set, DefaultDict, Tuple

import gevent
from redis import Redis

import const
from utils import LogSuppress
from contextlib import closing


class Service:
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
        self._services = set()  # type: Set[Tuple[str, str]]
        self._runner = None
        self._addresses = defaultdict(set)  # type: DefaultDict[str, Set[str]]
        self.refresh_callback = None

    def register(self, service_name, address):
        assert self._runner is None
        self._services.add((service_name, address))

    def start(self):
        logging.info(f'start')
        if not self._runner:
            self._unregister()  # in case process restart
            self._refresh()
            self._runner = gevent.spawn(self._run)

    def stop(self):
        logging.info(f'stop')
        if self._runner:
            gevent.kill(self._runner)
            self._runner = None
            self._unregister()

    def _unregister(self):
        keys = []
        for name, address in self._services:
            key = self._full_key(name, address)
            keys.append(key)
        self._redis.delete(*keys)
        self._redis.publish(self._PREFIX, 'unregister')

    def addresses(self, name) -> Set[str]:
        return self._addresses.get(name) or set()

    def _refresh(self):
        keys = set(self._redis.scan_iter(match=f'{self._PREFIX}*'))
        before = self._addresses.copy()
        self._addresses.clear()
        for key in keys:
            with LogSuppress(Exception):
                name, address = self._unpack(key)
                self._addresses[name].add(address)
        if before != self._addresses:
            logging.warning(f'{before} -> {self._addresses}')
            if self.refresh_callback:
                self.refresh_callback()

    def _run(self):
        published = False
        gevent.sleep(1)  # wait unregister publish & socket listen
        while True:
            try:
                if self._services:
                    with self._redis.pipeline() as pipe:
                        for name, address in self._services:
                            key = self._full_key(name, address)
                            pipe.set(key, '', self._TTL)
                        pipe.execute()
                    if not published:
                        logging.info(f'publish {self._services}')
                        self._redis.publish(self._PREFIX, 'register')
                        published = True
                with closing(self._redis.pubsub()) as sub:
                    sub.subscribe(self._PREFIX)
                    self._refresh()
                    timeout = self._REFRESH_INTERVAL
                    while timeout > 0.1:
                        before = time.time()
                        if sub.get_message(ignore_subscribe_messages=True, timeout=timeout):
                            break
                        timeout -= time.time() - before
            except Exception:
                logging.exception(f'')
                gevent.sleep(self._REFRESH_INTERVAL)
