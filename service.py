from redis import Redis
import gevent
import logging
from collections import defaultdict


class Service:
    _PREFIX = 'sErvIcE'
    _INTERVAL = 5

    @classmethod
    def _key_prefix(cls, name):
        return f'{cls._PREFIX}:{name}'

    @classmethod
    def _full_key(cls, name, address):
        return f'{cls._key_prefix(name)}:{address}'

    @classmethod
    def _unpack(cls, key: str):
        assert key.startswith(cls._PREFIX)
        _, name, address = key.split(':')
        return name, address

    def __init__(self, redis: Redis, services=None, watching=None):
        self._redis = redis
        self._services = services
        self._watching = watching
        self._runner = None
        self._addresses = defaultdict(set)

    def start(self):
        if not self._runner:
            self._runner = gevent.spawn(self._run)

    def stop(self):
        if self._runner:
            gevent.kill(self._runner)
            self._runner.join()
            self._runner = None

    def address(self, name):
        return self._addresses[name]

    def _run(self):
        running = True
        while True:
            try:
                if self._services:
                    pipe = self._redis.pipeline()
                    for name, address in self._services:
                        key = self._full_key(name, address)
                        pipe.set(key, '', 3 * self._INTERVAL)
                    pipe.execute()
                keys = set(self._redis.scan_iter(match=f'{self._PREFIX}*'))
                self._addresses.clear()
                for key in keys:
                    key = key.decode()
                    name, address = self._unpack(key)
                    self._addresses[name].add(address)
            except gevent.GreenletExit:
                logging.info(f'stopped')
                running = False
                return
            except Exception as e:
                logging.error(f'error: {e}')
            finally:
                if running:
                    gevent.sleep(self._INTERVAL)

