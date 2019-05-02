from redis import Redis
import gevent
import time
from collections import defaultdict


class Service:
    _PREFIX = 'sErvIcE'
    _INTERVAL = 10

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

    def __init__(self, redis: Redis, services=None):
        self._redis = redis
        self._services = services
        self._runner = None
        self._addresses = defaultdict(set)

    def start(self):
        print(f'start')
        if not self._runner:
            self._runner = gevent.spawn(self._run)

    def stop(self):
        print(f'stop')
        if self._runner:
            gevent.kill(self._runner)
            self._runner = None
            keys = []
            for name, address in self._services:
                key = self._full_key(name, address)
                keys.append(key)
            self._redis.delete(*keys)
            self._redis.publish(self._PREFIX, 'unregister')

    def address(self, name) -> set:
        return self._addresses[name]

    def _run(self):
        published = False
        while True:
            try:
                if self._services:
                    pipe = self._redis.pipeline()
                    for name, address in self._services:
                        key = self._full_key(name, address)
                        pipe.set(key, '', 3 * self._INTERVAL)
                    pipe.execute()
                    if not published:
                        self._redis.publish(self._PREFIX, 'register')
                        published = True
                sub = self._redis.pubsub()
                sub.subscribe(self._PREFIX)
                keys = set(self._redis.scan_iter(match=f'{self._PREFIX}*'))
                self._addresses.clear()
                for key in keys:
                    key = key.decode()
                    name, address = self._unpack(key)
                    self._addresses[name].add(address)
                print(f'{self._addresses}')
                timeout = self._INTERVAL
                while timeout > 0:
                    before = time.time()
                    if sub.get_message(ignore_subscribe_messages=True, timeout=timeout):
                        break
                    timeout -= time.time() - before
                print(f'running')
            except Exception as e:
                print(f'error: {e}')
                gevent.sleep(self._INTERVAL)

