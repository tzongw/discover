import logging
import gevent
from redis import Redis

from .utils import LogSuppress


class Registry:
    _PREFIX = 'service'
    _INTERVAL = 10
    _TTL = 3 * _INTERVAL
    COOLDOWN = _TTL + _INTERVAL

    @classmethod
    def _full_key(cls, name):
        return f'{cls._PREFIX}:{name}'

    def __init__(self, redis: Redis, services):
        self._redis = redis
        self._services = services
        self._registered = {}  # type: dict[str, str]
        self._stopped = False
        self._addresses = {}  # type: dict[str, frozenset[str]]
        self._callbacks = []

    def add_callback(self, cb):
        self._callbacks.append(cb)

    def start(self):
        logging.info(f'start')
        self._refresh()
        return [gevent.spawn(self._run)]

    def stop(self):
        logging.info(f'stop {self._registered}')
        self._stopped = True
        self._unregister()

    def register(self, services):
        logging.info(f'register {services}')
        self._registered.update(services)
        self._unregister()  # remove first & wake up

    def _unregister(self):
        if not self._registered:
            return
        with self._redis.pipeline(transaction=False) as pipe:
            for name, address in self._registered.items():
                pipe.hdel(self._full_key(name), address)
            pipe.publish(self._PREFIX, 'unregister')
            pipe.execute()

    def addresses(self, name) -> frozenset[str]:  # constant
        return self._addresses.get(name) or frozenset()

    def _refresh(self):
        addresses = {}
        with self._redis.pipeline(transaction=False) as pipe:
            for name in self._services:
                pipe.hkeys(self._full_key(name))
            for name, keys in zip(self._services, pipe.execute()):
                addresses[name] = frozenset(keys)
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
                if self._registered and not self._stopped:
                    with self._redis.pipeline(transaction=True) as pipe:
                        for name, address in self._registered.items():
                            key = self._full_key(name)
                            pipe.hset(key, address, '')
                            pipe.hexpire(key, self._TTL, address)
                        values = pipe.execute()
                    if self._stopped:  # race
                        self._unregister()
                    elif any(added for added in values[::2]):
                        logging.info(f'publish {self._registered}')
                        self._redis.publish(self._PREFIX, 'register')
                if not sub:
                    sub = self._redis.pubsub()
                    sub.subscribe(self._PREFIX)
                    res = sub.parse_response()
                    logging.info(res)
                self._refresh()
                timeout = self._INTERVAL
                while msg := sub.get_message(timeout=timeout):
                    logging.debug(f'got {msg}')
                    timeout = 0  # exhaust all msgs
            except Exception:
                logging.exception(f'')
                sub = None
                gevent.sleep(self._INTERVAL)
