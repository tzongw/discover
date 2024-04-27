import logging
from collections import defaultdict
import gevent
from redis import Redis

from .utils import LogSuppress


class Registry:
    _PREFIX = 'service'
    _INTERVAL = 3
    _TTL = 3 * _INTERVAL
    COOLDOWN = _TTL + _INTERVAL

    @classmethod
    def _full_key(cls, name, address):
        return f'{cls._PREFIX}:{name}:{address}'

    @classmethod
    def _unpack(cls, key: str):
        prefix, name, address = key.split(sep=':', maxsplit=2)
        assert prefix == cls._PREFIX
        return name, address

    def __init__(self, redis: Redis):
        self._redis = redis
        self._services = {}  # type: dict[str, str]
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
        logging.info(f'stop {self._services}')
        self._stopped = True
        self._unregister()

    def register(self, services):
        logging.info(f'register {services}')
        self._services.update(services)
        self._unregister()  # remove first & wake up

    def _unregister(self):
        if not self._services:
            return
        keys = [self._full_key(name, address) for name, address in self._services.items()]
        self._redis.delete(*keys)
        self._redis.publish(self._PREFIX, 'unregister')

    def addresses(self, name) -> frozenset[str]:  # constant
        return self._addresses.get(name) or frozenset()

    def _refresh(self):
        keys = set(self._redis.scan_iter(match=f'{self._PREFIX}:*', count=1000))
        addresses = defaultdict(set)
        for key in keys:
            with LogSuppress():
                name, address = self._unpack(key)
                addresses[name].add(address)
        if addresses != self._addresses:
            logging.info(f'{self._addresses} -> {addresses}')
            self._addresses = {k: frozenset(v) for k, v in addresses.items()}
            for cb in self._callbacks:
                with LogSuppress():
                    cb()

    def _run(self):
        sub = None
        while True:
            try:
                if self._services and not self._stopped:
                    with self._redis.pipeline(transaction=False) as pipe:
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
