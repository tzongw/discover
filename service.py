from redis import Redis
import gevent
import logging

class Service:
    _PREFIX = 'service'
    _INTERVAL = 5

    @classmethod
    def _key_prefix(cls, name):
        return f'{cls._PREFIX}:{name}'

    @classmethod
    def _full_key(cls, name, address):
        return f'{cls._key_prefix(name)}:{address}'

    def __init__(self, redis: Redis, services=None, watching=None):
        self._redis = redis
        self._services = services
        self._watching = watching
        self._runner = None

    def start(self):
        if not self._runner:
            self._runner = gevent.spawn(self._run)

    def stop(self):
        if self._runner:
            gevent.kill(self._runner)
            self._runner.join()
            self._runner = None

    def _run(self):
        try:
            if self._services:
                pipe = self._redis.pipeline()
                for name, address in self._services:
                    key = self._full_key(name, address)
                    pipe.set(key, '', 3 * self._INTERVAL)
                pipe.execute()
        except gevent.GreenletExit:
            logging.info(f'stopped')
        except Exception as e:
            logging.error(f'error: {e}')

