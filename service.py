from redis import Redis


class Service:
    _PREFIX = 'service'

    @classmethod
    def _key_prefix(cls, name):
        return f'{cls._PREFIX}:{name}'

    @classmethod
    def _full_key(cls, name, address):
        return f'{cls._key_prefix(name)}:{address}'

    def __init__(self, redis: Redis):
        self._redis = redis
        self._watched = set()

    def register(self, name, address):
        key = self._full_key(name, address)
        self._redis.set(key, '')

    def unregister(self, name, address):
        key = self._full_key(name, address)
        self._redis.delete(key)

    def watch(self, name):
        self._watched.add(name)

    def unwatch(self, name):
        self._watched.remove(name)
