# -*- coding: utf-8 -*-
from redis import Redis
from redis.client import bool_ok
from google.protobuf.message import Message
from google.protobuf.json_format import MessageToJson
import uuid


class Timer:
    _PREFIX = 'timer'

    def __init__(self, redis: Redis):
        self._redis = redis
        self._script2sha = {}

    @classmethod
    def _key(cls, key):
        return f'{cls._PREFIX}:{key}'

    def new(self, key: str, data: str, sha: str, interval: int, loop=False):
        params = [key, data, sha, interval]
        if loop:
            params.append('LOOP')
        with self._redis.pipeline(transaction=True) as pipe:
            pipe.execute_command('TIMER.NEW', *params)
            pipe.hset(self._key(key), mapping={
                'data': data,
                'sha': sha,
                'interval': interval
            })
            if loop:
                pipe.persist(self._key(key))
            else:
                pipe.expire(self._key(key), interval // 1000)
            res, *_ = pipe.execute()
        return bool_ok(res)

    def kill(self, *keys):
        with self._redis.pipeline(transaction=True) as pipe:
            pipe.execute_command('TIMER.KILL', *keys)
            pipe.delete(*[self._key(key) for key in keys])
            res, *_ = pipe.execute()
        return int(res)

    def new_stream_timer(self, message: Message, interval: int, loop=False, key=None, maxlen=4096):
        if key is None:
            if loop:
                raise ValueError('loop timer must set key')
            key = str(uuid.uuid4())
        stream = message.stream
        script = f"return redis.call('XADD', '{stream}', 'MAXLEN', '~', '{maxlen}', '*', '', ARGV[1])"
        sha = self._script2sha.get(script)
        if sha is None:
            sha = self._redis.script_load(script)
            self._script2sha[script] = sha
        data = MessageToJson(message)
        self.new(key, data, sha, interval, loop)
        return key
