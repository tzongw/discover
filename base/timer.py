# -*- coding: utf-8 -*-
from redis import Redis
from google.protobuf.message import Message
from google.protobuf.json_format import MessageToJson
import uuid
from datetime import timedelta
from typing import Union
from .utils import stream_name


class Timer:
    _PREFIX = 'TIMER'

    def __init__(self, redis: Redis, cache=False):
        self._redis = redis
        self._script2sha = {}
        self.cache = cache

    @classmethod
    def _key(cls, key):
        return f'{cls._PREFIX}:{key}'

    def new(self, key: str, data: str, sha: str, interval: Union[int, timedelta], loop=False):
        if isinstance(interval, timedelta):
            interval = int(interval.total_seconds() * 1000)
        params = [key, data, sha, interval]
        if loop:
            params.append('LOOP')
        with self._redis.pipeline() as pipe:
            pipe.execute_command('TIMER.NEW', *params)
            if self.cache:
                pipe.hset(self._key(key), mapping={
                    'data': data,
                    'sha': sha,
                    'interval': interval
                })
                if loop:
                    pipe.persist(self._key(key))
                else:
                    pipe.pexpire(self._key(key), interval)
            res, *_ = pipe.execute()
        return res

    def kill(self, *keys):
        with self._redis.pipeline() as pipe:
            pipe.execute_command('TIMER.KILL', *keys)
            if self.cache:
                pipe.delete(*[self._key(key) for key in keys])
            res, *_ = pipe.execute()
        return res

    def create(self, message: Message, interval: Union[int, timedelta], loop=False, key=None, maxlen=4096):
        stream = stream_name(message)
        if key is None:
            key = stream if loop else str(uuid.uuid4())
        script = f"return redis.call('XADD', '{stream}', 'MAXLEN', '~', '{maxlen}', '*', '', ARGV[1])"
        sha = self._script2sha.get(script)
        if sha is None:
            sha = self._redis.script_load(script)
            self._script2sha[script] = sha
        data = MessageToJson(message)
        self.new(key, data, sha, interval, loop)
        return key
