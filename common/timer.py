# -*- coding: utf-8 -*-
from redis import Redis
from redis.client import bool_ok
from google.protobuf.message import Message
from google.protobuf.json_format import MessageToJson
import uuid


class Timer:
    def __init__(self, redis: Redis):
        self._redis = redis

    def new(self, key: str, data: str, sha: str, interval: int, loop=False):
        params = [key, data, sha, interval]
        if loop:
            params.append('LOOP')
        res = self._redis.execute_command('TIMER.NEW', *params)
        return bool_ok(res)

    def kill(self, *keys):
        res = self._redis.execute_command('TIMER.KILL', *keys)
        return int(res)

    def stream_timer(self, message: Message, interval: int, loop=False, key=None, maxlen=4096):
        if key is None:
            key = str(uuid.uuid4())
        script = self._redis.register_script(f"""
            local table = cjson.decode(ARGV[1])
            local i = 1
            local array = {{}}
            for k, v in pairs(table) do
                array[i] = k
                array[i+1] = v
                i = i + 2
            end
            return redis.call('XADD', table.stream, 'MAXLEN', '~', '{maxlen}', '*', unpack(array))
        """)
        data = MessageToJson(message, including_default_value_fields=True)
        self.new(key, data, script.sha, interval, loop)
        return key


