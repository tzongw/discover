# -*- coding: utf-8 -*-
from redis import Redis
from google.protobuf.message import Message
from google.protobuf.json_format import MessageToJson
from datetime import timedelta
from typing import Union
from .utils import stream_name, timer_name


class Timer:
    _PREFIX = 'timer'
    _SCRIPT = """#!lua name=timer
        local function timer_xadd(keys, args)
          return redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], '*', '', args[1])
        end
        
        local function timer_xadd_hint(keys, args)
          return redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], 'HINT', args[3], '*', '', args[1])
        end
        
        redis.register_function('timer_xadd', timer_xadd)
        redis.register_function('timer_xadd_hint', timer_xadd_hint)
    """

    def __init__(self, redis: Redis, hint=None):
        self.redis = redis
        self.hint = hint
        self.registered = False

    def new(self, key: str, function: str, interval: Union[int, timedelta], loop: bool, num_keys: int,
            keys_and_args):
        if isinstance(interval, timedelta):
            interval = int(interval.total_seconds() * 1000)
        assert interval >= 1
        params = [key, function, interval]
        if loop:
            params.append('LOOP')
        params.append(num_keys)
        params += keys_and_args
        return self.redis.execute_command('TIMER.NEW', *params)

    def kill(self, *keys):
        return self.redis.delete(*keys)

    def exists(self, key: str):
        return self.redis.exists(key)

    def create(self, message: Message, interval: Union[int, timedelta], loop=False, key=None, maxlen=4096,
               do_hint=True):
        if not self.registered:
            self.redis.function_load(self._SCRIPT, replace=True)
            self.registered = True
        stream = stream_name(message)
        data = MessageToJson(message)
        if key is None:
            key = f'{timer_name(message)}:{data}'
        function = 'timer_xadd'
        keys_and_args = [stream, data, maxlen]
        if do_hint and self.hint:
            function = 'timer_xadd_hint'
            keys_and_args.append(self.hint)
        self.new(key, function, interval, loop=loop, num_keys=1, keys_and_args=keys_and_args)
        return key
