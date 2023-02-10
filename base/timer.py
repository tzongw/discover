# -*- coding: utf-8 -*-
from redis import Redis
from datetime import timedelta
from typing import Union
from .utils import stream_name, timer_name
from pydantic import BaseModel


class Timer:
    _PREFIX = 'timer'
    _SCRIPT = """#!lua name=timer
        local function timer_xadd(keys, args)
            return redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], '*', '', args[1])
        end
        
        local function timer_xadd_hint(keys, args)
            return redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], 'HINT', args[3], '*', '', args[1])
        end
        
        local function timer_tick(keys, args)
            local cur_ts = tonumber(redis.call('TIME')[1])
            local tick_ts = tonumber(redis.call('SET', keys[1], cur_ts, 'GET')) or cur_ts
            local init_ts = math.max(tick_ts + 1, cur_ts - args[1])
            for ts = init_ts, cur_ts
            do
                redis.call('XADD', keys[2], 'MAXLEN', '~', args[2], '*', '', ts)
            end
            return math.max(cur_ts - init_ts + 1, 0)
        end
        
        redis.register_function('timer_xadd', timer_xadd)
        redis.register_function('timer_xadd_hint', timer_xadd_hint)
        redis.register_function('timer_tick', timer_tick)
    """

    def __init__(self, redis: Redis, hint=None):
        redis.function_load(self._SCRIPT, replace=True)
        self.redis = redis
        self.hint = hint

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

    def kill(self, key):
        return self.redis.execute_command('TIMER.KILL', key)

    def exists(self, key: str):
        return self.redis.exists(key)

    def info(self, key: str):
        res = self.redis.execute_command('TIMER.INFO', key)
        if isinstance(res, list):
            return dict(zip(res[::2], res[1::2]))
        return res

    def create(self, message: BaseModel, interval: Union[int, timedelta], *, loop=False, key=None, maxlen=4096,
               do_hint=True, stream=None):
        stream = stream or stream_name(message)
        data = message.json()
        if key is None:
            key = f'{timer_name(message)}:{data}'
        function = 'timer_xadd'
        keys_and_args = [stream, data, maxlen]
        if do_hint and self.hint:
            function = 'timer_xadd_hint'
            keys_and_args.append(self.hint)
        self.new(key, function, interval, loop=loop, num_keys=1, keys_and_args=keys_and_args)
        return key

    def tick(self, key, interval: Union[int, timedelta], counter, stream, offset=10, maxlen=1024):
        keys_and_args = [counter, stream, offset, maxlen]
        return self.new(key, 'timer_tick', interval, loop=True, num_keys=2, keys_and_args=keys_and_args)
