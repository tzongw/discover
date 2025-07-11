# -*- coding: utf-8 -*-
from typing import Union
from datetime import timedelta
from pydantic import BaseModel
from redis import Redis, RedisCluster
from .utils import stream_name, redis_name

_SCRIPT = """#!lua name=timer
local function timer_xadd(keys, args)
    if args[3] then
        return redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], 'HINT', args[3], '*', '', args[1])
    else
        return redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], '*', '', args[1])
    end
end

local function array_to_table(array)
    local t = {}
    for i = 1, #array, 2 do
        t[array[i]] = array[i + 1]
    end
    return t
end

local function timer_tick(keys, args)
    local cur_ts = redis.call('TIME')[1]
    local info = array_to_table(redis.call('XINFO', 'STREAM', keys[1]))
    local tick_ts = string.sub(info['last-generated-id'], 1, -3)
    local init_ts = math.max(tick_ts + 1, cur_ts - args[1])
    for ts = init_ts, cur_ts do
        redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], ts, '', '')
    end
    return math.max(cur_ts - init_ts + 1, 0)
end

redis.register_function('timer_xadd', timer_xadd)
redis.register_function('timer_tick', timer_tick)
"""


class Timer:
    loaded = set()

    def __init__(self, redis: Union[Redis, RedisCluster], *, maxlen=4096, hint=None):
        name = redis_name(redis)
        if name not in self.loaded:
            redis.function_load(_SCRIPT, replace=True)
            self.loaded.add(name)
        self.redis = redis
        self.maxlen = maxlen
        self.hint = hint

    def new(self, key: str, function: str, interval: timedelta, loop: bool, num_keys: int, keys_and_args):
        interval = int(interval.total_seconds() * 1000)
        params = [key, function, interval]
        if loop:
            params.append('LOOP')
        params.append(num_keys)
        params += keys_and_args
        return self.redis.execute_command('TIMER.NEW', *params)

    def kill(self, key: str):
        return self.redis.execute_command('TIMER.KILL', key)

    def exists(self, key: str):
        return self.redis.exists(key)

    def info(self, key: str):
        res = self.redis.execute_command('TIMER.INFO', key)
        if res is None:
            return
        return dict(zip(res[::2], res[1::2]))

    def create(self, key: str, message: BaseModel, interval: timedelta, *, loop=False, stream=None):
        stream = stream or stream_name(message)
        data = message.json(exclude_defaults=True)
        keys_and_args = [stream, data, self.maxlen]
        if self.hint:
            keys_and_args.append(self.hint)
        return self.new(key, 'timer_xadd', interval, loop=loop, num_keys=1, keys_and_args=keys_and_args)

    def tick(self, key: str, stream: str, interval=timedelta(seconds=1), offset=10):
        keys_and_args = [stream, offset, self.maxlen]
        return self.new(key, 'timer_tick', interval, loop=True, num_keys=1, keys_and_args=keys_and_args)
