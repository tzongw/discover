# -*- coding: utf-8 -*-
from typing import Union
from datetime import timedelta
from redis import Redis, RedisCluster
from .utils import redis_name

_SCRIPT = """#!lua name=ztimer
local function timestamp()
    local result = redis.call('TIME')
    return result[1] + result[2] / 1000000
end

local function ztimer_add(keys, args)
    redis.call('ZADD', keys[1], timestamp() + args[3], args[1])
    local loop = '0'
    if args[4] then
        loop = '1'
    end
    local meta = loop .. '|' .. args[3] .. '|' .. args[2]
    return redis.call('HSET', keys[2], args[1], meta)
end

local function ztimer_poll(keys, args)
    local ts = timestamp()
    local timeout = redis.call('ZRANGE', keys[1], '-inf', ts, 'BYSCORE', 'LIMIT', 0, args[1])
    local result = {}
    if #timeout == 0 then
        return result
    end
    local meta = redis.call('HMGET', keys[2], unpack(timeout))
    local loop = {}
    local oneshot = {}
    for i, s in ipairs(meta) do
        local key = timeout[i]
        local j = string.find(s, '|', 3)
        table.insert(result, key)
        table.insert(result, s.sub(s, j + 1))
        if string.sub(s, 1, 1) == '1' then
            table.insert(loop, ts + string.sub(s, 3, j - 1))
            table.insert(loop, key)
        else
            table.insert(oneshot, key)
        end
    end
    if #loop > 0 then
        redis.call('ZADD', keys[1], unpack(loop))
    end
    if #oneshot > 0 then
        redis.call('ZREM', keys[1], unpack(oneshot))
        redis.call('HDEL', keys[2], unpack(oneshot))
    end
    return result
end
redis.register_function('ztimer_add', ztimer_add)
redis.register_function('ztimer_poll', ztimer_poll)
"""


class ZTimer:
    loaded = set()

    def __init__(self, redis: Union[Redis, RedisCluster], service):
        name = redis_name(redis)
        if name not in self.loaded:
            redis.function_load(_SCRIPT, replace=True)
            self.loaded.add(name)
        self.redis = redis
        self._meta_key = f'timer.meta:{{{service}}}'
        self._timeout_key = f'timer.timeout:{{{service}}}'

    def new(self, key: str, data: str, interval: timedelta, *, loop=False):
        keys_and_args = [self._timeout_key, self._meta_key, key, data, interval.total_seconds()]
        if loop:
            keys_and_args.append('LOOP')
        return self.redis.fcall('ztimer_add', 2, *keys_and_args)

    def kill(self, key: str):
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.hdel(self._meta_key, key)
            pipe.zrem(self._timeout_key, key)
            return pipe.execute()[0]

    def exists(self, key: str):
        return self.redis.hexists(self._meta_key, key)

    def poll(self, limit=100):
        keys_and_args = [self._timeout_key, self._meta_key, limit]
        res = self.redis.fcall('ztimer_poll', 2, *keys_and_args)
        return dict(zip(res[::2], res[1::2]))
