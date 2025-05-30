# -*- coding: utf-8 -*-
from typing import Union
from datetime import timedelta
from redis import Redis, RedisCluster
from .utils import redis_name

_SCRIPT = """#!lua name=utils
local function limited_incrby(keys, args)
    local val = redis.call('GET', keys[1])
    local cur = tonumber(val) or 0
    local amount = tonumber(args[1])
    local limit = tonumber(args[2])
    if amount > 0 then
        if cur >= limit then
            return 0
        end
        if limit - cur < amount then
            amount = limit - cur
        end
    else
        if cur <= limit then
            return 0
        end
        if limit - cur > amount then
            amount = limit - cur
        end
    end 
    redis.call('INCRBY', keys[1], amount)
    if not val and args[3] then
        redis.call('PEXPIRE', keys[1], args[3])
    end
    return amount
end

local function compare_set(keys, args)
    if redis.call('GET', keys[1]) == args[1] then
        redis.call('SET', keys[1], args[2], unpack(args, 3))
        return 1
    else
        return 0
    end
end

local function compare_del(keys, args)
    if redis.call('GET', keys[1]) == args[1] then
        redis.call('DEL', keys[1])
        return 1
    else
        return 0
    end
end

local function hget_set(keys, args)
    local value = redis.call('HGETALL', keys[1])
    redis.call('HSETEX', keys[1], unpack(args))
    return value
end

local function hget_del(keys, args)
    local value = redis.call('HGETALL', keys[1])
    redis.call('DEL', keys[1])
    return value
end

redis.register_function('limited_incrby', limited_incrby)
redis.register_function('compare_set', compare_set)
redis.register_function('compare_del', compare_del)
redis.register_function('hget_set', hget_set)
redis.register_function('hget_del', hget_del)
"""


class Script:
    loaded = set()

    def __init__(self, redis: Union[Redis, RedisCluster]):
        name = redis_name(redis)
        if name not in self.loaded:
            redis.function_load(_SCRIPT, replace=True)
            self.loaded.add(name)
        self.redis = redis

    def limited_incrby(self, key, amount: int, limit: int, expire: timedelta = None):
        keys_and_args = [key, amount, limit]
        if expire:
            keys_and_args.append(int(expire.total_seconds() * 1000))
        return self.redis.fcall('limited_incrby', 1, *keys_and_args)

    def compare_set(self, key, expected, value, expire: timedelta = None, keepttl=False):
        keys_and_args = [key, expected, value]
        if expire:
            keys_and_args += ['PX', int(expire.total_seconds() * 1000)]
        if keepttl:
            keys_and_args.append('KEEPTTL')
        return self.redis.fcall('compare_set', 1, *keys_and_args)

    def compare_del(self, key, expected):
        keys_and_args = [key, expected]
        return self.redis.fcall('compare_del', 1, *keys_and_args)

    def hget_set(self, key, mapping: dict, expire: timedelta = None, keepttl=False, fnx=False, fxx=False):
        keys_and_args = [key]
        if fnx:
            keys_and_args.append('FNX')
        if fxx:
            keys_and_args.append('FXX')
        if expire:
            keys_and_args += ['PX', int(expire.total_seconds() * 1000)]
        if keepttl:
            keys_and_args.append('KEEPTTL')
        keys_and_args += ['FIELDS', 1]
        for pair in mapping.items():
            keys_and_args += pair
        res = self.redis.fcall('hget_set', 1, *keys_and_args)
        return dict(zip(res[::2], res[1::2]))

    def hget_del(self, key):
        res = self.redis.fcall('hget_del', 1, key)
        return dict(zip(res[::2], res[1::2]))
