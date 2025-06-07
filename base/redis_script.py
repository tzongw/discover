# -*- coding: utf-8 -*-
from typing import Union
from datetime import timedelta
from redis import Redis, RedisCluster
from .utils import redis_name

_SCRIPT = """#!lua name=utils
local function limited_incrby(keys, args)
    local val = redis.call('GET', keys[1])
    local cur = tonumber(val) or 0
    local increment = tonumber(args[1])
    local limit = tonumber(args[2])
    if increment > 0 then
        if cur >= limit then
            return 0
        end
        if limit - cur < increment then
            increment = limit - cur
        end
    else
        if cur <= limit then
            return 0
        end
        if limit - cur > increment then
            increment = limit - cur
        end
    end 
    redis.call('INCRBY', keys[1], increment)
    if not val and args[3] then
        redis.call('PEXPIRE', keys[1], args[3])
    end
    return increment
end

local function compare_set(keys, args)
    if redis.call('GET', keys[1]) == args[1] then
        redis.call('SET', keys[1], unpack(args, 2))
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

local function compare_hset(keys, args)
    if redis.call('HGET', keys[1], args[1]) == args[2] then
        redis.call('HSETEX', keys[1], unpack(args, 3))
        return 1
    else
        return 0
    end
end

local function compare_hdel(keys, args)
    if redis.call('HGET', keys[1], args[1]) == args[2] then
        if args[3] then
            redis.call('HDEL', keys[1], unpack(args, 3))
        else
            redis.call('DEL', keys[1])
        end
        return 1
    else
        return 0
    end
end

local function hsetx(keys, args)
    if redis.call('EXISTS', keys[1]) == 1 then
        redis.call('HSETEX', keys[1], unpack(args))
        return 1
    else
        return 0
    end
end

redis.register_function('limited_incrby', limited_incrby)
redis.register_function('compare_set', compare_set)
redis.register_function('compare_del', compare_del)
redis.register_function('compare_hset', compare_hset)
redis.register_function('compare_hdel', compare_hdel)
redis.register_function('hsetx', hsetx)
"""


class Script:
    loaded = set()

    def __init__(self, redis: Union[Redis, RedisCluster]):
        name = redis_name(redis)
        if name not in self.loaded:
            redis.function_load(_SCRIPT, replace=True)
            self.loaded.add(name)
        self.redis = redis

    def limited_incrby(self, key, increment: int, limit: int, expire: timedelta = None):
        """return result increment"""
        keys_and_args = [key, increment, limit]
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
        return self.redis.fcall('compare_del', 1, key, expected)

    def compare_hset(self, key, field, expected, mapping: dict, expire: timedelta = None, keepttl=False, fnx=False,
                     fxx=False):
        keys_and_args = [key, field, expected]
        keys_and_args += self._hsetex_args(mapping, expire, keepttl, fnx, fxx)
        return self.redis.fcall('compare_hset', 1, *keys_and_args)

    def compare_hdel(self, key, field, expected, *fields):
        """empty fields to del key"""
        return self.redis.fcall('compare_hdel', 1, key, field, expected, *fields)

    @staticmethod
    def _hsetex_args(mapping, expire=None, keepttl=False, fnx=False, fxx=False):
        args = []
        if fnx:
            args.append('FNX')
        if fxx:
            args.append('FXX')
        if expire:
            args += ['PX', int(expire.total_seconds() * 1000)]
        if keepttl:
            args.append('KEEPTTL')
        args += ['FIELDS', 1]
        for pair in mapping.items():
            args += pair
        return args

    def hsetx(self, key, mapping: dict, expire: timedelta = None, keepttl=False, fnx=False, fxx=False):
        keys_and_args = [key]
        keys_and_args += self._hsetex_args(mapping, expire, keepttl, fnx, fxx)
        return self.redis.fcall('hsetx', 1, *keys_and_args)
