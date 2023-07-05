# -*- coding: utf-8 -*-
from typing import Union
from datetime import timedelta
from redis import Redis, RedisCluster

_SCRIPT = """#!lua name=utils
    local function limited_incr(keys, args)
        local cur = tonumber(redis.call('GET', keys[1])) or 0
        if cur < tonumber(args[1]) then
            redis.call('INCR', keys[1])
            if cur == 0 then
                redis.call('PEXPIRE', keys[1], args[2])
            end
            return cur + 1
        end
    end

    local function limited_consume(keys, args)
        local cur = redis.call('GET', keys[1])
        if not cur then
            return
        end
        local consume = math.min(tonumber(cur), tonumber(args[1]))
        if consume > 0 then
            redis.call('DECRBY', keys[1], consume)
        end
        return consume
    end

    redis.register_function('limited_incr', limited_incr)
    redis.register_function('limited_consume', limited_consume)
"""


def _ensure_script(redis):
    if '__utils_registered__' not in redis.__dict__:
        redis.__dict__['__utils_registered__'] = redis.function_load(_SCRIPT, replace=True)


def limited_incr(redis: Union[Redis, RedisCluster], key: str, limit: int, expire: timedelta):
    _ensure_script(redis)
    return redis.fcall('limited_incr', 1, key, limit, int(expire.total_seconds() * 1000))


def limited_consume(redis: Union[Redis, RedisCluster], key: str, consume):
    _ensure_script(redis)
    return redis.fcall('limited_consume', 1, key, consume)
