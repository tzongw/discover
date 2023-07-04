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

    redis.register_function('limited_incr', limited_incr)
"""


def _ensure_script(redis):
    if '__utils_registered__' not in redis.__dict__:
        redis.__dict__['__utils_registered__'] = redis.function_load(_SCRIPT, replace=True)


def limited_incr(redis: Union[Redis, RedisCluster], key: str, limit: int, expire: timedelta):
    _ensure_script(redis)
    return redis.fcall('limited_incr', 1, key, limit, int(expire.total_seconds() * 1000))
