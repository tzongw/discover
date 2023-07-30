# -*- coding: utf-8 -*-
from typing import Union
from datetime import timedelta
from redis import Redis, RedisCluster

_SCRIPT = """#!lua name=utils
local function limited_incr(keys, args)
    local cur = tonumber(redis.call('GET', keys[1])) or 0
    if cur >= tonumber(args[1]) then
        return
    end
    redis.call('INCR', keys[1])
    if cur == 0 then
        redis.call('PEXPIRE', keys[1], args[2])
    end
    return cur + 1
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


local function incrby_ex(keys, args)
    local cur = redis.call('GET', keys[1])
    if not cur then
        return
    end
    return redis.call('INCRBY', keys[1], args[1])
end

redis.register_function('limited_incr', limited_incr)
redis.register_function('limited_consume', limited_consume)
redis.register_function('incrby_ex', incrby_ex)
"""


class Script:
    def __init__(self, redis: Union[Redis, RedisCluster]):
        redis.function_load(_SCRIPT, replace=True)
        self.redis = redis

    def limited_incr(self, key: str, limit: int, expire: timedelta):
        return self.redis.fcall('limited_incr', 1, key, limit, int(expire.total_seconds() * 1000))

    def limited_consume(self, key: str, consume: int):
        return self.redis.fcall('limited_consume', 1, key, consume)

    def incrby_ex(self, key: str, amount: int):
        return self.redis.fcall('incrby_ex', 1, key, amount)
