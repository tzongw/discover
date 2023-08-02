# -*- coding: utf-8 -*-
from typing import Union
from datetime import timedelta
from redis import Redis, RedisCluster

_SCRIPT = """#!lua name=utils
local function limited_incrby(keys, args)
    local cur = tonumber(redis.call('GET', keys[1])) or 0
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
    if cur == 0 then
        redis.call('PEXPIRE', keys[1], args[3])
    end
    return amount
end

redis.register_function('limited_incrby', limited_incrby)
"""


class Script:
    def __init__(self, redis: Union[Redis, RedisCluster]):
        redis.function_load(_SCRIPT, replace=True)
        self.redis = redis

    def limited_incrby(self, key: str, amount: int, limit: int, expire: timedelta):
        return self.redis.fcall('limited_incrby', 1, key, amount, limit, int(expire.total_seconds() * 1000))
