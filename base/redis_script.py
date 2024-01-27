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

redis.register_function('limited_incrby', limited_incrby)
"""


class Script:
    loaded = set()

    def __init__(self, redis: Union[Redis, RedisCluster]):
        name = redis_name(redis)
        if name not in self.loaded:
            redis.function_load(_SCRIPT, replace=True)
            self.loaded.add(name)
        self.redis = redis

    def limited_incrby(self, key: str, amount: int, limit: int, expire: timedelta = None):
        keys_and_args = [key, amount, limit]
        if expire:
            keys_and_args.append(int(expire.total_seconds() * 1000))
        return self.redis.fcall('limited_incrby', 1, *keys_and_args)
