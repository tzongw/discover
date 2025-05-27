# -*- coding: utf-8 -*-
import time
from typing import Union
from datetime import timedelta
from redis import Redis, RedisCluster
from .utils import redis_name

_SCRIPT = """#!lua name=ztimer
local function ztimer_poll(keys, args)
    local ts = args[1]
    local timeouts = redis.call('ZRANGE', keys[1], '-inf', ts, 'BYSCORE', 'LIMIT', 0, args[2])
    local result = {}
    if #timeouts == 0 then
        return result
    end
    local metas = redis.call('HMGET', keys[2], unpack(timeouts))
    local loop = {}
    local oneshot = {}
    for i, meta in ipairs(metas) do
        local key = timeouts[i]
        local j = string.find(meta, '|', 3)
        table.insert(result, key)
        table.insert(result, string.sub(meta, j + 1))
        if string.sub(meta, 1, 1) == '1' then
            table.insert(loop, ts + string.sub(meta, 3, j - 1))
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
redis.register_function('ztimer_poll', ztimer_poll)
"""


class ZTimer:
    loaded = set()

    def __init__(self, redis: Union[Redis, RedisCluster], biz):
        name = redis_name(redis)
        if name not in self.loaded:
            redis.function_load(_SCRIPT, replace=True)
            self.loaded.add(name)
        self.redis = redis
        self._timeout_key = f'ztimer:timeout:{{{biz}}}'
        self._meta_key = f'ztimer:meta:{{{biz}}}'

    def new(self, key: str, data: str, interval: timedelta, *, loop=False):
        interval = interval.total_seconds()
        meta = f'{1 if loop else 0}|{interval}|{data}'
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.zadd(self._timeout_key, {key: time.time() + interval})
            pipe.hset(self._meta_key, key, meta)
            return pipe.execute()[0]

    def fire(self, key: str):
        return self.redis.zadd(self._timeout_key, {key: 0}, xx=True)

    def kill(self, key: str):
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self._timeout_key, key)
            pipe.hdel(self._meta_key, key)
            return pipe.execute()[0]

    def exists(self, key: str):
        return self.redis.hexists(self._meta_key, key)

    def info(self, key: str):
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.zscore(self._timeout_key, key)
            pipe.hget(self._meta_key, key)
            timeout, meta = pipe.execute()
        if timeout is None:
            return
        loop, interval, data = meta.split('|', maxsplit=2)
        return {
            'remaining': max(timeout - time.time(), 0),
            'data': data,
            'interval': float(interval),
            'loop': loop == '1',
        }

    def poll(self, limit=100):
        keys_and_args = [self._timeout_key, self._meta_key, time.time(), limit]
        res = self.redis.fcall('ztimer_poll', 2, *keys_and_args)
        return dict(zip(res[::2], res[1::2]))
