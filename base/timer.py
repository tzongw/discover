# -*- coding: utf-8 -*-
from redis import Redis, RedisCluster
from datetime import timedelta
from typing import Union

from base import clustered, clusters
from .utils import stream_name, timer_name
from pydantic import BaseModel


class Timer:
    _PREFIX = 'timer'
    _SCRIPT = """#!lua name=timer
        local function timer_xadd(keys, args)
          return redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], '*', '', args[1])
        end
        
        local function timer_xadd_hint(keys, args)
          return redis.call('XADD', keys[1], 'MAXLEN', '~', args[2], 'HINT', args[3], '*', '', args[1])
        end
        
        local function timer_tick(keys, args)
          local tick = redis.call('INCR', keys[1])
          return redis.call('XADD', keys[2], 'MAXLEN', '~', 1, '*', '', tick)
        end
        
        redis.register_function('timer_xadd', timer_xadd)
        redis.register_function('timer_xadd_hint', timer_xadd_hint)
        redis.register_function('timer_tick', timer_tick)
    """

    def __init__(self, redis: RedisCluster, hint=None):
        self.redis = redis
        self.hint = hint
        self.registered = False

    def new(self, key: str, function: str, interval: Union[int, timedelta], loop: bool, num_keys: int,
            keys_and_args):
        if isinstance(interval, timedelta):
            interval = int(interval.total_seconds() * 1000)
        assert interval >= 1
        params = [key, function, interval]
        if loop:
            params.append('LOOP')
        params.append(num_keys)
        params += keys_and_args
        return self.redis.execute_command('TIMER.NEW', *params)

    def kill(self, key):
        key = clustered(key)
        return self.redis.execute_command('TIMER.KILL', key)

    def exists(self, key: str):
        key = clustered(key)
        return self.redis.exists(key)

    def info(self, key: str):
        key = clustered(key)
        res = self.redis.execute_command('TIMER.INFO', key)
        if isinstance(res, list):
            return dict(zip(res[::2], res[1::2]))
        return res

    def create(self, message: BaseModel, interval: Union[int, timedelta], *, loop=False, key=None, maxlen=4096,
               do_hint=True, stream=None):
        if not self.registered:
            for node in self.redis.get_primaries():
                redis = Redis(host=node.host, port=node.port)
                redis.function_load(self._SCRIPT, replace=True)
                redis.close()
                self.registered = True
        stream = stream or stream_name(message)
        data = message.json()
        if key is None:
            key = f'{timer_name(message)}:{data}'
        node = hash(key) % clusters
        function = 'timer_xadd'
        keys_and_args = [clustered(stream, node), data, maxlen]
        if do_hint and self.hint:
            function = 'timer_xadd_hint'
            keys_and_args.append(self.hint)
        self.new(clustered(key, node), function, interval, loop=loop, num_keys=1, keys_and_args=keys_and_args)
        return key

    def tick(self, key, interval: Union[int, timedelta], counter, stream):
        node = hash(key) % clusters
        keys_and_args = [clustered(counter, node), clustered(stream, node)]
        return self.new(clustered(key, node), 'timer_tick', interval, loop=True, num_keys=2,
                        keys_and_args=keys_and_args)
