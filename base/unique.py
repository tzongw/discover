# -*- coding: utf-8 -*-
import logging
from random import randrange
from itertools import chain

import gevent
from redis import Redis, RedisCluster
from redis.commands.core import HashDataPersistOptions
from .utils import LogSuppress


class UniqueId:
    _PREFIX = 'unique'
    _INTERVAL = 10
    _TTL = 600

    def __init__(self, redis: Redis | RedisCluster):
        self._redis = redis
        self._keys = {}
        gevent.spawn(self._run)

    def gen(self, biz: str, r: range):
        key = f'{self._PREFIX}:{biz}'
        assert key not in self._keys
        partition = randrange(r.start, r.stop)
        range_chain = chain(range(partition, r.stop), range(r.start, partition))
        for unique_id in range_chain:
            if self._redis.hsetex(key, str(unique_id), '', ex=self._TTL,
                                  data_persist_option=HashDataPersistOptions.FNX):
                logging.info(f'{key} got unique id {unique_id}')
                self._keys[key] = unique_id
                return unique_id
            logging.info(f'{key} conflict id {unique_id}, retry next')
            continue
        raise ValueError('no unique id available')

    def stop(self):
        if not self._keys:
            return
        logging.info(f'stop {self._keys}')
        with self._redis.pipeline(transaction=False) as pipe:
            for key, unique_id in self._keys.items():
                pipe.hdel(key, unique_id)
            pipe.execute()
        self._keys.clear()

    def _run(self):
        while True:
            gevent.sleep(self._INTERVAL)
            with LogSuppress(), self._redis.pipeline(transaction=False) as pipe:
                for key, unique_id in self._keys.items():
                    pipe.hsetex(key, str(unique_id), '', ex=self._TTL)
                pipe.execute()
