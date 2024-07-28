# -*- coding: utf-8 -*-
import logging
from random import randrange
from itertools import chain

import gevent
from redis import Redis
from .utils import LogSuppress


class UniqueId:
    _PREFIX = 'unique'
    _INTERVAL = 10
    _TTL = 600

    def __init__(self, redis: Redis):
        self._redis = redis
        self._keys = set()
        gevent.spawn(self._run)

    def _key(self, biz: str, id: int):
        return f'{self._PREFIX}:{biz}:{id}'

    def gen(self, biz: str, r: range):
        partition = randrange(r.start, r.stop)
        range_chain = chain(range(partition, r.stop), range(r.start, partition))
        for id in range_chain:
            key = self._key(biz, id)
            if not self._redis.set(key, '', ex=self._TTL, nx=True):
                logging.info(f'{biz} conflict id {id}, retry next')
                continue
            logging.info(f'{biz} got unique id {id}')
            self._keys.add(key)
            return id
        raise ValueError('no id')

    def stop(self):
        logging.info(f'stop {self._keys}')
        if self._keys:
            self._redis.delete(*self._keys)
            self._keys.clear()

    def _run(self):
        while True:
            gevent.sleep(self._INTERVAL)
            with LogSuppress():
                with self._redis.pipeline(transaction=False) as pipe:
                    for key in self._keys:
                        pipe.set(key, '', ex=self._TTL)
                    pipe.execute()
