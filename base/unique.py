# -*- coding: utf-8 -*-
from random import randrange
from itertools import chain
from redis import Redis
from .schedule import Schedule, PeriodicCallback
import logging
from typing import Optional


class UniqueId:
    _PREFIX = 'unique'
    _INTERVAL = 10
    _TTL = 300

    def __init__(self, schedule: Schedule, redis: Redis):
        self._schedule = schedule
        self._redis = redis
        self._keys = set()
        self._pc = None  # type: Optional[PeriodicCallback]

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
            if not self._pc:
                logging.info(f'start')
                self._pc = PeriodicCallback(self._schedule, self._refresh, self._INTERVAL)
            return id
        raise ValueError('no id')

    def stop(self):
        logging.info(f'stop')
        if self._pc:
            self._pc.stop()
            self._pc = None
        if self._keys:
            self._redis.delete(*self._keys)
            self._keys.clear()

    def _refresh(self):
        with self._redis.pipeline() as pipe:
            for key in self._keys:
                pipe.set(key, '', ex=self._TTL)
            pipe.execute()
