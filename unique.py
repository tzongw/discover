# -*- coding: utf-8 -*-
from random import randrange
from itertools import chain
from redis import Redis
from schedule import Schedule, PeriodicCallback
import logging
from typing import Optional


class UniqueId:
    _PREFIX = 'unique'
    _REFRESH_INTERVAL = 10
    _TTL = 1800

    def __init__(self, schedule: Schedule, redis: Redis, name: str, range: range):
        self._schedule = schedule
        self._redis = redis
        self._name = name
        self._range = range
        self._ids = set()
        self._pc = None  # type: Optional[PeriodicCallback]

    def _key(self, id):
        return f'{self._PREFIX}:{self._name}:{id}'

    def generate(self):
        partition = randrange(self._range.start, self._range.stop)
        range_chain = chain(range(partition, self._range.stop), range(partition))
        for id in range_chain:
            key = self._key(id)
            if not self._redis.set(key, '', self._TTL, nx=True):
                logging.debug(f'conflict id {id}, retry next')
                continue
            logging.debug(f'got unique id {id}')
            self._ids.add(id)
            if not self._pc:
                logging.info(f'start')
                self._pc = PeriodicCallback(self._schedule, self._refresh, self._REFRESH_INTERVAL).start()
            return id
        raise ValueError('no id')

    def stop(self):
        logging.info(f'stop')
        if self._pc:
            self._pc.stop()
            self._pc = None
        if self._ids:
            keys = [self._key(id) for id in self._ids]
            self._redis.delete(*keys)
            self._ids.clear()

    def _refresh(self):
        with self._redis.pipeline(transaction=False) as pipe:
            for id in self._ids:
                key = self._key(id)
                pipe.set(key, '', self._TTL)
            pipe.execute()
