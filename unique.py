# -*- coding: utf-8 -*-
from random import randrange
from itertools import chain
from redis import Redis
from schedule import Schedule
import logging


class UniqueId:
    _PREFIX = 'unique'
    _REFRESH_INTERVAL = 60
    _TTL = 3600

    def __init__(self, schedule: Schedule, redis: Redis, name: str, range: range):
        self._schedule = schedule
        self._redis = redis
        self._name = name
        self._range = range
        self._ids = set()

    def _key(self, id):
        return f'{self._PREFIX}:{self._name}:{id}'

    def generate(self):
        partition = randrange(self._range.start, self._range.stop)
        range_chain = chain(range(partition, self._range.stop), range(partition))
        for id in range_chain:
            key = self._key(id)
            if not self._redis.set(key, '', self._TTL, nx=True):
                continue
            if not self._ids:
                logging.info(f'start')
                self._schedule.call_later(self._refresh, self._REFRESH_INTERVAL)
            self._ids.add(id)
            return id
        raise ValueError('no id')

    def stop(self):
        logging.info(f'stop')
        self._ids.clear()

    def _refresh(self):
        if not self._ids:
            logging.info(f'exit')
            return
        try:
            with self._redis.pipeline(transaction=False) as pipe:
                for id in self._ids:
                    key = self._key(id)
                    pipe.set(key, '', self._TTL)
                pipe.execute()
        except Exception:
            logging.exception(f'')
        finally:
            self._schedule.call_later(self._refresh, self._REFRESH_INTERVAL)
