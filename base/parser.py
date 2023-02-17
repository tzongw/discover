# -*- coding: utf-8 -*-
from typing import Union, Type, Optional, List, Dict, TypeVar
from redis import Redis
from redis.client import Pipeline
from pydantic import BaseModel

M = TypeVar('M', bound=BaseModel)


class Parser:
    def __init__(self, redis: Union[Redis, Pipeline]):
        self._redis = redis
        redis.response_callbacks['SET'] = self.set_callback
        redis.response_callbacks['GET'] = redis.response_callbacks['GETDEL'] = self.get_callback
        redis.response_callbacks['MGET'] = self.mget_callback
        redis.response_callbacks['HGETALL'] = self.hget_callback

    @staticmethod
    def set_callback(response, convert=None, **options):
        return convert(response) if convert else Redis.RESPONSE_CALLBACKS['SET'](response, **options)

    @staticmethod
    def get_callback(response, convert=None):
        return convert(response) if convert else response

    @staticmethod
    def mget_callback(response, convert=None):
        return [convert(value) for value in response] if convert else response

    @staticmethod
    def hget_callback(response, convert=None):
        response = Redis.RESPONSE_CALLBACKS['HGETALL'](response)
        return convert(response) if convert else response

    @staticmethod
    def _parser(cls: M):
        return lambda value: cls.parse_raw(value) if value is not None else None

    def set(self, name: str, model: M, **kwargs) -> Union[M, bool]:
        response = self._redis.set(name, model.json(exclude_defaults=True), **kwargs)
        if kwargs.get('get'):
            convert = self._parser(model.__class__)
            if response is self._redis:  # pipeline command staged
                _, options = self._redis.command_stack[-1]
                options['convert'] = convert
            else:
                response = convert(response)
        return response

    def get(self, name: str, cls: Type[M]) -> Optional[M]:
        return self._redis.execute_command('GET', name, convert=self._parser(cls))

    def getdel(self, name: str, cls: Type[M]) -> Optional[M]:
        return self._redis.execute_command('GETDEL', name, convert=self._parser(cls))

    def mget(self, keys, cls: Type[M]) -> List[M]:
        if not keys:
            return []
        return self._redis.execute_command('MGET', *keys, convert=self._parser(cls))

    @staticmethod
    def _pieces(mapping: Dict[str, M]):
        items = []
        for k, v in mapping.items():
            items.extend([k, v.json(exclude_defaults=True)])
        return items

    def mset(self, mapping: Dict[str, M]) -> bool:
        return self._redis.execute_command('MSET', *self._pieces(mapping))

    def msetnx(self, mapping: Dict[str, M]) -> bool:
        return self._redis.execute_command('MSETNX', *self._pieces(mapping))

    def hget(self, name, cls: Type[M]) -> Optional[M]:
        def convert(mapping):
            return cls.parse_obj(mapping) if mapping else None

        return self._redis.execute_command('HGETALL', name, convert=convert)

    def hset(self, name: str, model: M):
        mapping = model.dict(exclude_defaults=True) or model.dict()
        return self._redis.hset(name, mapping=mapping)
