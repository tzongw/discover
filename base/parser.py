# -*- coding: utf-8 -*-
from typing import Union, Type, Optional, List, Dict, TypeVar
from redis import Redis, RedisCluster
from redis.client import Pipeline
from pydantic import BaseModel
from redis.cluster import ClusterPipeline

M = TypeVar('M', bound=BaseModel)


def set_callback(response, convert=None, **options):
    return convert(response) if convert else Redis.RESPONSE_CALLBACKS['SET_ORIG'](response, **options)


def callback(response, convert=None):
    return convert(response) if convert else response


def mget_callback(response, convert=None):
    return [convert(value) for value in response] if convert else response


def hget_callback(response, convert=None):
    response = Redis.RESPONSE_CALLBACKS['HGETALL_ORIG'](response)
    return convert(response) if convert else response


Redis.RESPONSE_CALLBACKS['SET_ORIG'] = Redis.RESPONSE_CALLBACKS['SET']
Redis.RESPONSE_CALLBACKS['SET'] = set_callback
Redis.RESPONSE_CALLBACKS['GET'] = callback
Redis.RESPONSE_CALLBACKS['GETDEL'] = callback
Redis.RESPONSE_CALLBACKS['GETEX'] = callback
Redis.RESPONSE_CALLBACKS['HMGET'] = callback
Redis.RESPONSE_CALLBACKS['MGET'] = mget_callback
Redis.RESPONSE_CALLBACKS['HGETALL_ORIG'] = Redis.RESPONSE_CALLBACKS['HGETALL']
Redis.RESPONSE_CALLBACKS['HGETALL'] = hget_callback


class Parser:
    def __init__(self, redis: Union[Redis, Pipeline, RedisCluster, ClusterPipeline]):
        self._redis = redis

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

    def getex(self, name: str, cls: Type[M], **kwargs) -> Optional[M]:
        response = self._redis.getex(name, **kwargs)
        convert = self._parser(cls)
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

    def mset(self, mapping: Dict[str, M]) -> bool:
        mapping = {k: v.json(exclude_defaults=True) for k, v in mapping.items()}
        return self._redis.mset(mapping)

    def msetnx(self, mapping: Dict[str, M]) -> bool:
        mapping = {k: v.json(exclude_defaults=True) for k, v in mapping.items()}
        return self._redis.msetnx(mapping)

    def hget(self, name, cls: Type[M], *, include=None, exclude=None) -> Optional[M]:
        if exclude is not None:
            assert not include, '`include`, `exclude` are mutually exclusive'
            include = [field for field in cls.__fields__ if field not in exclude]
        if include:
            def convert(values):
                mapping = {k: v for k, v in zip(include, values) if v is not None}
                return cls.parse_obj(mapping)

            return self._redis.execute_command('HMGET', name, *include, convert=convert)
        else:
            def convert(mapping):
                return cls.parse_obj(mapping) if mapping else None

            return self._redis.execute_command('HGETALL', name, convert=convert)

    def hset(self, name: str, model: M):
        mapping = model.dict(exclude_unset=True)
        return self._redis.hset(name, mapping=mapping)
