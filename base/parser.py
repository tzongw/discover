# -*- coding: utf-8 -*-
from typing import Union, Type, Optional, List, TypeVar
from redis import Redis, RedisCluster
from redis.client import Pipeline
from pydantic import BaseModel
from redis.cluster import ClusterPipeline
from redis.client import _RedisCallbacks, _RedisCallbacksRESP2

M = TypeVar('M', bound=BaseModel)


def set_callback(response, convert=None, **options):
    return convert(response) if convert else _RedisCallbacks['SET_ORIG'](response, **options)


def callback(response, convert=None):
    return convert(response) if convert else response


def mget_callback(response, convert=None):
    return [convert(value) for value in response] if convert else response


def hgetall_callback(response, convert=None):
    response = _RedisCallbacksRESP2['HGETALL_ORIG'](response)
    return {k: convert(v) for k, v in response.items()} if convert else response


def patch_callbacks(callbacks):
    if 'SET_ORIG' not in callbacks and 'SET' in callbacks:
        callbacks['SET_ORIG'] = callbacks['SET']
        callbacks['SET'] = set_callback
        callbacks['GET'] = callback
        callbacks['GETDEL'] = callback
        callbacks['GETEX'] = callback
        callbacks['MGET'] = mget_callback
        callbacks['HMGET'] = callback
    if 'HGETALL_ORIG' not in callbacks and 'HGETALL' in callbacks:
        callbacks['HGETALL_ORIG'] = callbacks['HGETALL']
        callbacks['HGETALL'] = hgetall_callback


patch_callbacks(_RedisCallbacks)
patch_callbacks(_RedisCallbacksRESP2)


class Parser:
    def __init__(self, redis: Union[Redis, Pipeline, RedisCluster, ClusterPipeline]):
        self._redis = redis
        self.update_callbacks()

    def update_callbacks(self):
        patch_callbacks(self._redis.response_callbacks)

    @staticmethod
    def _parser(cls: M):
        return lambda value: cls.parse_raw(value) if value is not None else None

    def set(self, name: str, model: M, **kwargs) -> Union[M, bool]:
        response = self._redis.set(name, model, **kwargs)
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
        return self._redis.execute_command('MGET', *keys, convert=self._parser(cls))

    def hget(self, name, cls: Type[M], *, include=(), exclude=()) -> Optional[M]:
        if not include:
            include = [field for field in cls.__fields__ if field not in exclude]
        else:
            assert not exclude, '`include`, `exclude` are mutually exclusive'

        def convert(values):
            mapping = {k: v for k, v in zip(include, values) if v is not None}
            return cls.parse_obj(mapping) if mapping else None

        return self._redis.execute_command('HMGET', name, *include, convert=convert)

    def hset(self, name: str, model: M) -> int:
        mapping = model.dict(exclude_unset=True)
        return self._redis.hset(name, mapping=mapping)

    def hgetall(self, name, cls: Type[M]):
        return self._redis.execute_command('HGETALL', name, convert=cls.parse_raw)

    mget_nonatomic = mget


class ParserCluster(Parser):
    def update_callbacks(self):
        for node in self._redis.get_nodes():
            if redis := node.redis_connection:
                patch_callbacks(redis.response_callbacks)

    def mget_nonatomic(self, keys, cls: Type[M]) -> List[M]:
        assert type(self._redis) is RedisCluster
        response = self._redis.mget_nonatomic(keys)
        return mget_callback(response, convert=self._parser(cls))


def create_parser(redis):
    return ParserCluster(redis) if isinstance(redis, RedisCluster) else Parser(redis)
