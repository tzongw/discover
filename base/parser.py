# -*- coding: utf-8 -*-
import json
from typing import Union, Type, Optional, List, TypeVar
from redis import Redis, RedisCluster
from redis.client import Pipeline
from pydantic import BaseModel
from redis.cluster import ClusterPipeline
from redis.client import _RedisCallbacks, _RedisCallbacksRESP2
from .misc import JSONEncoder

M = TypeVar('M', bound=BaseModel)


def set_callback(response, convert=None, **options):
    return convert(response) if convert else _RedisCallbacks['SET_ORIG'](response, **options)


def callback(response, convert=None):
    return convert(response) if convert else response


def list_callback(response, convert=None):
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
        callbacks['MGET'] = list_callback
        callbacks['HGETEX'] = list_callback
        callbacks['HMGET'] = callback
        callbacks['HGET'] = callback
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

    def hget(self, name, cls: Type[M], *, default=False) -> Optional[M]:
        def convert(values):
            json_fields = getattr(cls, '__json_fields__', [])
            mapping = {k: json.loads(v) if k in json_fields else v for k, v in zip(fields, values) if v is not None}
            return cls.parse_obj(mapping) if mapping or default else None

        fields = cls.__fields__
        return self._redis.execute_command('HMGET', name, *fields, convert=convert)

    def hset(self, name: str, model: M) -> int:
        json_fields = getattr(model, '__json_fields__', [])
        mapping = model.dict(exclude_unset=True)
        for k, v in mapping.items():
            if k in json_fields:
                mapping[k] = json.dumps(v, cls=JSONEncoder)
        return self._redis.hset(name, mapping=mapping)

    def hgetall(self, name, cls: Type[M]) -> dict[str, M]:
        return self._redis.execute_command('HGETALL', name, convert=cls.parse_raw)

    def hgetex(self, name, keys, cls: Type[M], **kwargs) -> List[M]:
        response = self._redis.hgetex(name, *keys, **kwargs)
        convert = self._parser(cls)
        if response is self._redis:  # pipeline command staged
            _, options = self._redis.command_stack[-1]
            options['convert'] = convert
        else:
            response = [convert(value) for value in response]
        return response

    mget_nonatomic = mget


class ParserCluster(Parser):
    def update_callbacks(self):
        for node in self._redis.get_nodes():
            if redis := node.redis_connection:
                patch_callbacks(redis.response_callbacks)

    def mget_nonatomic(self, keys, cls: Type[M]) -> List[M]:
        assert type(self._redis) is RedisCluster
        response = self._redis.mget_nonatomic(keys)
        return list_callback(response, convert=self._parser(cls))


def create_parser(redis):
    return ParserCluster(redis) if isinstance(redis, RedisCluster) else Parser(redis)
