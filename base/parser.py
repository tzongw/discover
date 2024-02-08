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


def patch_callbacks(callbacks):
    if 'SET_ORIG' in callbacks:  # already done
        return
    callbacks['SET_ORIG'] = callbacks['SET']
    callbacks['SET'] = set_callback
    callbacks['GET'] = callback
    callbacks['GETDEL'] = callback
    callbacks['GETEX'] = callback
    callbacks['HMGET'] = callback
    callbacks['MGET'] = mget_callback


patch_callbacks(Redis.RESPONSE_CALLBACKS)


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
        return self._redis.execute_command('MGET', *keys, convert=self._parser(cls))

    def mset(self, mapping: Dict[str, M]) -> bool:
        mapping = {k: v.json(exclude_defaults=True) for k, v in mapping.items()}
        return self._redis.mset(mapping)

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

    mget_nonatomic = mget
    mset_nonatomic = mset


class ParserCluster(Parser):
    def update_callbacks(self):
        for node in self._redis.get_nodes():
            if redis := node.redis_connection:
                patch_callbacks(redis.response_callbacks)

    def mget_nonatomic(self, keys, cls: Type[M]) -> List[M]:
        assert type(self._redis) is RedisCluster
        with self._redis.pipeline(transaction=False) as pipe:
            parser = ParserCluster(pipe)
            for key in keys:
                parser.get(key, cls)
            return pipe.execute()

    def mset_nonatomic(self, mapping: Dict[str, M]) -> bool:
        assert type(self._redis) is RedisCluster
        with self._redis.pipeline(transaction=False) as pipe:
            parser = ParserCluster(pipe)
            for k, v in mapping.items():
                parser.set(k, v)
            pipe.execute()
        return True


def create_parser(redis):
    return ParserCluster(redis) if isinstance(redis, RedisCluster) else Parser(redis)
