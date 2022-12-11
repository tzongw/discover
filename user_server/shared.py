# -*- coding: utf-8 -*-
from common.shared import *
from flask import Flask
from base.cache import TTLCache
from hash_pb2 import Session
from gevent.local import local

app = Flask(__name__)
ctx = local()


def online_key(uid: int):
    return f'online:{uid}'


def session_key(uid: int):
    return f'session:{uid}'


def session(uid: int):
    key = session_key(uid)
    with redis.pipeline() as pipe:
        parser = Parser(pipe)
        parser.hget(key, Session())
        pipe.ttl(key)
        return pipe.execute()


session_cache = TTLCache[Session](get=session)
session_cache.listen(invalidator, 'session')
