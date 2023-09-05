# -*- coding: utf-8 -*-
import functools

from common.shared import *
from flask import Flask
from base.cache import TTLCache
from models import Session
from gevent.local import local

app = Flask(__name__)
ctx = local()


def ctx_cache(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        key = func_desc(f)
        if not hasattr(ctx, key):
            @functools.lru_cache()
            def inner(*args, **kwargs):
                return f(*args, **kwargs)

            setattr(ctx, key, inner)
        cache_f = getattr(ctx, key)
        return cache_f(*args, **kwargs)

    return wrapper


@ctx_cache
def get_user_coupons(user_id):
    print(user_id)


def online_key(uid: int):
    return f'online:{uid}'


def session_key(uid: int):
    return f'session:{uid}'


def session(uid: int):
    key = session_key(uid)
    with redis.pipeline(transaction=False) as pipe:
        parser = SmartParser(pipe)
        parser.get(key, Session)
        pipe.ttl(key)
        return pipe.execute()


sessions: TTLCache[Session] = TTLCache(get=session)
sessions.listen(invalidator, 'session')
