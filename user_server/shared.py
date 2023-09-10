# -*- coding: utf-8 -*-
from common.shared import *
from flask import Flask
from base.cache import TTLCache
from models import Session

app = Flask(__name__)


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
