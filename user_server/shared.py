# -*- coding: utf-8 -*-
from common.shared import *
import functools
from flasgger import Swagger
from flask import Flask
from base import ListConverter
from base.misc import JSONProvider, make_response
from base.cache import TTLCache
from models import Session

app = Flask(__name__)
app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter
app.json = JSONProvider(app)
app.make_response = functools.partial(make_response, app)
swagger = Swagger(app)


def online_key(uid: int):
    return f'online:{uid}'


def session_key(uid: int):
    return f'session:{uid}'


def get_session(uid: int):
    key = session_key(uid)
    with redis.pipeline(transaction=False) as pipe:
        create_parser(pipe).get(key, Session)
        pipe.ttl(key)
        return pipe.execute()


sessions: TTLCache[Session] = TTLCache(get=get_session)
sessions.listen(invalidator, 'session')
