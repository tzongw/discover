# -*- coding: utf-8 -*-
from types import MappingProxyType
from common.shared import *
import functools
from flasgger import Swagger
from flask import Flask
from base import ListConverter
from base.misc import JSONProvider, make_response
from base.cache import TtlCache
import const
from models import Session

app = Flask(__name__)
app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter
app.json = JSONProvider(app)
app.json.ensure_ascii = False
app.debug = options.env is const.Environment.DEV
app.make_response = functools.partial(make_response, app)
swagger = Swagger(app)


def online_key(uid: int):
    return f'online:{uid}'


def session_key(uid: int):
    return f'session:{uid}'


def _get_tokens(uid: int):
    key = session_key(uid)
    tokens = parser.hgetall(key, Session)
    ttls = redis.httl(key, *tokens)
    return MappingProxyType(tokens), min(ttls)


sessions: TtlCache[MappingProxyType[str, Session]] = TtlCache(get=_get_tokens, make_key=int)
sessions.listen(invalidator, 'session')
