# -*- coding: utf-8 -*-
from types import MappingProxyType
from werkzeug.exceptions import TooManyRequests
from common.shared import *
from flasgger import Swagger
from flask import Flask, g
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
    ttl = min(redis.httl(key, *tokens)) if tokens else None
    return MappingProxyType(tokens), ttl


sessions: TtlCache[MappingProxyType[str, Session]] = TtlCache(get=_get_tokens, make_key=int)
sessions.listen(invalidator, 'session')


def user_limiter(cooldown):
    users = {}

    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            now = time.time()
            if users.get(g.uid, 0) > now:
                raise TooManyRequests
            while users:
                uid, expire = next(iter(users.items()))
                if expire > now:
                    break
                users.pop(uid)
            try:
                users[g.uid] = float('inf')  # not reentrant
                return f(*args, **kwargs)
            finally:
                users[g.uid] = now + cooldown

        return wrapper

    return decorator
