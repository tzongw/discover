# -*- coding: utf-8 -*-
from common.shared import *
from types import MappingProxyType
from functools import partial, wraps
from werkzeug.exceptions import TooManyRequests
from werkzeug.debug import DebuggedApplication
from flasgger import Swagger
from flask import Flask, g
from base import ZTimer
from base import TtlCache
from base import ListConverter
from base.misc import JSONProvider, make_response
from base.sharding import ShardingKey, ShardingZTimer
import const
from models import Session

app = Flask(__name__)
app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter
app.json = JSONProvider(app)
app.json.ensure_ascii = False
app.make_response = partial(make_response, app)
if options.env is const.Environment.DEV:
    app.debug = True
    app.wsgi_app = DebuggedApplication(app.wsgi_app, evalex=True, pin_security=False)
swagger = Swagger(app)

ztimer = ShardingZTimer(redis, app_name, sharding_key=ShardingKey(shards=3)) if isinstance(redis, RedisCluster) \
    else ZTimer(redis, app_name)


def online_key(uid: int):
    return f'online:{uid}'


def session_key(uid: int):
    return f'session:{uid}'


def _get_tokens(uid: int):
    key = session_key(uid)
    tokens = parser.hgetall(key, Session)
    ttl = min(redis.httl(key, *tokens)) if tokens else None
    return MappingProxyType(tokens), ttl


sessions: TtlCache[dict[str, Session]] = TtlCache(get=_get_tokens, make_key=int)
sessions.listen(invalidator, 'session')


def user_limiter(cooldown):
    users = {}

    def decorator(f):
        @wraps(f)
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


def dispatch_timeout(full_key, data):
    if full_key == const.TICK_TIMER:
        now = int(time.time())
        increment = script.limited_incrby('timestamp:tick', amount=now, limit=now)
        offset = min(increment, 10)
        for ts in range(now - offset + 1, now + 1):
            time_dispatcher.dispatch_tick(ts)
    else:
        group, key = full_key.split(':', maxsplit=1)
        time_dispatcher.dispatch(group, key, data)
