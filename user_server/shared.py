# -*- coding: utf-8 -*-
from common.shared import *
from dataclasses import dataclass
from types import MappingProxyType
from functools import partial, wraps
from werkzeug.exceptions import TooManyRequests
from werkzeug.debug import DebuggedApplication
from flasgger import Swagger
from flask import Flask, g
from base import TtlCache
from base import ListConverter
from base.misc import JSONProvider, make_response, SwitchTracer
import const
from models import Session

app = Flask(__name__)
app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter
app.json = JSONProvider(app)
app.json.ensure_ascii = False
app.make_response = partial(make_response, app)
if options.env == const.Environment.DEV:
    app.debug = True
    app.wsgi_app = DebuggedApplication(app.wsgi_app, evalex=True, pin_security=False)
swagger = Swagger(app)
switch_tracer = SwitchTracer()


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


@dataclass
class Limiter:
    expire: float
    count: int


def user_limiter(*, cooldown, threshold=1):
    doing = set()
    limiters = {}  # type: dict[int, Limiter]

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            uid = g.uid
            if uid in doing:  # not reentrant
                raise TooManyRequests
            now = time.time()
            while limiters:  # expire sorted
                uid, limiter = next(iter(limiters.items()))
                if limiter.expire > now:
                    break
                limiters.pop(uid)
            limiter = limiters.get(uid)
            if not limiter:
                limiters[uid] = Limiter(expire=now + cooldown, count=1)
            elif limiter.count < threshold:
                limiter.count += 1
            else:
                raise TooManyRequests
            doing.add(uid)
            try:
                return f(*args, **kwargs)
            finally:
                doing.discard(uid)

        return wrapper

    return decorator


def dispatch_timeout(full_key, data):
    if full_key != const.TICK_TIMER:
        group, key = full_key.split(':', maxsplit=1)
        time_dispatcher.dispatch(group, key, data)
    elif options.tick_timer:
        now = int(time.time())
        increment = script.limited_incrby('timestamp:tick', amount=now, limit=now)
        offset = min(increment, 10)
        for ts in range(now - offset + 1, now + 1):
            time_dispatcher.dispatch_tick(ts)


if options.tick_timer:
    @receiver(const.TICK_STREAM)
    def on_tick(_, sid):
        ts = int(sid[:-2])
        time_dispatcher.dispatch_tick(ts)
