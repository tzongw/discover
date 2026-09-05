# -*- coding: utf-8 -*-
from common.shared import *
from dataclasses import dataclass
from types import MappingProxyType
from functools import partial, wraps
from werkzeug.exceptions import TooManyRequests
from werkzeug.debug import DebuggedApplication
from flasgger import Swagger
from flask import Flask, g
from base import Cache, create_invalidator
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
if options.env in [const.Environment.DEV, const.Environment.TEST]:
    swagger = Swagger(app)
    app.debug = True
    app.wsgi_app = DebuggedApplication(app.wsgi_app, evalex=True, pin_security=options.env == const.Environment.TEST)
switch_tracer = SwitchTracer()
online_users_key = 'users:online'


def online_key(uid: int):
    return f'online:{uid}'


def session_key(uid: int):
    return f'session:{uid}'


def _get_user_sessions(uid: int):
    key = session_key(uid)
    user_sessions = caching_parser.hgetall(key, Session)
    return MappingProxyType(user_sessions)


caching_redis = create_redis(options.redis)
caching_parser = create_parser(caching_redis)
invalidator = create_invalidator(caching_redis)
sessions: Cache[dict[str, Session]] = Cache(get=_get_user_sessions, make_key=int)
sessions.listen(invalidator, 'session', bcast=False)


@dataclass
class Limiter:
    expire: float
    count: int


def user_limiter(*, cooldown, threshold=1):
    assert threshold > 0
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
                doing.remove(uid)

        return wrapper

    return decorator


def dispatch_timeout(full_key, data):
    if full_key != const.TICK_TIMER:
        group, key = full_key.split(':', maxsplit=1)
        dispatcher.dispatch(group, key, data)
    elif options.tick_timer:
        cur_ts = int(time.time())
        sync_ts = cur_ts - const.TICK_OFFSET
        value, incr = redis.increx('timestamp:tick', lbound=sync_ts, ubound=cur_ts)
        if incr:
            dispatcher.dispatch_tick(value)
            return
        if value >= cur_ts:  # upper bound
            return
        # lower bound
        if redis.set('timestamp:tick', cur_ts, ifeq=value):
            logging.info(f'tick: {value + 1} -> {cur_ts}')
            dispatcher.dispatch_tick(cur_ts)


if options.tick_timer:
    @consumer(const.TICK_STREAM)
    def on_tick(_, sid):
        ts = int(sid[:-2])
        dispatcher.dispatch_tick(ts)


def delay_policy(op):
    for delay in [1, 3, 10]:
        scheduler.call_later(op, delay)
