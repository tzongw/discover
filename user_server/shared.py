# -*- coding: utf-8 -*-
from common.shared import *
import json
from flasgger import Swagger
from flask.app import DefaultJSONProvider, Flask
from base import ListConverter
from base.misc import JSONEncoder, make_response
from base.cache import TTLCache
from models import Session


class JSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs) -> str:
        return json.dumps(obj, cls=JSONEncoder, **kwargs)


app = Flask(__name__)
app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter
app.json = JSONProvider(app)
Flask.make_response = make_response
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
