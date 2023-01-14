# -*- coding: utf-8 -*-
import functools
import time
import uuid
from collections import OrderedDict
import logging
import flask
from flask import jsonify, Blueprint, g, request
from webargs import fields
from webargs.flaskparser import use_kwargs
from dao import Account, Session
from shared import app, parser, dispatcher, id_generator, session_cache, ctx
import gevent
from gevent import pywsgi
from config import options
from const import CTX_UID, CTX_TOKEN
from shared import session_key, heavy_task
from werkzeug.exceptions import UnprocessableEntity, Unauthorized, TooManyRequests, Forbidden
from base.utils import ListConverter
from flasgger import Swagger
from hashlib import sha1
import models

app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter


def serve():
    server = pywsgi.WSGIServer(options.http_port, app, log=logging.getLogger(), error_log=logging.getLogger())
    g = gevent.spawn(server.serve_forever)
    gevent.sleep(0.1)
    if not options.http_port:
        options.http_port = server.address[1]
    logging.info(f'Starting http server {options.http_address} ...')
    return g


@app.before_request
def init_trace():
    ctx.trace = id_generator.gen()


@heavy_task
def log(message):
    for i in range(10):
        logging.info(f'{message} {i}')
        gevent.sleep(1)


@app.route('/hello/<list:names>')
def hello(names):
    """say hello
    ---
    tags:
      - hello
    parameters:
      - name: names
        in: path
        type: string
        required: true
    responses:
      200:
        description: hello
    """
    heavy_task.push(log('processing'))
    return f'say hello {names}'


@app.route('/eval', methods=['POST'])
def eval_code():
    if request.remote_addr not in ['127.0.0.1', '::1', '::ffff:127.0.0.1']:
        raise Forbidden(request.remote_addr)
    r = ['OK']
    exec(request.data)
    return r[0]


@app.errorhandler(UnprocessableEntity)
def args_error(e: UnprocessableEntity):
    return e.data['messages']


def hash_password(uid: int, password: str) -> str:
    # uid as salt
    return sha1(f'{uid}{password}'.encode()).hexdigest()


@app.route('/register', methods=['POST'])
@use_kwargs({'username': fields.Str(required=True), 'password': fields.Str(required=True)}, location='json_or_form')
def register(username: str, password: str):
    """register
    ---
    tags:
      - account
    parameters:
      - name: username
        in: formData
        type: string
        required: true
      - name: password
        in: formData
        type: string
        required: true
    responses:
      200:
        description: account
    """
    session = Session()
    uid = id_generator.gen()
    hashed = hash_password(uid, password)
    account = Account(id=uid, username=username, hashed=hashed)
    session.add(account)
    session.commit()
    dispatcher.signal(account)
    return jsonify(account)


@app.route('/login', methods=['POST'])
@use_kwargs({'username': fields.Str(required=True), 'password': fields.Str(required=True)}, location='json_or_form')
def login(username: str, password: str):
    """login
    ---
    tags:
      - account
    parameters:
      - name: username
        in: formData
        type: string
        required: true
      - name: password
        in: formData
        type: string
        required: true
    responses:
      200:
        description: session
    """
    session = Session()
    account = session.query(Account).filter(Account.username == username).first()  # type: Account
    if account is None or account.hashed != hash_password(account.id, password):
        return 'account not exist or password error'
    else:
        flask.session[CTX_UID] = account.id
        token = str(uuid.uuid4())
        flask.session[CTX_TOKEN] = token
        flask.session.permanent = True
        key = session_key(account.id)
        parser.set(key, models.Session(token=token), ex=app.permanent_session_lifetime)
        return jsonify(account)


bp = Blueprint('/', __name__)


@bp.before_request
def authorize():
    uid, token = flask.session.get(CTX_UID), flask.session.get(CTX_TOKEN)
    if not uid or not token or token != session_cache.get(uid).token:
        raise Unauthorized
    ctx.uid = g.uid = uid


def user_limiter(cooldown):
    def decorator(f):
        current = OrderedDict()

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            now = time.time()
            if current.get(g.uid, 0) > now:
                raise TooManyRequests
            count = 10
            while current and count > 0:
                uid, expire = next(iter(current.items()))
                if expire > now:
                    break
                current.pop(uid)
                count -= 1
            try:
                current[g.uid] = float('inf')  # not reentrant
                return f(*args, **kwargs)
            finally:
                current[g.uid] = now + cooldown

        return wrapper

    return decorator


@bp.route('/whoami')
@user_limiter(cooldown=5)
def whoami():
    """whoami
    ---
    tags:
      - account
    responses:
      200:
        description: account
    """
    logging.info(f'{ctx.__dict__}')
    account = Account(id=g.uid)
    return jsonify(account)


app.register_blueprint(bp)
swagger = Swagger(app)
