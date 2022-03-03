# -*- coding: utf-8 -*-
import uuid
import flask
from flask import jsonify, Blueprint, g
from webargs import fields
from webargs.flaskparser import use_kwargs
from dao import Account, Session
from shared import app, parser, dispatcher, id_generator
import hash_pb2
from gevent import pywsgi
from config import options
from const import CONTEXT_UID, CONTEXT_TOKEN
import gevent
import logging
from shared import session_key
from werkzeug.exceptions import UnprocessableEntity, Unauthorized
from base.utils import ListConverter
from flasgger import Swagger
from hashlib import sha1

app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter


def serve():
    server = pywsgi.WSGIServer(('', options.http_port), app, log=logging.getLogger(), error_log=logging.getLogger())
    g = gevent.spawn(server.serve_forever)
    gevent.sleep(0.1)
    if not options.http_port:
        options.http_port = server.address[1]
    logging.info(f'Starting http server {options.http_address} ...')
    return g


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
    return f'say hello {names}'


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
        flask.session[CONTEXT_UID] = account.id
        token = str(uuid.uuid4())
        flask.session[CONTEXT_TOKEN] = token
        flask.session.permanent = True
        key = session_key(account.id)
        parser.hset(key, hash_pb2.Session(token=token), expire=app.permanent_session_lifetime)
        return jsonify(account)


bp = Blueprint('/', __name__)


@bp.before_request
def before_request():
    uid, token = flask.session.get(CONTEXT_UID), flask.session.get(CONTEXT_TOKEN)
    if not uid or not token or token != parser.hget(session_key(uid), hash_pb2.Session()).token:
        raise Unauthorized()
    g.uid = uid


@bp.route('/whoami')
def whoami():
    """whoami
    ---
    tags:
      - account
    responses:
      200:
        description: account
    """
    account = Account(id=g.uid)
    return jsonify(account)


app.register_blueprint(bp)
swagger = Swagger(app)
