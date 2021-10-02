# -*- coding: utf-8 -*-
import uuid
import flask
from flask import jsonify, g, request
from webargs import fields
from webargs.flaskparser import use_kwargs
from dao import Account, Session
from shared import app, parser
import hash_pb2
from gevent import pywsgi
from config import options
from const import CONTEXT_UID, CONTEXT_TOKEN
import gevent
import logging
from base import snowflake
from shared import app_id, session_key
from werkzeug.exceptions import UnprocessableEntity
from base.utils import ListConverter
from flasgger import Swagger
from hashlib import sha1

app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter
swagger = Swagger(app)
no_auth_methods = set()

user_id = snowflake.IdGenerator(options.datacenter, app_id)


def no_auth(f):
    no_auth_methods.add(f)
    return f


def serve():
    server = pywsgi.WSGIServer(('', options.http_port), app, log=logging.getLogger(), error_log=logging.getLogger())
    g = gevent.spawn(server.serve_forever)
    gevent.sleep(0.1)
    if not options.http_port:
        options.http_port = server.address[1]
    logging.info(f'Starting http server {options.http_address} ...')
    return g


@app.before_request
def before_request():
    rule = request.url_rule
    if rule and app.view_functions[rule.endpoint] not in no_auth_methods:
        g.uid = flask.session[CONTEXT_UID]


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
    return f'{g.uid} say hello {names}'


@app.errorhandler(UnprocessableEntity)
def args_error(e: UnprocessableEntity):
    return e.data['messages']


def hash_password(uid: int, password: str) -> str:
    # uid as salt
    return sha1(f'{uid}{password}'.encode()).hexdigest()


@app.route('/register', methods=['POST'])
@no_auth
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
    uid = user_id.gen()
    hashed = hash_password(uid, password)
    account = Account(id=uid, username=username, hashed=hashed)
    session.add(account)
    session.commit()
    return jsonify(account)


@app.route('/login', methods=['POST'])
@no_auth
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
