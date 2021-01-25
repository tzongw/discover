# -*- coding: utf-8 -*-
import uuid
import flask
from flask import jsonify
from webargs import fields
from webargs.flaskparser import use_args
from .dao import Account, Session
from .shared import app, parser
from . import hash_pb2
from gevent import pywsgi
from .config import options
from .const import CONTEXT_UID, CONTEXT_TOKEN
import gevent
import logging
from base import snowflake
from .shared import app_id, session_key
from werkzeug.exceptions import UnprocessableEntity
from base.utils import ListConverter
from flasgger import Swagger

app.secret_key = b'\xc8\x04\x12\xc7zJ\x9cO\x99\xb7\xb3eb\xd6\xa4\x87'
app.url_map.converters['list'] = ListConverter
swagger = Swagger(app)

user_id = snowflake.IdGenerator(options.datacenter, app_id)


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
    return f'{flask.session[CONTEXT_UID]} say hello {names}'


@app.errorhandler(UnprocessableEntity)
def args_error(e: UnprocessableEntity):
    return e.data['messages']


@app.route('/register', methods=['POST'])
@use_args({'username': fields.Str(required=True), 'password': fields.Str(required=True)}, location='json_or_form')
def register(args):
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
    account = Account(id=user_id.gen(), username=args['username'], password=args['password'])
    session.add(account)
    session.commit()
    return jsonify(account)


@app.route('/login', methods=['POST'])
@use_args({'username': fields.Str(required=True), 'password': fields.Str(required=True)}, location='json_or_form')
def login(args):
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
    account = session.query(Account).filter(Account.username == args['username']).first()  # type: Account
    if account is None or account.password != args['password']:
        return 'account not exist or password error'
    else:
        flask.session[CONTEXT_UID] = account.id
        token = str(uuid.uuid4())
        flask.session[CONTEXT_TOKEN] = token
        flask.session.permanent = True
        key = session_key(account.id)
        parser.hset(key, hash_pb2.Session(token=token), expire=app.permanent_session_lifetime)
        return jsonify(account)
