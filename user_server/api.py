# -*- coding: utf-8 -*-
from flask import Flask, jsonify
from webargs import fields
from webargs.flaskparser import use_args
from .dao import Account, Session
from gevent import pywsgi
from .config import options
import gevent
import logging
from base import snowflake
from .shared import app_id
from werkzeug.exceptions import UnprocessableEntity
from base.utils import ListConverter
from flasgger import Swagger

app = Flask(__name__)
app.url_map.converters['list'] = ListConverter
swagger = Swagger(app)

user_id = snowflake.IdGenerator(options.datacenter, app_id)


def serve():
    server = pywsgi.WSGIServer(('', options.http_port), app)
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
    return f'hello {names}'


@app.errorhandler(UnprocessableEntity)
def args_error(e: UnprocessableEntity):
    return e.data['messages']


@app.route('/register', methods=['POST'])
@use_args({'username': fields.Str(required=True), 'password': fields.Str(required=True)}, location='json_or_form')
def register(args):
    session = Session()
    account = Account(id=user_id.gen(), username=args['username'], password=args['password'])
    session.add(account)
    session.commit()
    return jsonify(account)


@app.route('/login', methods=['POST'])
@use_args({'username': fields.Str(required=True), 'password': fields.Str(required=True)}, location='json_or_form')
def login(args):
    session = Session()
    account = session.query(Account).filter(Account.username == args['username']).first()  # type: Account
    if account is None:
        return 'account not exist'
    elif account.password != args['password']:
        return 'password error'
    else:
        return jsonify(account)
