# -*- coding: utf-8 -*-
from flask import Flask, jsonify
from webargs import fields
from webargs.flaskparser import use_args
from .dao import User, engine
from sqlalchemy.orm import Session
from gevent import pywsgi
from .config import options
import gevent
import logging
from base import snowflake
from .shared import app_id
from werkzeug.exceptions import UnprocessableEntity

app = Flask(__name__)
user_id = snowflake.IdGenerator(options.datacenter, app_id)


def serve():
    server = pywsgi.WSGIServer(('', options.http_port), app)
    g = gevent.spawn(server.serve_forever)
    gevent.sleep(0.1)
    if not options.http_port:
        options.http_port = server.address[1]
    logging.info(f'Starting http server {options.http_address} ...')
    return g


@app.route('/')
def hello():
    return 'hello'


@app.errorhandler(UnprocessableEntity)
def error_handler(e: UnprocessableEntity):
    return e.data['messages']


@app.route('/register', methods=['POST'])
@use_args({'username': fields.Str(required=True), 'password': fields.Str(required=True)}, location='form')
def register(args):
    session = Session(engine)
    user = User(id=user_id.gen(), username=args['username'], password=args['password'])
    session.add(user)
    session.commit()
    return jsonify(user)
