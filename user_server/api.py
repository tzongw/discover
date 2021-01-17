# -*- coding: utf-8 -*-
from flask import Flask
from webargs import fields
from webargs.flaskparser import use_args
from .dao import User
from gevent import pywsgi
from .config import options
import gevent
import logging

app = Flask(__name__)


def serve():
    server = pywsgi.WSGIServer(('', options.http_port), app)
    g = gevent.spawn(server.serve_forever)
    gevent.sleep(0.1)
    if not options.http_port:
        options.http_port = server.address[1]
    logging.info(f'Starting http server {options.http_address} ...')
    return g


@app.route('/register', methods=['POST'])
@use_args({'username': fields.Str(required=True), 'password': fields.Str(required=True)})
def register(args):
    return User.create(args['username'], args['password'])
