# -*- coding: utf-8 -*-
import functools
import json
import logging
import time
import uuid
from collections import OrderedDict
from datetime import timedelta
from hashlib import sha1

import flask
import gevent
from flask import Blueprint, g, request, stream_with_context, current_app
from gevent import pywsgi
from marshmallow.validate import Range
from mongoengine import NotUniqueError
from webargs import fields
from webargs.flaskparser import use_kwargs
from werkzeug.exceptions import UnprocessableEntity, Unauthorized, TooManyRequests, Forbidden

import models
from base.poller import PollStatus
from base.utils import base62
from config import options, ctx
from const import CTX_UID, CTX_TOKEN
from dao import Account, Session, collections
from shared import app, parser, dispatcher, id_generator, sessions, redis, poller, spawn_worker, invalidator
from shared import session_key, async_task, run_in_process

cursor_filed = fields.Int(default=0, validate=Range(min=0, max=1000))
cursor_filed.num_type = lambda v: int(v or 0)


def serve():
    server = pywsgi.WSGIServer(options.http_port, app, log=logging.getLogger(), error_log=logging.getLogger())
    g = gevent.spawn(server.serve_forever)
    if not options.http_port:
        gevent.sleep(0.01)
        options.http_port = server.address[1]
    logging.info(f'Starting http server {options.http_address} ...')
    return g


@app.before_request
def init_trace():
    ctx.trace = base62(id_generator.gen())


@async_task
@run_in_process
def log(message):
    for i in range(10):
        logging.info(f'{message} {i}')
        gevent.sleep(1)


@poller.handler('hello', spawn=spawn_worker)
def poll(queue):
    name = redis.lpop(queue)
    if name is None:
        return PollStatus.DONE
    logging.info(f'{queue} got {name}')
    return PollStatus.YIELD if name == 'tang' else PollStatus.ASAP


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
    if len(names) > 1:
        if redis.rpush('queue:hello', *names) == len(names):  # head of the queue
            poller.notify('hello', 'queue:hello')
    else:
        async_task.post(f'task:{uuid.uuid4()}', log(names[0]), timedelta(seconds=5))
    return f'say hello {names}'


@app.route('/echo/<message>')
def echo(message):
    gevent.sleep(0.1)
    tick = redis.get('tick')
    if request.headers.get('If-None-Match') == f'W/"{tick}"':
        return '', 304
    tick = redis.incr('tick')
    logging.warning(f'tick {tick}')
    response = current_app.make_response(f'say hello {message} {tick}')
    response.headers['Cache-Control'] = 'max-age=10'
    response.headers['ETag'] = f'W/"{tick}"'
    return response


@app.route('/stream')
def streaming_response():
    def generate():
        for line in ['Hell\no', 'World', '!']:
            yield f'{json.dumps(line)}'
            gevent.sleep(1)

    return app.response_class(stream_with_context(generate()))


@app.route('/collections/<collection>/documents')
@use_kwargs({'cursor': cursor_filed,
             'count': fields.Int(validate=Range(min=1, max=50)),
             'order_by': fields.DelimitedList(fields.Str())},
            location='query', unknown='include')
def get_documents(collection: str, cursor=0, count=10, order_by=None, **kwargs):
    coll = collections[collection]
    order_by = order_by or [f'-{coll.id.name}']
    for key, value in kwargs.items():
        if key.endswith('__in'):
            kwargs[key] = value.split(',')
    docs = [doc.to_dict(exclude=[]) for doc in coll.objects(**kwargs).order_by(*order_by).skip(cursor).limit(count)]
    return {
        'documents': docs,
        'cursor': '' if len(docs) < count else str(cursor + count),
    }


@app.route('/collections/<collection>/documents', methods=['POST'])
@use_kwargs({}, location='json_or_form', unknown='include')
def create_document(collection: str, **kwargs):
    coll = collections[collection]
    key = kwargs.get(coll.id.name)
    if key is not None and coll.get(key):
        raise NotUniqueError(f'document `{key}` already exists')
    doc = coll(**kwargs).save()
    doc.invalidate(invalidator)  # notify full cache new document created
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>')
def get_document(collection: str, doc_id):
    coll = collections[collection]
    doc = coll.get(doc_id, ensure=True)
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>', methods=['PATCH'])
@use_kwargs({}, location='json_or_form', unknown='include')
def update_document(collection: str, doc_id, **kwargs):
    coll = collections[collection]
    doc = coll.get(doc_id, ensure=True)
    doc.modify(**kwargs)
    doc.invalidate(invalidator)
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>', methods=['DELETE'])
@use_kwargs({}, location='json_or_form')
def delete_documents(collection: str, doc_id):
    coll = collections[collection]
    doc = coll.get(doc_id, ensure=True)
    doc.delete()
    doc.invalidate(invalidator)
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>/fields/<field>', methods=['PATCH'])
@use_kwargs({}, location='json_or_form', unknown='include')
def move_documents(collection: str, doc_id, field: str, **kwargs):
    coll = collections[collection]
    target = coll.get(doc_id, ensure=True)
    value = target[field]
    kwargs[f'{field}__gte'] = value
    kwargs[f'{coll.id.name}__ne'] = doc_id
    docs = []
    for doc in coll.objects(**kwargs).only(field).order_by(field):
        if doc[field] != value:
            break
        docs.append(doc)
        value += 1
    if docs:
        doc_ids = [doc.id for doc in docs]
        coll.objects(**{f'{coll.id.name}__in': doc_ids}).update(**{f'inc__{field}': 1})
        for doc in docs:
            doc.invalidate(invalidator)


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
    return account


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
        return account


bp = Blueprint('/', __name__)


@bp.before_request
def authorize():
    uid, token = flask.session.get(CTX_UID), flask.session.get(CTX_TOKEN)
    if not uid or not token:
        raise Unauthorized
    session = sessions.get(uid)
    if not session or token != session.token:
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
    return account


app.register_blueprint(bp)
