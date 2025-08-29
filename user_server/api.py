# -*- coding: utf-8 -*-
import json
import logging
import time
import uuid
from datetime import timedelta, datetime
from copy import copy

import flask
import gevent
from flask import Blueprint, g, request, stream_with_context, current_app
from gevent import pywsgi
from marshmallow.validate import Range
from redis.commands.core import HashDataPersistOptions
from sqlalchemy import Column
from sqlalchemy.exc import OperationalError
from webargs import fields
from webargs.flaskparser import use_kwargs
from werkzeug.exceptions import UnprocessableEntity, Unauthorized, Forbidden, Conflict

import models
from base import singleflight
from base.poller import PollStatus
from base.utils import base62, salt_hash, LogSuppress
from base.misc import DoesNotExist, CacheMixin, build_order_by, build_condition, convert_type, build_operation, \
    SqlCacheMixin, JSONEncoder
from config import options, ctx
from const import CTX_UID, CTX_TOKEN, MAX_SESSIONS, Environment
from dao import Account, Session, collections, tables, Config, config_models, Change, RowChange
from shared import app, dispatcher, id_generator, sessions, redis, poller, spawn_worker, invalidator, user_limiter
from shared import session_key, async_task, heavy_task, script, scheduler, exclusion
import push

cursor_filed = fields.Int(default=0, validate=Range(min=0, max=1000))
cursor_filed.num_type = lambda v: int(v or 0)


def serve():
    logger = None if options.env == Environment.PROD else logging.getLogger()
    server = pywsgi.WSGIServer(options.http_port, app, log=logger, error_log=logging.getLogger())
    g = gevent.spawn(server.serve_forever)
    if not options.http_port:
        while not server.address[1]:
            gevent.sleep(0.01)
        options.http_port = server.address[1]
    logging.info(f'Starting http server {options.http_address} ...')
    return g


@app.before_request
def init_trace():
    ctx.trace = base62.encode(id_generator.gen())


@async_task
@heavy_task(priority=heavy_task.Priority.HIGH)
@exclusion('lock:{message}', timedelta(seconds=30))
def log(message):
    for i in range(10):
        logging.info(f'{message} {i}')
        gevent.sleep(1)


@poller('hello', spawn=spawn_worker)
def poll(queue):
    gevent.sleep(0.5)
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
        if redis.rpush('queue', *names) == len(names):  # head of the queue
            poller.notify('hello', 'queue')
    else:
        async_task.post(f'task:{uuid.uuid4()}', log(names[0]), timedelta(seconds=3))
    return f'say hello {names}'


@app.route('/echo/<message>')
def echo(message):
    gevent.sleep(0.1)
    tick = redis.get('tick')
    if request.headers.get('If-None-Match') == f'W/"{tick}"':
        return '', 304
    tick = redis.incr('tick')
    logging.info(f'tick {tick}')
    response = current_app.make_response(f'say hello {message} {tick}')
    response.headers['ETag'] = tick
    return response


@invalidator.getter('future')
def getter(full_key):
    return redis.get(full_key)


@app.route('/future/<key>')
@singleflight
def get_future(key):
    full_key = invalidator.full_key('future', key)
    placeholder = f'PLACEHOLDER-{uuid.uuid4()}'
    value = redis.set(full_key, placeholder, nx=True, ex=10, get=True)
    if value is None:  # first request
        gevent.sleep(5)
        value = options.http_address
        script.compare_set(full_key, placeholder, value, expire=timedelta(seconds=20))
        return value
    if value.startswith('PLACEHOLDER-'):  # wait for others
        fut = invalidator.future('future', key)
        return fut.result(10)
    return value  # cached


@app.route('/stream')
def streaming_response():
    def generate():
        for line in ['Hell\no', 'World', '!']:
            yield f'{json.dumps(line)}'
            gevent.sleep(1)

    return app.response_class(stream_with_context(generate()))


@app.route('/tables/<table>/rows')
@use_kwargs({'cursor': cursor_filed,
             'count': fields.Int(validate=Range(min=1, max=100)),
             'order_by': fields.DelimitedList(fields.Str())},
            location='query', unknown='include')
def get_rows(table: str, cursor=0, count=20, order_by=None, **kwargs):
    tb = tables[table]
    with Session() as session:
        order_by = build_order_by(tb, order_by)
        cond = build_condition(tb, kwargs)
        query = session.query(tb).filter(cond).order_by(*order_by).offset(cursor).limit(count)
        rows = [row.to_dict(exclude=[]) for row in query]
        if cursor:
            total = -1
        elif len(rows) < count:
            total = len(rows)
        else:
            total = len(session.query(tb.id).filter(cond).limit(1000).all())
    return {
        'total': total,
        'rows': rows,
        'cursor': '' if len(rows) < count or total == count else str(cursor + count),
    }


@app.route('/tables/<table>/rows', methods=['POST'])
@use_kwargs({}, location='json_or_form', unknown='include')
def create_row(table: str, **kwargs):
    tb = tables[table]
    convert_type(tb, kwargs)
    with Session() as session:
        row = tb(**kwargs)
        session.add(row)
    if isinstance(row, SqlCacheMixin):
        row.invalidate(invalidator)  # notify full cache new row created
    return row.to_dict(exclude=[])


@app.route('/tables/<table>/rows/<row_id>')
def get_row(table: str, row_id):
    tb = tables[table]
    row = tb.get(row_id, ensure=True)
    return row.to_dict(exclude=[])


@app.route('/tables/<table>/rows/<row_id>', methods=['PATCH'])
@use_kwargs({}, location='json_or_form', unknown='include')
def update_row(table: str, row_id, **kwargs):
    tb = tables[table]
    kwargs = build_operation(tb, kwargs)
    for key in kwargs:
        if key == tb.id.name or key in tb.__readonly__:
            raise Forbidden(key)
    convert_type(tb, kwargs)
    with Session() as session:
        session.query(tb).filter(tb.id == row_id).update(kwargs)
    row = tb.get(row_id, ensure=True)
    if isinstance(tb, SqlCacheMixin):
        row.invalidate(invalidator)
    return row.to_dict(exclude=[])


@app.route('/tables/<table>/rows/<row_id>', methods=['DELETE'])
def delete_row(table: str, row_id):
    tb = tables[table]
    row = tb.get(row_id, ensure=True)
    with Session() as session:
        session.query(tb).filter(tb.id == row_id).delete()
    if isinstance(row, SqlCacheMixin):
        row.invalidate(invalidator)
    return row.to_dict(exclude=[])


@app.route('/tables/<table>/rows/<row_id>/columns/<column>', methods=['PATCH'])
@use_kwargs({}, location='json_or_form', unknown='include')
def move_rows(table: str, row_id, column, **kwargs):
    tb = tables[table]
    if column == tb.id.name or column in tb.__readonly__:
        raise Forbidden(column)
    col = getattr(tb, column)  # type: Column
    row = tb.get(row_id, ensure=True)
    rank = getattr(row, column)
    kwargs[f'{column}__gte'] = rank
    kwargs[f'{tb.id.name}__ne'] = row_id
    row_ids = []
    with Session() as session:
        cond = build_condition(tb, kwargs)
        for row in session.query(tb).filter(cond).order_by(col.asc()):
            if getattr(row, column) != rank:
                break
            row_ids.append(row.id)
            rank += 1
        if row_ids:
            session.query(tb).filter(tb.id.in_(row_ids)).update({col: col + 1})
            if issubclass(tb, SqlCacheMixin):
                tb.bulk_invalidate(invalidator, row_ids)


@app.route('/tables/configs/rows/<int:row_id>')
def get_config(row_id):
    return Config.get(row_id)


@app.route('/tables/configs/rows/<int:row_id>/snapshots/<int:change_id>')
def get_config_snapshot(row_id, change_id):
    snapshot = RowChange.snapshot(row_id, change_id)
    return snapshot['value']


@app.route('/tables/configs/rows/<int:row_id>', methods=['PATCH'])
@use_kwargs({}, location='json_or_form', unknown='include')
def update_config(row_id, **kwargs):
    model = config_models[row_id]
    now = datetime.now()
    with Session.transaction() as session:
        config = session.query(Config).filter(Config.id == row_id).first()
        if config:
            origin = copy(config)
            obj = config.value | kwargs
            value = model.parse_obj(obj)
            config.value = json.loads(value.json())
            config.update_time = now
        else:
            origin = None
            value = model(**kwargs)
            config = Config(id=row_id, value=json.loads(value.json()), update_time=now)
            session.add(config)
        diff = json.loads(json.dumps(config.diff(origin), cls=JSONEncoder))
        change = RowChange(table_name=Config.__tablename__, row_id=config.id, diff=diff)
        session.add(change)
    config.invalidate(invalidator)
    return value


@app.route('/collections/<collection>/documents')
@use_kwargs({'cursor': cursor_filed,
             'count': fields.Int(validate=Range(min=1, max=100)),
             'order_by': fields.DelimitedList(fields.Str())},
            location='query', unknown='include')
def get_documents(collection: str, cursor=0, count=20, order_by=None, **kwargs):
    coll = collections[collection]
    order_by = order_by or [f'-{coll.id.name}']
    for key, value in kwargs.items():
        if key.endswith('__in'):
            kwargs[key] = value.split(',')
    query = coll.objects(**kwargs).order_by(*order_by).skip(cursor).limit(count)
    docs = [doc.to_dict(exclude=[]) for doc in query]
    if cursor:
        total = -1
    elif len(docs) < count:
        total = len(docs)
    else:
        total = len(coll.objects(**kwargs).only(coll.id.name).as_pymongo().limit(1000))
    return {
        'total': total,
        'documents': docs,
        'cursor': '' if len(docs) < count or total == count else str(cursor + count),
    }


@app.route('/collections/<collection>/documents', methods=['POST'])
@use_kwargs({}, location='json_or_form', unknown='include')
def create_document(collection: str, **kwargs):
    coll = collections[collection]
    doc_id = kwargs.get(coll.id.name)
    if doc_id is not None and coll.get(doc_id):  # save will update doc unexpectedly if doc_id already exists
        raise Conflict(f'document `{doc_id}` already exists')
    doc = coll(**kwargs).save()
    Change(coll_name=coll.__name__, doc_id=doc.id, diff=doc.diff()).save()
    if isinstance(doc, CacheMixin):
        doc.invalidate(invalidator)  # notify full cache new document created
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>')
def get_document(collection: str, doc_id):
    coll = collections[collection]
    doc = coll.get(doc_id, ensure=True)
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>/snapshots/<int:change_id>')
def get_snapshot(collection: str, doc_id, change_id):
    coll = collections[collection]
    doc_id = coll.id.to_python(doc_id)
    snapshot = Change.snapshot(doc_id, change_id)
    snapshot[coll.id.name] = doc_id
    doc = coll(**snapshot)
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>', methods=['PATCH'])
@use_kwargs({}, location='json_or_form', unknown='include')
def update_document(collection: str, doc_id, **kwargs):
    coll = collections[collection]
    for key in kwargs:
        if key == coll.id.name or key in coll.__readonly__:
            raise Forbidden(key)
    doc = coll.get(doc_id, ensure=True)
    origin = coll.from_json(doc.to_json())  # clone
    if not doc.modify(**kwargs):  # not exists, when doc is default
        kwargs[coll.id.name] = doc_id
        doc = coll(**kwargs).save()
        origin = None
    Change(coll_name=coll.__name__, doc_id=doc.id, diff=doc.diff(origin)).save()
    if isinstance(doc, CacheMixin):
        doc.invalidate(invalidator)
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>', methods=['DELETE'])
def delete_document(collection: str, doc_id):
    coll = collections[collection]
    doc = coll.get(doc_id, ensure=True)
    doc.delete()
    if isinstance(doc, CacheMixin):
        doc.invalidate(invalidator)
    return doc.to_dict(exclude=[])


@app.route('/collections/<collection>/documents/<doc_id>/fields/<field>', methods=['PATCH'])
@use_kwargs({}, location='json_or_form', unknown='include')
def move_documents(collection: str, doc_id, field: str, **kwargs):
    coll = collections[collection]
    if field == coll.id.name or field in coll.__readonly__:
        raise Forbidden(field)
    doc = coll.get(doc_id, ensure=True)
    rank = doc[field]
    kwargs[f'{field}__gte'] = rank
    kwargs[f'{coll.id.name}__ne'] = doc_id
    docs = []
    for doc in coll.objects(**kwargs).only(field).order_by(field):
        if doc[field] != rank:
            break
        docs.append(doc)
        rank += 1
    if docs:
        doc_ids = [doc.id for doc in docs]
        coll.objects(**{f'{coll.id.name}__in': doc_ids}).update(**{f'inc__{field}': 1})
        changes = [Change(coll_name=coll.__name__, doc_id=doc.id, diff={field: doc[field] + 1}) for doc in docs]
        Change.objects.insert(changes)
        if issubclass(coll, CacheMixin):
            coll.bulk_invalidate(invalidator, doc_ids)


@app.route('/eval', methods=['POST'])
def eval_code():
    if request.remote_addr not in ['127.0.0.1', '::1', '::ffff:127.0.0.1']:
        raise Forbidden(request.remote_addr)
    r = ['OK']
    exec(request.data)
    return r[0]


@app.errorhandler(UnprocessableEntity)
def unprocessable_entity_error(e: UnprocessableEntity):
    return e.data['messages'], e.code


@app.errorhandler(DoesNotExist)
def does_not_exist_error(e: DoesNotExist):
    return e.args[0], 400


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
    with Session.transaction() as session:
        account = session.query(Account).filter(Account.username == username).first()  # type: Account
        if account is None:  # register
            account = Account(username=username)
            account.hashed = salt_hash(password, salt=account.id)
            session.add(account)
            dispatcher.signal(account)
        elif account.hashed != salt_hash(password, salt=account.id):
            return 'account not exist or password error'
    flask.session[CTX_UID] = account.id
    token = str(uuid.uuid4())
    flask.session[CTX_TOKEN] = token
    flask.session.permanent = True
    key = session_key(account.id)
    with redis.pipeline(transaction=True) as pipe:
        pipe.hkeys(key)
        session_id = salt_hash(token, salt=account.id)
        pipe.hsetex(key, session_id, models.Session(create_time=datetime.now()), ex=app.permanent_session_lifetime,
                    data_persist_option=HashDataPersistOptions.FNX)
        session_ids, added = pipe.execute()
        assert added, 'session id conflicts'
    if len(session_ids) >= MAX_SESSIONS:
        ttls = {sid: ttl for sid, ttl in zip(session_ids, redis.httl(key, *session_ids))}
        session_ids.sort(key=lambda x: ttls[x])
        to_delete = session_ids[:len(session_ids) - MAX_SESSIONS + 1]
        redis.hdel(key, *to_delete)
        for sid in to_delete:  # normally, len(to_delete) == 1
            push.kick(account.id, 'token expired', session_id=sid)
    return account


bp = Blueprint('/', __name__)
user_actives = {}


@scheduler(timedelta(seconds=1))
def reap_user_active():
    past = time.time() - timedelta(minutes=10).total_seconds()
    while user_actives:
        uid, active = next(iter(user_actives.items()))
        if active > past:
            break
        user_actives.pop(uid)


@bp.before_request
def authorize():
    uid, token = flask.session.get(CTX_UID), flask.session.get(CTX_TOKEN)
    if not uid or not token:
        raise Unauthorized
    session_id = salt_hash(token, salt=uid)
    if session_id not in sessions.get(uid):
        raise Unauthorized
    g.uid, g.session_id = uid, session_id
    if uid in user_actives:
        return
    # refresh last active & token ttl
    logging.info(f'user active: {uid}')
    now = datetime.now()
    user_actives[uid] = now.timestamp()
    with LogSuppress(OperationalError), Session() as session:  # ignore db locked error, when creating indexes
        session.query(Account).filter(Account.id == uid).update({Account.last_active: now})
    key = session_key(uid)
    ttl = app.permanent_session_lifetime.total_seconds()
    if redis.httl(key, session_id)[0] < 0.9 * ttl:
        redis.hexpire(key, int(ttl), session_id)  # will invalidate local cache


@bp.route('/whoami')
@user_limiter(cooldown=10, threshold=2)
def whoami():
    """whoami
    ---
    tags:
      - account
    responses:
      200:
        description: account
    """
    logging.info(f'uid: {g.uid}')
    gevent.sleep(0.01)
    account = Account.get(g.uid)
    return account


app.register_blueprint(bp)
