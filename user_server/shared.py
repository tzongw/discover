# -*- coding: utf-8 -*-
from common.shared import *
from base.mq import Receiver
from base.snowflake import max_worker_id
from flask import Flask
from common.task import AsyncTask
from base import snowflake
from base.cache import TTLCache
from hash_pb2 import Session
from gevent.local import local

app_name = const.APP_USER
app_id = unique_id.gen(app_name, range(max_worker_id))
id_generator = snowflake.IdGenerator(options.datacenter, app_id)

app = Flask(__name__)
ctx = local()

receiver = Receiver(redis, app_name, str(app_id))
at_exit(receiver.stop)

async_task = AsyncTask(timer, receiver)


def online_key(uid: int):
    return f'online:{uid}'


def session_key(uid: int):
    return f'session:{uid}'


def session(uid: int):
    key = session_key(uid)
    with redis.pipeline() as pipe:
        parser = Parser(pipe)
        parser.hget(key, Session())
        pipe.ttl(key)
        return pipe.execute()


session_cache = TTLCache[Session](get=session)
session_cache.listen(invalidator, 'session')
