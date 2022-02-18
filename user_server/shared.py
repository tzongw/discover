# -*- coding: utf-8 -*-
from common.shared import *
from base.mq import Receiver
from base.snowflake import max_worker_id
from flask import Flask
from common.task import AsyncTask

app_name = const.APP_USER
app_id = unique_id.gen(app_name, range(max_worker_id))

app = Flask(__name__)

receiver = Receiver(redis, app_name, str(app_id))
at_exit(receiver.stop)

async_task = AsyncTask(timer, receiver)


def online_key(uid: int):
    return f'online:{uid}'


def session_key(uid: int):
    return f'session:{uid}'
