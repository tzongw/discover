# -*- coding: utf-8 -*-
from common.shared import *
from base.mq import Receiver
from base.snowflake import max_worker_id
from flask import Flask

app_name = const.APP_USER
app_id = unique_id.gen(app_name, range(max_worker_id))

app = Flask(__name__)

receiver = Receiver(redis, app_name, str(app_id))
at_exit(receiver.stop)
