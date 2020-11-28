# -*- coding: utf-8 -*-
from common.shared import *
from base.mq import Receiver

app_name = const.APP_USER
app_id = unique_id.generate(app_name, range(1024))

receiver = Receiver(redis, app_name, str(app_id))
at_exit(receiver.stop)
