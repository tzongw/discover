# -*- coding: utf-8 -*-
from common.shared import *
from base.snowflake import max_worker_id

app_name = const.APP_GATE
app_id = unique_id.generate(app_name, range(max_worker_id))
