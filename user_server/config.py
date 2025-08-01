# -*- coding: utf-8 -*-
from common.config import *
import const

define('back_port', 0, int, 'back port')
define('init_timer', 'none', str, 'init timer')
define('tick_timer', False, bool, 'tick timer')
define('slow_log', 0, float, 'slow log threshold, 0 to disable')
define('sqlite', 'db.sqlite3', str, 'sqlite db path')
define('mongo', type=str, help='mongo url')

options.app_name = const.APP_USER
remaining = options.parse_command_line()
