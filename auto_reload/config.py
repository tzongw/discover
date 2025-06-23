# -*- coding: utf-8 -*-
from common.config import *
import os
import sys
import const

define('pid_file', '/opt/homebrew/var/run/nginx.pid' if sys.platform == 'darwin' else '/var/run/nginx.pid', str,
       'nginx pid file')
define('conf_d', '/opt/homebrew/etc/nginx/servers' if sys.platform == 'darwin' else '/etc/nginx/conf.d', str,
       'nginx conf dir')
define('same_host', True, bool, 'only same host addr')

options.app_name = const.APP_RELOAD
options.parse_command_line()

assert os.path.exists(options.pid_file) and os.path.exists(options.conf_d)
