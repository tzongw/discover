# -*- coding: utf-8 -*-
import sys
from common.config import define, options
import const

define('conf_d', '/usr/local/etc/nginx/servers' if sys.platform == 'darwin' else '/etc/nginx/conf.d', str,
       'nginx conf dir')

options.app_name = const.APP_RELOAD
options.parse_command_line()
