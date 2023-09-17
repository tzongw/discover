# -*- coding: utf-8 -*-
import sys
from common.config import *
import const

define('conf_d', '/opt/homebrew/etc/nginx/servers' if sys.platform == 'darwin' else '/etc/nginx/conf.d', str,
       'nginx conf dir')
define('same_host', True, bool, 'only same host addr')

options.app_name = const.APP_RELOAD
options.parse_command_line()
