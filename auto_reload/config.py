# -*- coding: utf-8 -*-
import sys
# noinspection PyUnresolvedReferences
import common.config
from tornado.options import define, options

define("conf_d", "/usr/local/etc/nginx/servers" if sys.platform == "darwin" else "/etc/nginx/conf.d", str,
       "nginx conf dir")

options.parse_command_line()
