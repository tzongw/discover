# -*- coding: utf-8 -*-
# noinspection PyUnresolvedReferences
import common.config
from tornado.options import define, options

define("conf_d", "/etc/nginx/conf.d", str, "nginx conf dir")

options.parse_command_line()
