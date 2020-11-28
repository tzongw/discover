# -*- coding: utf-8 -*-
from tornado.options import define, parse_command_line, options
from base import utils

define("host", utils.ip_address(), str, "public host")
define("rpc_port", 0, int, "rpc port")

parse_command_line()
