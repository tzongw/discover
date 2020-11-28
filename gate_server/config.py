# -*- coding: utf-8 -*-
import common.shared
from tornado.options import define, options
from base import utils

define("host", utils.ip_address(), str, "public host")
define("ws_port", 0, int, "ws port")
define("rpc_port", 0, int, "rpc port")

options.parse_command_line()