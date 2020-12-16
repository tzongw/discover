# -*- coding: utf-8 -*-
# noinspection PyUnresolvedReferences
import common.config
from tornado.options import define, options
from base import utils

define("host", utils.ip_address(), str, "public host")
define("rpc_port", 0, int, "rpc port")

options.parse_command_line()
