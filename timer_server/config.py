# -*- coding: utf-8 -*-
# noinspection PyUnresolvedReferences
import common.config
from tornado.options import define, options
from base import utils


def rpc_port_callback(port: int):
    if port:
        options.define('rpc_address')
        options.rpc_address = f'{options.host}:{port}'


define('host', utils.ip_address(), str, 'public host')
define('rpc_port', 0, int, 'rpc port', callback=rpc_port_callback)

options.parse_command_line()
