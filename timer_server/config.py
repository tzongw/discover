# -*- coding: utf-8 -*-
from common.config import define, options
from base import utils
import const


def rpc_port_callback(port: int):
    if port:
        options.define('rpc_address')
        options.rpc_address = f'{options.host}:{port}'


define('host', utils.ip_address(), str, 'public host')
define('rpc_port', 0, int, 'rpc port', callback=rpc_port_callback)

options.app_name = const.APP_TIMER
options.parse_command_line()
