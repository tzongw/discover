# -*- coding: utf-8 -*-
from common.config import *
from base import utils
import const


def rpc_port_callback(port: int):
    if port:
        options.define('rpc_address')
        options.rpc_address = f'{options.host}:{port}'


def http_port_callback(port: int):
    if port:
        options.define('http_address')
        options.http_address = f'{options.host}:{port}'


define('host', utils.ip_address(), str, 'public host')
define('rpc_port', 0, int, 'rpc port', callback=rpc_port_callback)
define('http_port', 0, int, 'http port', callback=http_port_callback)
define('back_port', 0, int, 'back port')
define('init_timer', 'none', str, 'init timer')
define('slow_log', 0.1, float, 'slow log threshold')

options.app_name = const.APP_USER
remaining = options.parse_command_line()
