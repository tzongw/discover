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


def unix_sock_callback(sock_path: str):
    if sock_path:
        options.define('http_address')
        options.http_address = 'unix://' + sock_path


define('host', utils.ip_address(), str, 'public host')
define('unix_sock', '', str, 'ws unix sock', callback=unix_sock_callback)
define('http_port', 0, int, 'http port', callback=http_port_callback)
define('rpc_port', 0, int, 'rpc port', callback=rpc_port_callback)

options.parse_command_line()

if not options.app_name:
    options.app_name = const.APP_GATE
