# -*- coding: utf-8 -*-
from common.config import *
import const


def unix_sock_callback(sock_path: str):
    if sock_path:
        options.define('http_address')
        options.http_address = 'unix://' + sock_path


define('unix_sock', '', str, 'ws unix sock', callback=unix_sock_callback)

options.app_name = const.APP_GATE
options.parse_command_line()
