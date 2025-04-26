# -*- coding: utf-8 -*-
import logging
from tornado.log import LogFormatter
from tornado.options import define, parse_config_file
from tornado.options import options
from concurrent_log_handler import ConcurrentRotatingFileHandler
from base import utils
from .const import Environment
from gevent.local import local

ctx = local()


class CtxLogFormatter(LogFormatter):
    def format(self, record) -> str:
        record.context = ctx.__dict__ or '\b'
        return super().format(record)


LOG_FORMAT = '%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(funcName)s:%(lineno)d %(process)d]%(end_color)s ' \
             '%(context)s %(message)s'


def parse_callback():
    if options.log_file:
        channel = ConcurrentRotatingFileHandler(filename=options.log_file, maxBytes=options.log_file_max_size,
                                                backupCount=options.log_file_num_backups, encoding='utf-8')
    else:
        channel = logging.StreamHandler()
    channel.setFormatter(CtxLogFormatter(fmt=LOG_FORMAT, datefmt='', color=not options.log_file))
    logger = logging.getLogger()
    logger.addHandler(channel)


def parse_app_name(app_name: str):
    options.define('http_service')
    options.http_service = f'http_{app_name}'
    options.define('rpc_service')
    options.rpc_service = f'rpc_{app_name}'


def host_callback(host: str):
    assert 'rpc_address' not in options and 'http_address' not in options, f'set host={host} BEFORE rpc_port & http_port'


def rpc_port_callback(port: int):
    if port:
        options.define('rpc_address')
        options.rpc_address = f'{options.host}:{port}'


def http_port_callback(port: int):
    if port:
        options.define('http_address')
        options.http_address = f'{options.host}:{port}'


options.log_to_stderr = False
options.add_parse_callback(parse_callback)

define('config', type=str, help='path to config file', callback=lambda path: parse_config_file(path, final=False))
define('app_name', None, str, 'app name', callback=parse_app_name)
define('env', Environment.DEV, Environment, 'environment')
define('redis', '', str, 'biz redis addr')
define('registry', None, str, 'registry redis addr, use biz redis if none')
define('datacenter', 0, int, 'data center id')
define('log_file', type=str, help='log file path')
define('mongo', type=str, help='mongo url')
define('host', utils.ip_address(), str, 'public host', callback=host_callback)
define('rpc_port', 0, int, 'rpc port', callback=rpc_port_callback)
define('http_port', 0, int, 'http port', callback=http_port_callback)
