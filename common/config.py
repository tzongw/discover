# -*- coding: utf-8 -*-
import logging
from tornado.log import LogFormatter
from tornado.options import define, parse_config_file
from tornado.options import options
from concurrent_log_handler import ConcurrentRotatingFileHandler
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
    channel = ConcurrentRotatingFileHandler(filename=options.log_file, maxBytes=options.log_file_max_size,
                                            backupCount=options.log_file_num_backups,
                                            encoding='utf-8') if options.log_file else logging.StreamHandler()
    channel.setFormatter(CtxLogFormatter(fmt=LOG_FORMAT, datefmt='', color=not options.log_file))
    logger = logging.getLogger()
    logger.addHandler(channel)


options.log_to_stderr = False
options.add_parse_callback(parse_callback)

define('config', type=str, help='path to config file',
       callback=lambda path: parse_config_file(path, final=False))
define('app_name', 'app', str, 'app name')
define('env', Environment.DEV, Environment, 'environment')
define('redis', 'redis://', str, 'biz redis url')
define('redis_cluster', '', str, 'biz redis cluster url')
define('datacenter', 0, int, 'data center id')
define('log_file', type=str, help='log file path')
define('mongo', type=str, help='mongo url')
