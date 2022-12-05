# -*- coding: utf-8 -*-
import logging
from tornado.log import LogFormatter
from tornado.options import define, parse_config_file
# noinspection PyUnresolvedReferences
from tornado.options import options
from concurrent_log_handler import ConcurrentRotatingFileHandler
from .const import Environment

LOG_FORMAT = '%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(funcName)s:%(lineno)d %(process)d]%(end_color)s ' \
             '%(message)s'


def config_logging():
    channel = ConcurrentRotatingFileHandler(filename=options.log_file, maxBytes=options.log_file_max_size,
                                            backupCount=options.log_file_num_backups,
                                            encoding='utf-8') if options.log_file else logging.StreamHandler()
    channel.setFormatter(LogFormatter(fmt=LOG_FORMAT, datefmt='', color=not options.log_file))
    logger = logging.getLogger()
    logger.addHandler(channel)


options.log_to_stderr = False
options.add_parse_callback(config_logging)

define('config', type=str, help='path to config file',
       callback=lambda path: parse_config_file(path, final=False))
define('redis', 'redis://', str, 'redis url')
define('datacenter', 0, int, 'data center id')
define('env', Environment.DEV, Environment, 'environment')
define('log_file', type=str, help='log file path')
