# -*- coding: utf-8 -*-
import logging
from tornado.log import LogFormatter
from tornado.options import define, parse_config_file
# noinspection PyUnresolvedReferences
from tornado.options import options

LOG_FORMAT = "%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s"
channel = logging.StreamHandler()
channel.setFormatter(LogFormatter(fmt=LOG_FORMAT, datefmt=None))
logger = logging.getLogger()
logger.addHandler(channel)

define("config", type=str, help="path to config file",
       callback=lambda path: parse_config_file(path, final=False))
define("redis", "redis://", str, "redis url")
define("datacenter", 0, int, "data center id")
