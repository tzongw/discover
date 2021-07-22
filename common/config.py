# -*- coding: utf-8 -*-
import logging
from tornado.log import LogFormatter
from tornado.options import define, parse_config_file

LOG_FORMAT = "%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s"
channel = logging.StreamHandler()
channel.setFormatter(LogFormatter(fmt=LOG_FORMAT, datefmt=None))
logger = logging.getLogger()
logger.addHandler(channel)


class Addr:
    def __init__(self, value: str):
        host, port = value.rsplit(':', maxsplit=1)
        self.host = host
        self.port = int(port)

    def __str__(self):
        return f'{self.host}:{self.port}'


define("config", type=str, help="path to config file",
       callback=lambda path: parse_config_file(path, final=False))
define("redis", "redis://", str, "redis url")
define("datacenter", 0, int, "data center id")
