# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import gevent
from tornado.options import define
from tornado.options import parse_command_line
import common

define("conf_d", "/etc/nginx/conf.d", str, "nginx conf dir")

parse_command_line()


def main():
    common.registry.start({})
    while True:
        gevent.sleep(3600)


if __name__ == '__main__':
    main()
