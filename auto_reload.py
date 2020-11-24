# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import gevent
from tornado.options import define, options
from tornado.options import parse_command_line
import os
from common import const, shared

define("conf_d", "/etc/nginx/conf.d", str, "nginx conf dir")

parse_command_line()

upstreams = [const.WS_GATE]
addr_map = {}


def update_upstreams():
    updated = False
    for name in upstreams:
        addrs = shared.registry.addresses(name)
        if addrs != addr_map.get(name):
            addr_map[name] = addrs
            with open(os.path.join(options.conf_d, name + '_upstream'), 'w', encoding='utf-8') as f:
                f.write('\n'.join([f'server {l};' for l in addrs]))
                f.write('\n')
            updated = True
    if updated:
        os.system("nginx -t && nginx -s reload")


def main():
    shared.registry.add_callback(update_upstreams)
    shared.registry.start({})
    while True:
        gevent.sleep(3600)


if __name__ == '__main__':
    main()
