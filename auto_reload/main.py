# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import gevent
import os
from common import shared
import logging

addr_map = {}


def is_api(name: str):
    return name.startswith('ws') or name.startswith('http')


def update_upstreams():
    updated = False
    # noinspection PyProtectedMember
    for name, addrs in shared.registry._addresses.items():
        if is_api(name) and addrs != addr_map.get(name):
            addr_map[name] = addrs
            with open(os.path.join(options.conf_d, name + '_upstream'), 'w', encoding='utf-8') as f:
                f.write('\n'.join([f'server {l};' for l in addrs]))
                f.write('\n')
            updated = True
            logging.info(f'update {name}')
    if updated:
        os.system("nginx -s reload")


def main():
    shared.registry.add_callback(update_upstreams)
    shared.registry.start()
    while True:
        gevent.sleep(3600)


if __name__ == '__main__':
    main()
