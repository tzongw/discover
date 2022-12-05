# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import gevent
import os
from common import shared
import logging
from base import LogSuppress

addr_map = {}
changed = set()


def is_api(name: str):
    return name.startswith('ws') or name.startswith('http')


def reload_nginx():
    for name in changed:
        addrs = sorted(addr_map[name])
        logging.info(f'updating: {name} {addrs}')
        with open(os.path.join(options.conf_d, name + '_upstream'), 'w', encoding='utf-8') as f:
            f.write('\n'.join([f'server {l};' for l in addrs]))
            f.write('\n')
    changed.clear()
    os.system("nginx -s reload")
    logging.info('reload done')


def update_upstreams():
    empty = not changed
    # noinspection PyProtectedMember
    for name, addrs in shared.registry._addresses.items():
        if is_api(name) and addrs != addr_map.get(name):
            logging.info(f'changed: {name} {addr_map.get(name)} -> {addrs}')
            addr_map[name] = addrs
            changed.add(name)
    if empty and changed:
        logging.info('spawn reload')
        gevent.spawn_later(0.2, reload_nginx)


def main():
    shared.registry.add_callback(update_upstreams)
    shared.registry.start()
    shared.init_main()
    while True:
        gevent.sleep(3600)


if __name__ == '__main__':
    with LogSuppress(Exception):
        main()
