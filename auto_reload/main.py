# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import os
import logging
import gevent
from setproctitle import setproctitle
from common import shared
from base import ip_address, Addr

addr_map = {}
changed = set()


def is_api(name: str):
    return name.startswith('ws') or name.startswith('http')


def is_valid(addr: str):
    if addr.startswith('unix://'):
        return os.path.exists(addr[len('unix://'):])
    else:
        return not options.same_host or Addr(addr).host == ip_address()


def reload_nginx():
    for name in changed:
        addrs = sorted(addr_map[name])
        logging.info(f'updating: {name} {addrs}')
        with open(os.path.join(options.conf_d, name + '_upstream'), 'w', encoding='utf-8') as f:
            f.write('\n'.join([f'server {addr};' for addr in addrs]))
            f.write('\n')
    changed.clear()
    os.system('nginx -s reload')
    logging.info('reload done')


def update_upstreams():
    empty = not changed
    # noinspection PyProtectedMember
    for name, addrs in shared.registry._addresses.items():
        if not is_api(name):
            continue
        addrs = {addr for addr in addrs if is_valid(addr)}
        exists = addr_map.get(name)
        if addrs and addrs != exists:
            logging.info(f'{name} changed: {exists} -> {addrs}')
            addr_map[name] = addrs
            changed.add(name)
    if empty and changed:
        logging.info('spawn reload')
        gevent.spawn_later(1.0, reload_nginx)


def main():
    logging.info(f'{shared.app_name} app id: {shared.app_id}')
    setproctitle(f'{shared.app_name}-{shared.app_id}')
    shared.registry.add_callback(update_upstreams)
    workers = shared.registry.start()
    shared.init_main()
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logging.exception('')
        exit(1)
