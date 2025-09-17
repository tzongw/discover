# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import os
import signal
import logging
from datetime import timedelta
import gevent
from setproctitle import setproctitle
from common import shared
from base import ip_address, Addr
from shared import scheduler

service_addresses = {}
changed = set()


def is_api(name: str):
    return name.startswith('http')


def is_valid(addr: str):
    if addr.startswith('unix://'):
        return os.path.exists(addr[len('unix://'):])
    else:
        return not options.same_host or Addr(addr).host == options.host


@scheduler(timedelta(milliseconds=100))
def reload_nginx():
    if not changed:
        return
    prefix = options.host + '_' if options.host != ip_address() else ''
    for service in changed:
        addresses = sorted(service_addresses[service])
        logging.info(f'updating: {service} {addresses}')
        path = os.path.join(options.conf_d, prefix + service + '_upstream')
        temp_path = path + '.temp'
        with open(temp_path, 'w') as f:
            f.write('\n'.join([f'server {addr};' for addr in addresses]))
            f.write('\n')
        os.rename(temp_path, path)  # atomic
    changed.clear()
    with open(options.pid_file, 'r') as f:
        pid = int(f.readline().strip())
    os.kill(pid, signal.SIGHUP)
    logging.info('nginx reload done')


def update_upstreams():
    # noinspection PyProtectedMember
    for service, addresses in shared.registry._addresses.items():
        if not is_api(service):
            continue
        addresses = {addr for addr in addresses if is_valid(addr)}
        current = service_addresses.get(service)
        if addresses and addresses != current:
            logging.info(f'{service} changed: {current} -> {addresses}')
            service_addresses[service] = addresses
            changed.add(service)


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
