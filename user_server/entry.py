# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options, remaining
import time
import logging
from importlib import import_module
import shared
from shared import app_name, app_id, init_main
from setproctitle import setproctitle
from base import LogSuppress


def main():
    logging.info(f'{app_name} app id: {app_id}')
    shared.registry.start()
    shared.invalidator.start()
    init_main()
    if not remaining:
        raise RuntimeError('usage: python user_server/entry.py [--option ...] <module> [--option ...]')
    entry = remaining[0]
    setproctitle(f'{app_name}-{app_id}-{entry}')
    module = import_module(entry)
    options.parse_command_line(remaining, final=False)
    start = time.time()
    logging.info(f'doing task {entry}')
    module.main()
    logging.info(f'done task {entry} {time.time() - start}')


if __name__ == '__main__':
    with LogSuppress():
        main()
