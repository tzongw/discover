# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import time
from importlib import import_module
from config import options, remaining
import shared
from shared import app_name, app_id, init_main
from setproctitle import setproctitle
import logging


def main():
    logging.info(f'app id: {app_id}')
    shared.registry.start()
    shared.invalidator.start()
    init_main()
    if entry := options.entry:
        setproctitle(f'{app_name}-{app_id}-{entry}')
        module = import_module(entry)
        options.parse_command_line(remaining, final=False)
        start = time.time()
        getattr(module, 'main')()
        logging.info(f'cron task {entry} {time.time() - start}')
    elif task := shared.heavy_task.pop(block=False):
        setproctitle(f'{app_name}-{app_id}-{task.path}')
        shared.heavy_task.exec(task)


if __name__ == '__main__':
    main()
