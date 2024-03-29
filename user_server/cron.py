# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options, remaining
import time
from importlib import import_module
import shared
from shared import app_name, app_id, init_main
from setproctitle import setproctitle
import logging
from base import LogSuppress


def main():
    logging.info(f'{app_name} app id: {app_id}')
    shared.registry.start()
    shared.invalidator.start()
    init_main()
    if remaining:
        entry = remaining[0]
        if entry == '+':  # heavy_task
            for value in remaining[1:]:
                task = shared.heavy_task.parse(value)
                setproctitle(f'{app_name}-{app_id}-{task.path}')
                shared.heavy_task.exec(task)
        else:
            setproctitle(f'{app_name}-{app_id}-{entry}.main')
            module = import_module(entry)
            options.parse_command_line(remaining, final=False)
            start = time.time()
            logging.info(f'doing task {entry}')
            module.main()
            logging.info(f'done task {entry} {time.time() - start}')
    elif task := shared.heavy_task.pop(block=False):
        setproctitle(f'{app_name}-{app_id}-{task.path}')
        shared.heavy_task.exec(task)


if __name__ == '__main__':
    with LogSuppress():
        main()
