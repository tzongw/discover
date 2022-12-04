# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
# noinspection PyUnresolvedReferences
from config import options
import shared
from shared import app_name, app_id, init_main
from setproctitle import setproctitle
import logging


def main():
    logging.info(f'app id: {app_id}')
    shared.registry.start()
    shared.invalidator.start()
    init_main()
    if task := shared.heavy_task.pop(block=False):
        setproctitle(f'{app_name}-{app_id}-{task.path}')
        shared.heavy_task.exec(task)


if __name__ == '__main__':
    main()
