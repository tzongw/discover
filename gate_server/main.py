# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import logging
import gevent
from setproctitle import setproctitle
import shared
import rpc
import ws
from shared import app_name, app_id, rpc_service, http_service


def main():
    logging.info(f'{app_name} app id: {app_id}')
    workers = [ws.serve(), rpc.serve()]
    setproctitle(f'{app_name}-{app_id}-{options.http_port}-{options.rpc_port}')
    workers += shared.registry.start()
    shared.init_main()
    shared.registry.register({http_service: options.http_address, rpc_service: options.rpc_address})
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logging.exception('')
        exit(1)
