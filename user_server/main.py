# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import logging
import gevent
from setproctitle import setproctitle
import shared
import const
import rpc
# noinspection PyUnresolvedReferences
import handlers
import api
from shared import app_name, app_id, init_main


def main():
    logging.info(f'{app_name} app id: {app_id}')
    workers = [rpc.serve(), api.serve()]
    if options.env == const.Environment.DEV and options.back_port:
        from gevent.backdoor import BackdoorServer
        logging.info(f'Starting backdoor: {options.back_port}')
        server = BackdoorServer(('127.0.0.1', options.back_port), locals={'shared': shared})
        workers.append(gevent.spawn(server.serve_forever))
    setproctitle(f'{app_name}-{app_id}-{options.http_port}-{options.rpc_port}')
    workers += shared.registry.start()
    workers += shared.invalidator.start()
    init_main()
    workers += shared.receiver.start()
    shared.registry.register({const.HTTP_USER: options.http_address, const.RPC_USER: options.rpc_address})
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logging.exception('')
        exit(1)
