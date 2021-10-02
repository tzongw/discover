# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import shared
import const
import rpc
import handlers
import api
from shared import app_name, app_id
from setproctitle import setproctitle
import logging
import gevent


def main():
    logging.warning(f'app id: {app_id}')
    workers = [rpc.serve(), api.serve()]
    setproctitle(f'{app_name}-{app_id}-{options.http_address}-{options.rpc_address}')
    shared.registry.start({const.HTTP_USER: options.http_address, const.RPC_USER: options.rpc_address})
    shared.receiver.start()
    shared.invalidator.start()
    handlers.init()
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    main()
