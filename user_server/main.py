# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from user_server.config import options
from user_server import shared, const, rpc, handlers
from user_server.shared import app_name, app_id
from setproctitle import setproctitle
import logging
import gevent


def main():
    logging.warning(f'app id: {app_id}')
    g = rpc.serve()
    shared.registry.start({const.RPC_USER: f'{options.host}:{options.rpc_port}'})
    setproctitle(f'{app_name}-{app_id}-{options.host}:{options.rpc_port}')
    shared.receiver.start()
    handlers.init()
    gevent.joinall([g], raise_error=True)


if __name__ == '__main__':
    main()
