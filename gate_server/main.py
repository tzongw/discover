# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from config import options
import const
import shared
import rpc
import ws
from shared import app_name, app_id
from setproctitle import setproctitle
import gevent
import logging
from base import LogSuppress


def main():
    logging.info(f'{app_name} app id: {app_id}')
    workers = [ws.serve(), rpc.serve()]
    setproctitle(f'{app_name}-{app_id}-{options.ws_port}-{options.rpc_port}')
    shared.registry.start()
    shared.init_main()
    shared.registry.register({const.WS_GATE: options.ws_address, const.RPC_GATE: options.rpc_address})
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    with LogSuppress(Exception):
        main()
