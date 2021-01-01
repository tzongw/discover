# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from gate_server.config import options
from gate_server import const, shared, rpc, ws
from gate_server.shared import app_name, app_id
from setproctitle import setproctitle
import gevent
import logging


def main():
    logging.warning(f'app id: {app_id}')
    workers = [ws.serve(), rpc.serve()]
    setproctitle(f'{app_name}-{app_id}-{options.ws_address}-{options.rpc_address}')
    shared.registry.start({const.WS_GATE: options.ws_address, const.RPC_GATE: options.rpc_address})
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    main()
