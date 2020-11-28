# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import gevent
import logging
from gate_server.config import options
from gate_server import const, shared, rpc, ws
from gate_server.shared import app_name, app_id
from setproctitle import setproctitle


def main():
    logging.warning(f'app id: {app_id}')
    workers = [ws.serve(), rpc.serve()]
    rpc_address = f'{options.host}:{options.rpc_port}'
    ws_address = f'{options.host}:{options.ws_port}'
    setproctitle(f'{app_name}-{app_id}-{ws_address}-{rpc_address}')
    shared.registry.start({const.WS_GATE: ws_address, const.RPC_GATE: rpc_address})
    gevent.joinall(workers, raise_error=True)


if __name__ == '__main__':
    main()
