# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import const
from tornado.options import options, define, parse_command_line
import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from generated.service import user
import common
import json
from typing import Dict

define("host", "127.0.0.1", str, "listen host")
define("port", 50001, int, "listen port")


class Handler:
    def login(self, address: str, conn_id: str, params: Dict[str, str]):
        logging.info(f'{address} {conn_id} {params}')
        try:
            uid = int(params.pop(const.PARAM_UID))
            token = params.pop(const.PARAM_TOKEN)
            if token != "pass":
                raise ValueError("token")
        except Exception as e:
            if isinstance(e, (KeyError, ValueError)):
                logging.warning(f'{address} {conn_id} {params}')
            else:
                logging.error(f'{address} {conn_id} {params} {e}')
            common.service_pools.send_text(conn_id, f'login fail {e}')
            common.service_pools.remove_conn(conn_id)
        else:
            common.service_pools.set_context(conn_id, json.dumps({const.PARAM_UID: uid}))
            common.service_pools.send_text(conn_id, f'login success')

    def ping(self, address: str, conn_id: str, context: str):
        logging.debug(f'{address} {conn_id}, {context}')

    def disconnect(self, address: str, conn_id: str, context: str):
        logging.info(f'{address} {conn_id}, {context}')


def main():
    parse_command_line()
    common.service.register(const.SERVICE_USER, f'{options.host}:{options.port}')
    common.service.start()

    handler = Handler()
    processor = user.Processor(handler)
    transport = TSocket.TServerSocket(options.host, options.port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    logging.info(f'Starting the server {options.host}:{options.port} ...')
    server.serve()


if __name__ == '__main__':
    main()
