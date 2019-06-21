# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import common
import const
from tornado.options import options, define, parse_command_line
import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from generated.service import user

define("host", "127.0.0.1", str, "listen host")
define("port", 50002, int, "listen port")


class Handler:
    def login(self, address, conn_id, params):
        logging.info(f'{address} {conn_id}, {params}')

    def ping(self, address, conn_id, context):
        logging.info(f'{address} {conn_id}, {context}')

    def disconnect(self, address, conn_id, context):
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
