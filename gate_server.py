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
from generated.service import gate

define("host", "127.0.0.1", str, "listen host")
define("port", 40001, int, "listen port")


class Handler:
    def set_context(self, conn_id, context):
        pass

    def unset_context(self, conn_id, context):
        pass

    def remove_conn(self, conn_id):
        pass


def main():
    parse_command_line()
    common.service.register(const.SERVICE_GATE, f'{options.host}:{options.port}')
    common.service.start()

    handler = Handler()
    processor = gate.Processor(handler)
    transport = TSocket.TServerSocket(options.host, options.port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    logging.info(f'Starting the server {options.host}:{options.port} ...')
    server.serve()


if __name__ == '__main__':
    main()
