# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()
import common
import const
from tornado import options
import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from generated.service import gate


host = "localhost"
port = 40001


class Handler:
  def set_context(self, conn_id, context):
    pass

  def unset_context(self, conn_id, context):
    pass

  def remove_conn(self, conn_id):
    pass


def main():
    options.parse_command_line()
    common.service.register(const.SERVICE_GATE, f'{host}:{port}')
    common.service.start()

    handler = Handler()
    processor = gate.Processor(handler)
    transport = TSocket.TServerSocket(host, port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
    logging.info('Starting the server...')
    server.serve()


if __name__ == '__main__':
    main()
