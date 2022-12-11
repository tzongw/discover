from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.Thrift import TException
from .pool import Pool


class ThriftPool(Pool):
    def __init__(self, host, port, **settings):
        super().__init__(**settings, biz_exception=self.biz_exception)
        self._host = host
        self._port = port

    def create_connection(self):
        transport = TSocket.TSocket(self._host, self._port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        return protocol

    def close_connection(self, conn: TBinaryProtocol.TBinaryProtocol):
        conn.trans.close()

    @staticmethod
    def biz_exception(e: Exception):
        return isinstance(e, TException) and not isinstance(e, TTransport.TTransportException)
