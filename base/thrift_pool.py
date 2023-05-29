from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.Thrift import TException
from .utils import Addr
from .pool import Pool


class ThriftPool(Pool):
    def __init__(self, addr: Addr, **settings):
        super().__init__(**settings, biz_exception=self.biz_exception)
        self.addr = addr

    def create_connection(self):
        transport = TSocket.TSocket(self.addr.host, self.addr.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        return protocol

    def close_connection(self, conn: TBinaryProtocol.TBinaryProtocol):
        conn.trans.close()

    @staticmethod
    def biz_exception(e: Exception):
        return isinstance(e, TException) and not isinstance(e, TTransport.TTransportException)
