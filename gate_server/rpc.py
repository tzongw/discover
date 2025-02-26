# -*- coding: utf-8 -*-
import logging
import gevent
from service import gate
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport
from ws import clients, Client, remove_from_group, groups
from config import options


class Handler:
    def set_context(self, conn_id, key, value):
        if client := clients.get(conn_id):
            client.set_context(key, value)

    def unset_context(self, conn_id, key, value):
        if client := clients.get(conn_id):
            client.unset_context(key, value)

    def remove_conn(self, conn_id):
        if client := clients.get(conn_id):
            client.stop()

    def _send_message(self, conn_id, message):
        if client := clients.get(conn_id):
            client.send(message)

    send_text = _send_message
    send_binary = _send_message

    def join_group(self, conn_id, group):
        if client := clients.get(conn_id):
            client.groups.add(group)
            groups[group].add(client)

    def leave_group(self, conn_id, group):
        if client := clients.get(conn_id):
            client.groups.discard(group)
            remove_from_group(client, group)

    def _broadcast_message(self, group, exclude, message):
        members = groups.get(group, []) if group else clients.values()
        for client in members:  # type: Client
            if client.conn_id not in exclude:
                client.send(message)

    broadcast_binary = _broadcast_message
    broadcast_text = _broadcast_message


def serve():
    handler = Handler()
    processor = gate.Processor(handler)
    transport = TSocket.TServerSocket(port=options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    g = gevent.spawn(server.serve)
    if not options.rpc_port:
        gevent.sleep(0.01)
        options.rpc_port = transport.handle.getsockname()[1]
    logging.info(f'Starting rpc server {options.rpc_address} ...')
    return g
