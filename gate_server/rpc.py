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
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {key} {value}')
            client.set_context(key, value)

    def unset_context(self, conn_id, key, value):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {key} {value}')
            client.unset_context(key, value)

    def remove_conn(self, conn_id):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client}')
            client.stop()

    def _send_message(self, conn_id, message):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {message}')
            client.send(message)

    send_text = _send_message
    send_binary = _send_message

    def join_group(self, conn_id, group):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {group}')
            client.groups.add(group)
            groups[group].add(client)

    def leave_group(self, conn_id, group):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {group}')
            client.groups.discard(group)
            remove_from_group(client, group)

    def _broadcast_message(self, group, exclude, message):
        logging.debug(f'{group} {exclude} {message} {groups}')
        for client in groups.get(group) or set():  # type: Client
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
    gevent.sleep(0.1)
    if not options.rpc_port:
        options.rpc_port = transport.handle.getsockname()[1]
    logging.info(f'Starting rpc server {options.rpc_address} ...')
    return g
