# -*- coding: utf-8 -*-
import logging
import gevent
from service import gate
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport
from base import utils
from .ws import clients, Client, remove_from_group, groups
from .config import options


class Handler:
    def set_context(self, conn_id, context):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {context}')
            client.set_context(context)
        else:
            logging.warning(f'not found {conn_id} {context}')

    def unset_context(self, conn_id, context):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {context}')
            client.unset_context(context)
        else:
            logging.warning(f'not found {conn_id} {context}')

    def remove_conn(self, conn_id):
        client = clients.get(conn_id)
        if client:
            logging.info(f'{client}')
            client.stop()
        else:
            logging.warning(f'not found {conn_id}')

    def _send_message(self, conn_id, message):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {message}')
            client.send(message)
        else:
            logging.warning(f'not found {conn_id} {message}')

    send_text = _send_message
    send_binary = _send_message

    def join_group(self, conn_id, group):
        client = clients.get(conn_id)
        if client:
            logging.info(f'{client} {group}')
            client.groups.add(group)
            groups[group].add(client)
        else:
            logging.warning(f'not found {conn_id} {group}')

    def leave_group(self, conn_id, group):
        client = clients.get(conn_id)
        if client:
            logging.info(f'{client} {group}')
            client.groups.discard(group)
            remove_from_group(client, group)
        else:
            logging.warning(f'not found {conn_id} {group}')

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
    transport = TSocket.TServerSocket(utils.wildcard, options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    g = gevent.spawn(server.serve)
    gevent.sleep(0.1)
    options.rpc_port = transport.handle.getsockname()[1]
    rpc_address = f'{options.host}:{options.rpc_port}'
    logging.info(f'Starting rpc server {rpc_address} ...')
    return g
