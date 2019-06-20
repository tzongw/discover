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
from flask import Flask
from flask_sockets import Sockets
from gevent import pywsgi, joinall, spawn
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket.websocket import WebSocket
import uuid
from typing import Dict
import json

define("host", "127.0.0.1", str, "listen host")
define("rpc_port", 40001, int, "rpc port")
define("ws_port", 40002, int, "ws port")

rpc_address = f'{options.host}:{options.rpc_port}'
ws_address = f'{options.host}:{options.ws_port}'

app = Flask(__name__)
sockets = Sockets(app)


def ws_serve():
    server = pywsgi.WSGIServer((options.host, options.ws_port), app, handler_class=WebSocketHandler)
    logging.info(f'Starting ws server {ws_address} ...')
    server.serve_forever()


class Client:
    def __init__(self, ws: WebSocket):
        self.context = {}
        self.ws = ws


clients = {}  # type: Dict[str, Client]


@sockets.route('/')
def client_serve(ws: WebSocket):
    h = ws.handler  # type: WebSocketHandler
    conn_id = str(uuid.uuid4())
    clients[conn_id] = Client(ws)
    logging.debug(f'{h.headers} f{conn_id}')
    common.service_pools.login(rpc_address, conn_id, h.headers)
    try:
        while not ws.closed:
            ws.receive()
    except Exception as e:
        logging.error(f'{conn_id} f{e}')
    finally:
        client = clients.pop(conn_id)
        context = json.dumps(client.context)
        common.service_pools.disconnect(rpc_address, conn_id, context)


class Handler:
    def set_context(self, conn_id, context):
        pass

    def unset_context(self, conn_id, context):
        pass

    def remove_conn(self, conn_id):
        pass


def rpc_serve():
    common.service.register(const.SERVICE_GATE, rpc_address)
    common.service.start()

    handler = Handler()
    processor = gate.Processor(handler)
    transport = TSocket.TServerSocket(options.host, options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    logging.info(f'Starting rpc server {rpc_address} ...')
    server.serve()


def main():
    parse_command_line()
    joinall([spawn(ws_serve), spawn(rpc_serve)])


if __name__ == '__main__':
    main()
