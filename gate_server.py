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
from gevent import pywsgi
import gevent
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket.websocket import WebSocket
import uuid
from typing import Dict
import json
from gevent.queue import Queue
from urllib import parse

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
        self._context = {}
        self.ws = ws
        ws.handler.socket.timeout = const.MISS_TIMES * const.PING_INTERVAL
        params = parse.parse_qsl(ws.handler.environ['QUERY_STRING'])
        self.params = {}
        for k, v in params:
            self.params[k] = v
        self.messages = Queue()
        self.writer = gevent.spawn(self._writer)

    def __del__(self):
        self.stop()

    @property
    def context(self):
        return json.dumps(self._context)

    def __repr__(self):
        return f'{self._context} {self.params}'

    def send(self, message):
        self.messages.put_nowait(message)

    def serve(self):
        while not self.ws.closed:
            self.ws.receive()

    def _writer(self):
        try:
            while True:
                message = self.messages.get()
                self.ws.send(message)
        except Exception as e:
            logging.error(f'{self} {e}')
            raise
        finally:
            logging.info(f'{self}')

    def stop(self):
        self.ws.close()
        gevent.kill(self.writer)

    def set_context(self, context):
        d = json.loads(context)
        self._context.update(d)

    def unset_context(self, context):
        for key in context:
            self._context.pop(key, None)


clients = {}  # type: Dict[str, Client]


@sockets.route('/ws')
def client_serve(ws: WebSocket):
    conn_id = str(uuid.uuid4())
    client = Client(ws)
    clients[conn_id] = client
    logging.info(f'{conn_id} {client}')
    common.service_pools.login(rpc_address, conn_id, client.params)
    try:
        client.serve()
    except Exception as e:
        logging.error(f'{conn_id} {client} {e}')
    finally:
        logging.info(f'{conn_id} {client}')
        clients.pop(conn_id, None)
        common.service_pools.disconnect(rpc_address, conn_id, client.context)


class Handler:
    def set_context(self, conn_id, context):
        logging.debug(f'{conn_id} {context}')
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{conn_id} {client}')
            client.set_context(context)

    def unset_context(self, conn_id, context):
        logging.debug(f'{conn_id} {context}')
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{conn_id} {client}')
            client.unset_context(context)

    def remove_conn(self, conn_id):
        logging.info(f'{conn_id}')
        client = clients.get(conn_id)
        if client:
            logging.info(f'{conn_id} {client}')
            client.stop()


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
    gevent.joinall([gevent.spawn(ws_serve), gevent.spawn(rpc_serve)])


if __name__ == '__main__':
    main()
