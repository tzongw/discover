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
from geventwebsocket.exceptions import WebSocketError
import uuid
from typing import Dict
import json
from gevent import queue
from urllib import parse
from utils import LogSuppress

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
    def __init__(self, ws: WebSocket, conn_id):
        self.conn_id = conn_id
        self._context = {}
        self.ws = ws
        ws.handler.socket.settimeout(const.MISS_TIMES * const.PING_INTERVAL)
        params = parse.parse_qsl(ws.environ['QUERY_STRING'])
        self.params = {}
        for k, v in params:
            self.params[k] = v
        self.messages = queue.Queue()
        gevent.spawn(self._writer)
        gevent.spawn(self._ping)

    def __del__(self):
        logging.debug(f'del {self}')

    @property
    def context(self):
        return json.dumps(self._context)

    def __repr__(self):
        return f' {self.conn_id} {self._context}'

    def send(self, message):
        self.messages.put_nowait(message)

    def serve(self):
        while not self.ws.closed:
            self.ws.receive()

    def _ping(self):
        logging.info(f'start {self}')
        while True:
            gevent.sleep(const.PING_INTERVAL)
            if self.ws.closed:
                break
            with LogSuppress(Exception):
                common.service_pools.ping(rpc_address, self.conn_id, self.context)
        logging.info(f'exit {self}')

    def _writer(self):
        logging.info(f'start {self}')
        try:
            while True:
                message = self.messages.get()
                if message is None:
                    break
                self.ws.send(message)
        except (WebSocketError, OSError) as e:
            logging.info(f'peer closed {self} {e}')
        except Exception:
            logging.exception(f'{self}')
        else:
            logging.info(f'exit {self}')
        finally:
            self.ws.close()

    def stop(self):
        self.messages.put_nowait(None)

    def set_context(self, context):
        d = json.loads(context)
        self._context.update(d)

    def unset_context(self, context):
        for key in context:
            self._context.pop(key, None)


clients = {}  # type: Dict[str, Client]


def clean_up():
    for client in clients.values():
        client.stop()


common.clean_ups.append(clean_up)


@sockets.route('/ws')
def client_serve(ws: WebSocket):
    conn_id = str(uuid.uuid4())
    client = Client(ws, conn_id)
    clients[conn_id] = client
    logging.info(f'new client {client}')
    common.service_pools.login(rpc_address, conn_id, client.params)
    try:
        client.serve()
    except Exception:
        logging.exception(f'{client}')
    finally:
        logging.info(f'finish {client}')
        clients.pop(conn_id, None)
        client.stop()
        common.service_pools.disconnect(rpc_address, conn_id, client.context)


class Handler:
    def set_context(self, conn_id, context):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {context}')
            client.set_context(context)
        else:
            logging.debug(f'not found {conn_id} {context}')

    def unset_context(self, conn_id, context):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {context}')
            client.unset_context(context)
        else:
            logging.debug(f'not found {conn_id} {context}')

    def remove_conn(self, conn_id):
        client = clients.get(conn_id)
        if client:
            logging.info(f'{client}')
            client.stop()
        else:
            logging.info(f'not found {conn_id}')

    def _send_message(self, conn_id, message):
        client = clients.get(conn_id)
        if client:
            logging.debug(f'{client} {message}')
            client.send(message)
        else:
            logging.debug(f'not found {conn_id} {message}')

    send_text = _send_message
    send_binary = _send_message


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
