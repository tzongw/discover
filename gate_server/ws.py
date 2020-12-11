# -*- coding: utf-8 -*-
import logging
import uuid
from typing import Dict
from urllib import parse
from collections import defaultdict
from typing import DefaultDict, Set
import gevent
from flask import Flask
from flask_sockets import Sockets
from gevent import pywsgi
from gevent import queue
from geventwebsocket.exceptions import WebSocketError
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket.websocket import WebSocket
from base.schedule import PeriodicCallback
from . import shared, const
from .config import options

app = Flask(__name__)
sockets = Sockets(app)


def serve():
    server = pywsgi.WSGIServer(('', options.ws_port), app, handler_class=WebSocketHandler)
    g = gevent.spawn(server.serve_forever)
    gevent.sleep(0.1)
    options.ws_port = server.address[1]
    ws_address = f'{options.host}:{options.ws_port}'
    logging.info(f'Starting ws server {ws_address} ...')
    return g


class Client:
    schedule = shared.schedule
    ping_message = object()

    __slots__ = ["conn_id", "context", "ws", "messages", "groups"]

    def __init__(self, ws: WebSocket, conn_id):
        self.conn_id = conn_id
        self.context = {}
        self.ws = ws
        self.messages = queue.Queue()
        self.groups = set()

    def __del__(self):
        logging.debug(f'del {self}')

    def __repr__(self):
        return f'{self.conn_id} {self.context}'

    @property
    def rpc_address(self):
        return f'{options.host}:{options.rpc_port}'

    def send(self, message):
        self.messages.put_nowait(message)

    def serve(self):
        self.ws.handler.socket.settimeout(const.MISS_TIMES * const.PING_INTERVAL)
        gevent.spawn(self._writer)
        pc = PeriodicCallback(self.schedule, self._ping, const.PING_INTERVAL).start()
        try:
            while not self.ws.closed:
                message = self.ws.receive()
                if isinstance(message, bytes):
                    shared.user_service.recv_binary(self.rpc_address, self.conn_id, self.context, message)
                elif isinstance(message, str):
                    shared.user_service.recv_text(self.rpc_address, self.conn_id, self.context, message)
                else:
                    logging.warning(f'receive {message}')
        finally:
            pc.stop()

    def _ping(self):
        self.send(self.ping_message)
        shared.user_service.ping(self.rpc_address, self.conn_id, self.context)

    def _writer(self):
        logging.info(f'start {self}')
        try:
            while True:
                message = self.messages.get()
                if message is None:
                    break
                if message is self.ping_message:
                    self.ws.send_frame('', WebSocket.OPCODE_PING)
                else:
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
        self.context.update(context)

    def unset_context(self, context):
        for key in context:
            self.context.pop(key, None)


clients = {}  # type: Dict[str, Client]
groups = defaultdict(set)  # type: DefaultDict[str, Set[Client]]


def remove_from_group(client: Client, group):
    members = groups[group]
    members.discard(client)
    if not members:
        groups.pop(group)


@sockets.route('/ws')
def client_serve(ws: WebSocket):
    rpc_address = f'{options.host}:{options.rpc_port}'
    conn_id = str(uuid.uuid4())
    client = Client(ws, conn_id)
    clients[conn_id] = client
    logging.info(f'new client {client}')
    try:
        params = {}
        for k, v in parse.parse_qsl(ws.environ['QUERY_STRING']):
            params[k] = v
        shared.user_service.login(rpc_address, conn_id, params)
        client.serve()
    except Exception:
        logging.exception(f'{client}')
    finally:
        logging.info(f'finish {client}')
        for group in client.groups:
            remove_from_group(client, group)
        clients.pop(conn_id)
        client.stop()
        shared.user_service.disconnect(rpc_address, conn_id, client.context)