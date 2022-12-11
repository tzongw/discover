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
import shared, const
from config import options

app = Flask(__name__)
sockets = Sockets(app)


def serve():
    server = pywsgi.WSGIServer(options.ws_port, app, handler_class=WebSocketHandler, log=logging.getLogger(),
                               error_log=logging.getLogger())
    g = gevent.spawn(server.serve_forever)
    gevent.sleep(0.1)
    if not options.ws_port:
        options.ws_port = server.address[1]
    logging.info(f'Starting ws server {options.ws_address} ...')
    return g


class Client:
    ping_message = object()

    __slots__ = ["conn_id", "context", "ws", "messages", "groups", "writing"]

    def __init__(self, ws: WebSocket, conn_id):
        self.conn_id = conn_id
        self.context = {}
        self.ws = ws
        self.messages = queue.Queue()
        self.groups = set()
        self.writing = False

    def __del__(self):
        logging.debug(f'del {self}')

    def __str__(self):
        return f'{self.conn_id} {self.context}'

    def send(self, message):
        self.messages.put_nowait(message)
        if not self.writing:
            self.writing = True
            if message is self.ping_message:
                shared.executor.submit(self._writer, 0)
            else:
                gevent.spawn(self._writer, const.PING_INTERVAL / 2)

    def serve(self):
        self.ws.handler.socket.settimeout(const.CLIENT_TTL)
        pc = PeriodicCallback(shared.schedule, self._ping, const.PING_INTERVAL)
        try:
            while not self.ws.closed:
                message = self.ws.receive()
                addr = shared.user_service.address(hint=self.conn_id)
                if isinstance(message, bytes):
                    with shared.user_service.client(addr) as client:
                        client.recv_binary(options.rpc_address, self.conn_id, self.context, message)
                elif isinstance(message, str):
                    with shared.user_service.client(addr) as client:
                        client.recv_text(options.rpc_address, self.conn_id, self.context, message)
        finally:
            pc.stop()

    def _ping(self):
        self.send(self.ping_message)
        addr = shared.user_service.address(hint=self.conn_id)
        with shared.user_service.client(addr) as client:
            client.ping(options.rpc_address, self.conn_id, self.context)

    def _writer(self, timeout):
        logging.debug(f'start {self}')
        idle_timeout = False
        try:
            while True:
                idle_timeout = False
                message = self.messages.get(timeout=timeout)
                if message is None:
                    break
                if message is self.ping_message:
                    self.ws.send_frame('', WebSocket.OPCODE_PING)
                else:
                    self.ws.send(message)
        except queue.Empty:
            logging.debug(f'writer idle exit')
            idle_timeout = True
        except (WebSocketError, OSError) as e:
            logging.debug(f'peer closed {self} {e}')
        except Exception:
            logging.exception(f'{self}')
        else:
            logging.debug(f'exit {self}')
        finally:
            self.writing = False
            if not idle_timeout:
                self.ws.close()
            elif not self.messages.empty():  # race condition, do again
                self.send(self.ping_message)

    def stop(self):
        self.messages.put_nowait(None)

    def set_context(self, key, value):
        self.context[key] = value

    def unset_context(self, key, value):
        if self.context.get(key) == value or value == "":
            self.context.pop(key, None)


clients = {}  # type: Dict[str, Client]
groups = defaultdict(set)  # type: DefaultDict[str, Set[Client]]


def remove_from_group(client: Client, group):
    members = groups[group]
    members.discard(client)
    if not members:
        groups.pop(group)


def normalize_header(name: str):
    return name[len('HTTP_X_'):].replace('_', '-')


@sockets.route('/ws')
def client_serve(ws: WebSocket):
    conn_id = str(uuid.uuid4())
    client = Client(ws, conn_id)
    clients[conn_id] = client
    logging.info(f'new client {client}')
    try:
        params = {normalize_header(k): v for k, v in ws.environ.items() if k.startswith('HTTP_X_')}
        cookie = ws.environ.get('HTTP_COOKIE')
        if cookie:
            params['cookie'] = cookie
        for k, v in parse.parse_qsl(ws.environ['QUERY_STRING']):
            params[k] = v
        shared.user_service.login(options.rpc_address, conn_id, params)
        client.serve()
    except Exception:
        logging.exception(f'{client}')
    finally:
        logging.info(f'finish {client}')
        for group in client.groups:
            remove_from_group(client, group)
        clients.pop(conn_id)
        client.stop()
        shared.user_service.disconnect(options.rpc_address, conn_id, client.context)
