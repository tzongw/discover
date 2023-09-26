# -*- coding: utf-8 -*-
import logging
from typing import Dict
from urllib import parse
from collections import defaultdict
from typing import Set
import gevent
from gevent import pywsgi
from gevent import queue
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket.websocket import WebSocket
from base import utils
import shared
import const
from config import options


def app(environ, start_response):
    if environ['PATH_INFO'] == '/ws':
        if ws := environ.get('wsgi.websocket'):
            client_serve(ws)
        else:
            start_response('400 Bad Request', [])
    else:
        start_response('404 Not Found', [])
    return b''


def serve():
    server = pywsgi.WSGIServer(options.ws_port, app, handler_class=WebSocketHandler, log=logging.getLogger(),
                               error_log=logging.getLogger())
    g = gevent.spawn(server.serve_forever)
    if not options.ws_port:
        gevent.sleep(0.01)
        options.ws_port = server.address[1]
    logging.info(f'Starting ws server {options.ws_address} ...')
    return g


def handle_ping(self: WebSocket, header, payload):
    self.send_frame(payload, self.OPCODE_PONG)
    client: Client = self.environ['WS_CLIENT']
    client.rpc_ping()


WebSocket.handle_ping = handle_ping


class Client:
    CLIENT_TTL = 3 * const.PING_INTERVAL

    __slots__ = ['conn_id', 'context', 'ws', 'messages', 'groups', 'writing', 'step']

    def __init__(self, ws: WebSocket, conn_id):
        self.conn_id = conn_id
        self.context = {}
        self.ws = ws
        self.messages = queue.Queue()
        self.groups = set()
        self.writing = False
        self.step = 0

    def __str__(self):
        return f'{self.conn_id} {self.context}'

    def send(self, message):
        self.messages.put_nowait(message)
        if self.writing:
            return
        self.writing = True
        gevent.spawn(self._writer)

    def serve(self):
        self.ws.handler.socket.settimeout(self.CLIENT_TTL)
        while message := self.ws.receive():
            addr = shared.user_service.address(hint=self.conn_id)
            with shared.user_service.client(addr) as client:
                if isinstance(message, str):
                    client.recv_text(options.rpc_address, self.conn_id, self.context, message)
                else:
                    client.recv_binary(options.rpc_address, self.conn_id, self.context, message)

    def rpc_ping(self):
        self.step += 1
        if self.step < const.RPC_PING_STEP:
            return
        self.step = 0
        addr = shared.user_service.address(hint=self.conn_id)
        with shared.user_service.client(addr) as client:
            client.ping(options.rpc_address, self.conn_id, self.context)

    def _writer(self):
        logging.debug(f'start {self}')
        try:
            while message := self.messages.get(timeout=const.PING_INTERVAL / 2):
                self.ws.send(message)
        except queue.Empty:
            logging.debug(f'idle {self}')
            if self.messages.empty():
                self.writing = False
            else:  # race condition, do again
                gevent.spawn(self._writer)
        except Exception as e:
            logging.debug(f'exit {self} {e}')
            self.ws.close()
        else:
            logging.debug(f'stop {self}')
            self.ws.close()

    def stop(self):
        self.messages.put_nowait(None)

    def set_context(self, key, value):
        self.context[key] = value

    def unset_context(self, key, value):
        if not value or self.context.get(key) == value:
            self.context.pop(key, None)


clients = {}  # type: Dict[str, Client]
groups = defaultdict(set)  # type: Dict[str, Set[Client]]


def remove_from_group(client: Client, group):
    members = groups[group]
    members.discard(client)
    if not members:
        groups.pop(group)


def normalize_header(name: str):
    return name[len('HTTP_X_'):].replace('_', '-')


def client_serve(ws: WebSocket):
    conn_id = utils.base62(shared.id_generator.gen())
    client = Client(ws, conn_id)
    clients[conn_id] = client
    environ = ws.environ
    environ['WS_CLIENT'] = client
    logging.info(f'++ {len(clients)} {client}')
    try:
        params = {normalize_header(k): v for k, v in environ.items() if k.startswith('HTTP_X_')}
        cookie = environ.get('HTTP_COOKIE')
        if cookie:
            params['cookie'] = cookie
        for k, v in parse.parse_qsl(environ['QUERY_STRING']):
            params[k] = v
        shared.user_service.login(options.rpc_address, conn_id, params)
        client.serve()
    except Exception:
        logging.exception(f'{client}')
    finally:
        environ.pop('WS_CLIENT')
        for group in client.groups:
            remove_from_group(client, group)
        clients.pop(conn_id)
        client.stop()
        logging.info(f'-- {len(clients)} {client}')
        shared.user_service.disconnect(options.rpc_address, conn_id, client.context)
