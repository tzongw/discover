# -*- coding: utf-8 -*-
import os
import logging
import socket
from urllib import parse
from collections import defaultdict
import gevent
from gevent import pywsgi
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket.websocket import WebSocket
from base.utils import base62
from base.sharding import ShardingDict, ShardingSet
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
    if sock_path := options.unix_sock:
        try:
            os.unlink(sock_path)
        except FileNotFoundError:
            pass
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(sock_path)
        os.chmod(sock_path, 0o777)
        sock.listen()
        listener = sock
    else:
        listener = options.http_port
    logger = None if options.env == const.Environment.PROD else logging.getLogger()
    server = pywsgi.WSGIServer(listener, app, handler_class=WebSocketHandler, log=logger, error_log=logging.getLogger())
    g = gevent.spawn(server.serve_forever)
    if not options.unix_sock and not options.http_port:
        gevent.sleep(0.01)
        options.http_port = server.address[1]
    logging.info(f'Starting ws server {options.http_address} ...')
    return g


def handle_ping(self: WebSocket, header, payload):
    client: Client = self.environ['WS_CLIENT']
    client.handle_ping()


WebSocket.handle_ping = handle_ping


class Client:
    __slots__ = ['conn_id', 'context', 'ws', 'messages', 'groups', 'writing', 'step']
    PONG_MESSAGE = object()

    def __init__(self, ws: WebSocket, conn_id):
        self.conn_id = conn_id
        self.context = {}
        self.ws = ws
        self.messages = []
        self.groups = set()
        self.writing = False
        self.step = 0

    def __str__(self):
        return f'{self.conn_id} {self.context}'

    def send(self, message):
        self.messages.append(message)
        if self.writing:
            return
        self.writing = True
        gevent.spawn(self._writer)

    def serve(self):
        self.ws.handler.socket.settimeout(const.WS_TIMEOUT)
        while message := self.ws.receive():
            addr = shared.user_service.address(hint=self.conn_id)
            with shared.user_service.client(addr) as client:
                if isinstance(message, str):
                    client.recv_text(options.rpc_address, self.conn_id, self.context, message)
                else:
                    client.recv_binary(options.rpc_address, self.conn_id, self.context, message)

    def handle_ping(self):
        # pong or no pong?
        # self.send(self.PONG_MESSAGE)
        self.step += 1
        if self.step < const.RPC_PING_STEP:
            return
        self.step = 0
        addr = shared.user_service.address(hint=self.conn_id)
        with shared.user_service.client(addr) as client:
            client.ping(options.rpc_address, self.conn_id, self.context)

    def _writer(self):
        assert self.writing
        try:
            while self.messages:
                messages = self.messages
                self.messages = []
                for message in messages:
                    if message is None:
                        raise StopIteration
                    elif message is self.PONG_MESSAGE:
                        self.ws.send_frame(b'', WebSocket.OPCODE_PONG)
                    else:
                        self.ws.send(message)
            else:
                self.writing = False
        except Exception:
            self.ws.close()  # keep writing status true

    def stop(self):
        self.send(None)

    def set_context(self, key, value):
        self.context[key] = value

    def unset_context(self, key, value):
        if not value or self.context.get(key) == value:
            self.context.pop(key, None)


clients = ShardingDict[str, Client](shards=32)
groups = defaultdict(lambda: ShardingSet(shards=8))  # type: dict[str, ShardingSet[Client]]


def remove_from_group(client: Client, group):
    members = groups[group]
    members.discard(client)
    if not members:
        groups.pop(group)


def client_serve(ws: WebSocket):
    conn_id = base62.encode(shared.id_generator.gen())
    client = Client(ws, conn_id)
    clients[conn_id] = client
    environ = ws.environ
    environ['WS_CLIENT'] = client
    logging.info(f'++ {len(clients)} {client}')
    try:
        params = {k[5:].replace('_', '-'): v for k, v in environ.items() if k.startswith('HTTP_')}
        for k, v in parse.parse_qsl(environ['QUERY_STRING']):
            params[k] = v
        shared.user_service.login(options.rpc_address, conn_id, params)
        client.serve()
    except Exception as e:
        logging.info(f'{client} {e}')
    finally:
        environ.pop('WS_CLIENT')
        for group in client.groups:
            remove_from_group(client, group)
        clients.pop(conn_id)
        client.stop()
        logging.info(f'-- {len(clients)} {client}')
        shared.user_service.disconnect(options.rpc_address, conn_id, client.context)
