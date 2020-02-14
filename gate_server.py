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
from typing import Dict, DefaultDict, Set
from gevent import queue
from urllib import parse
from utils import LogSuppress
from collections import defaultdict
import utils
from concurrent.futures import Future
from schedule import Schedule

define("host", utils.ip_address(), str, "listen host")
define("ws_port", 0, int, "ws port")
define("rpc_port", 0, int, "rpc port")

parse_command_line()

rpc_address = f'{options.host}:{options.rpc_port}'
ws_address = f'{options.host}:{options.ws_port}'

app = Flask(__name__)
sockets = Sockets(app)


def ws_serve(fut: Future):
    server = pywsgi.WSGIServer(('', options.ws_port), app, handler_class=WebSocketHandler)

    def register():
        global ws_address
        options.ws_port = server.address[1]
        ws_address = f'{options.host}:{options.ws_port}'
        logging.info(f'Starting ws server {ws_address} ...')
        fut.set_result(ws_address)

    gevent.spawn_later(0.1, register)
    server.serve_forever()


class Client:
    schedule = Schedule()

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

    def send(self, message):
        self.messages.put_nowait(message)

    def serve(self):
        self.ws.handler.socket.settimeout(const.MISS_TIMES * const.PING_INTERVAL)
        self.schedule.call_later(self._ping, 5)  # will kick if not login
        gevent.spawn(self._writer)
        while not self.ws.closed:
            message = self.ws.receive()
            if isinstance(message, bytes):
                common.user_service.recv_binary(rpc_address, self.conn_id, self.context, message)
            elif isinstance(message, str):
                common.user_service.recv_text(rpc_address, self.conn_id, self.context, message)
            else:
                logging.warning(f'receive {message}')

    def _ping(self):
        if self.ws.closed:
            return
        with LogSuppress(Exception):
            common.user_service.ping(rpc_address, self.conn_id, self.context)
        self.schedule.call_later(self._ping, const.PING_INTERVAL)

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
        self.context.update(context)

    def unset_context(self, context):
        for key in context:
            self.context.pop(key, None)


clients = {}  # type: Dict[str, Client]
groups = defaultdict(set)  # type: DefaultDict[str, Set[Client]]


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
    try:
        params = {}
        for k, v in parse.parse_qsl(ws.environ['QUERY_STRING']):
            params[k] = v
        common.user_service.login(rpc_address, conn_id, params)
        client.serve()
    except Exception:
        logging.exception(f'{client}')
    finally:
        logging.info(f'finish {client}')
        for group in client.groups:
            remove_from_group(client, group)
        clients.pop(conn_id, None)
        client.stop()
        common.user_service.disconnect(rpc_address, conn_id, client.context)


def remove_from_group(client: Client, group):
    members = groups[group]
    members.discard(client)
    if not members:
        groups.pop(group)


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
        for client in groups.get(group, set()):  # type: Client
            if client.conn_id not in exclude:
                client.send(message)

    broadcast_binary = _broadcast_message
    broadcast_text = _broadcast_message


def rpc_serve(fut: Future):
    handler = Handler()
    processor = gate.Processor(handler)
    transport = TSocket.TServerSocket(utils.addr_wildchar, options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    def register():
        global rpc_address
        options.rpc_port = transport.handle.getsockname()[1]
        rpc_address = f'{options.host}:{options.rpc_port}'
        logging.info(f'Starting rpc server {rpc_address} ...')
        fut.set_result(rpc_address)

    gevent.spawn_later(0.1, register)
    server.serve()


def main():
    ws_future = Future()
    rpc_future = Future()
    ws = gevent.spawn(ws_serve, ws_future)
    rpc = gevent.spawn(rpc_serve, rpc_future)
    http_addr, rpc_addr = ws_future.result(timeout=1), rpc_future.result(timeout=1)
    common.service.start({const.WS_GATE: http_addr, const.RPC_GATE: rpc_addr})
    gevent.joinall([ws, rpc], raise_error=True)


if __name__ == '__main__':
    main()
