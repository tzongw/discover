# -*- coding: utf-8 -*-
import logging
from typing import Dict
import gevent
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from base import create_parser
from base import LogSuppress
from base.utils import salt_hash
import const
from config import options
from models import Online
from service import user
from common.messages import Connect, Disconnect
import shared
from shared import app, online_key, redis, dispatch_timeout
import push


class Handler:
    def login(self, address: str, conn_id: str, params: Dict[str, str]):
        logging.info(f'{address} {conn_id} {params}')
        try:
            params = {k[2:].lower() if k.startswith('X-') else k.lower(): v for k, v in params.items()}
            if cookie := params.pop('cookie', None):
                request = app.request_class({'HTTP_COOKIE': cookie})
                session = app.session_interface.open_session(app, request)
                params.update(session)
            uid = int(params[const.CTX_UID])
            token = params[const.CTX_TOKEN]
            sid = salt_hash(token, salt=uid)
            if sid not in shared.sessions.get(uid) and options.env != const.Environment.DEV:
                raise ValueError('token error')
            key = online_key(uid)
            with redis.pipeline(transaction=True) as pipe:
                create_parser(pipe).hgetall(key, Online)
                pipe.hsetex(key, conn_id, Online(session_id=sid, address=address), ex=const.ONLINE_TTL)
                conns = pipe.execute()[0]
            for _conn_id, online in conns.items():
                if online.session_id != sid:
                    continue
                logging.info(f'kick conn {uid} {_conn_id}')
                with LogSuppress(), shared.gate_service.client(online.address) as client:
                    client.send_text(_conn_id, f'login again')
                    client.remove_conn(_conn_id)
        except Exception as e:
            if isinstance(e, (KeyError, ValueError)):
                logging.info(f'login fail {address} {conn_id} {params}')
            else:
                logging.exception(f'login error {address} {conn_id} {params}')
            with shared.gate_service.client(address) as client:
                client.send_text(conn_id, f'login fail {e}')
                client.remove_conn(conn_id)
        else:
            shared.publisher.publish(Connect(uid=uid))
            with shared.gate_service.client(address) as client:
                client.set_context(conn_id, const.CTX_UID, str(uid))
                client.send_text(conn_id, f'login success: ping interval: {const.PING_INTERVAL}')

    def ping(self, address: str, conn_id: str, context: Dict[str, str]):
        try:
            logging.debug(f'{address} {conn_id} {context}')
            uid = int(context[const.CTX_UID])
            key = online_key(uid)
            values = redis.hexpire(key, const.ONLINE_TTL, conn_id)
            if values[0] != 1:
                raise ValueError(f'invalid {conn_id}')
        except (KeyError, ValueError) as e:
            logging.info(f'{address} {conn_id} {context} {e}')
            with shared.gate_service.client(address) as client:
                client.send_text(conn_id, f'not login')
                client.remove_conn(conn_id)

    def disconnect(self, address: str, conn_id: str, context: Dict[str, str]):
        logging.info(f'{address} {conn_id} {context}')
        if not context:
            return
        uid = int(context[const.CTX_UID])
        key = online_key(uid)
        if redis.hdel(key, conn_id):
            logging.info(f'logout {uid} {conn_id}')
            shared.publisher.publish(Disconnect(uid=uid))

    def recv_binary(self, address: str, conn_id: str, context: Dict[str, str], message: bytes):
        logging.debug(f'{address} {conn_id} {context} {message}')
        with shared.gate_service.client(address) as client:
            client.send_text(conn_id, f'can not read binary')

    def recv_text(self, address: str, conn_id: str, context: Dict[str, str], message: str):
        logging.debug(f'{address} {conn_id} {context} {message}')
        if not context:
            logging.warning(f'not login {address} {conn_id} {context} {message}')
            return
        uid = int(context[const.CTX_UID])
        group = context.get(const.CTX_GROUP)
        if message == 'join':
            with shared.gate_service.client(address) as client:
                client.join_group(conn_id, const.ROOM)
                client.set_context(conn_id, const.CTX_GROUP, const.ROOM)
            push.broadcast(const.ROOM, f'sys: {uid} join')
        elif message == 'leave':
            with shared.gate_service.client(address) as client:
                client.leave_group(conn_id, const.ROOM)
                client.unset_context(conn_id, const.CTX_GROUP, const.ROOM)
            push.broadcast(const.ROOM, f'sys: {uid} leave')
        else:
            if not group:
                with shared.gate_service.client(address) as client:
                    client.send_text(conn_id, f'not in group')
            else:
                shared.gate_service.broadcast_text(const.ROOM, [conn_id], f'{uid}: {message}')

    def timeout(self, full_key, data):
        dispatch_timeout(full_key, data)


def serve():
    handler = Handler()
    processor = user.Processor(handler)
    transport = TSocket.TServerSocket(port=options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    g = gevent.spawn(server.serve)
    if not options.rpc_port:
        gevent.sleep(0.01)
        options.rpc_port = transport.handle.getsockname()[1]
    logging.info(f'Starting rpc server {options.rpc_address} ...')
    return g
