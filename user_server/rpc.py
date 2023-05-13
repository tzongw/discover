# -*- coding: utf-8 -*-
import logging
import gevent
from typing import Dict
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import shared
import const
from common.messages import Login, Logout
from models import Online, Session
from redis.client import Pipeline
from service import user
from shared import dispatcher, app, online_key, redis, session_key
from config import options
from base import SmartParser
import helpers


class Handler:
    def login(self, address: str, conn_id: str, params: Dict[str, str]):
        logging.info(f'{address} {conn_id} {params}')
        try:
            params = {k.lower(): v for k, v in params.items()}
            cookie = params.pop('cookie', None)
            if cookie:
                request = app.request_class({'HTTP_COOKIE': cookie})
                session = app.open_session(request)
                params.update(session)
            uid = int(params[const.CTX_UID])
            token = params[const.CTX_TOKEN]
            session = shared.parser.get(session_key(uid), Session)
            if (session is None or session.token != token) and options.env is not const.Environment.DEV:
                raise ValueError("token error")
            old_online = shared.parser.set(online_key(uid), Online(address=address, conn_id=conn_id),
                                           ex=const.CLIENT_TTL, get=True)
            shared.publisher.publish(Login(uid=uid))
            if old_online:
                logging.info(f'kick conn {uid} {old_online}')
                with shared.gate_service.client(old_online.address) as client:
                    client.send_text(old_online.conn_id, f'login other device')
                    client.remove_conn(old_online.conn_id)
        except Exception as e:
            if isinstance(e, (KeyError, ValueError)):
                logging.info(f'login fail {address} {conn_id} {params}')
            else:
                logging.exception(f'login error {address} {conn_id} {params}')
            with shared.gate_service.client(address) as client:
                client.send_text(conn_id, f'login fail {e}')
                client.remove_conn(conn_id)
        else:
            with shared.gate_service.client(address) as client:
                client.set_context(conn_id, const.CTX_UID, str(uid))
                client.send_text(conn_id, f'login success')

    def ping(self, address: str, conn_id: str, context: Dict[str, str]):
        try:
            logging.debug(f'{address} {conn_id} {context}')
            uid = int(context[const.CTX_UID])
            key = online_key(uid)
            online = shared.parser.get(key, Online)
            if online is None or online.conn_id != conn_id:
                raise ValueError(f'{online} {conn_id}')
            redis.expire(key, const.CLIENT_TTL)
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

        def unset_online(pipe: Pipeline):
            online = SmartParser(pipe).get(key, Online)
            if online and online.conn_id == conn_id:
                logging.info(f'clear {uid} {online}')
                pipe.multi()
                pipe.delete(key)

        if redis.transaction(unset_online, key):
            shared.publisher.publish(Logout(uid=uid))

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
            helpers.broadcast(const.ROOM, f'sys: {uid} join')
        elif message == 'leave':
            with shared.gate_service.client(address) as client:
                client.leave_group(conn_id, const.ROOM)
                client.unset_context(conn_id, const.CTX_GROUP, const.ROOM)
            helpers.broadcast(const.ROOM, f'sys: {uid} leave')
        else:
            if not group:
                with shared.gate_service.client(address) as client:
                    client.send_text(conn_id, f'not in group')
            else:
                shared.gate_service.broadcast_text(const.ROOM, [conn_id], f'{uid}: {message}')

    def timeout(self, key, data):
        group, key = key.split(':', maxsplit=1)
        dispatcher.dispatch(group, key, data)


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
