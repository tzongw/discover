# -*- coding: utf-8 -*-
import logging
import gevent
from typing import Dict
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from . import shared, const
from redis.client import Pipeline
from redis import Redis
from base import utils
from base.mq import Publisher
from service import user
from .shared import timer_dispatcher
from .config import options
from common import mq_pb2
from base.utils import Parser


class Handler:
    _PREFIX = 'online'
    _TTL = const.MISS_TIMES * const.PING_INTERVAL

    def __init__(self, redis: Redis, dispatcher: utils.Dispatcher):
        self._redis = redis
        self._parser = Parser(redis)
        self._timer_dispatcher = dispatcher

    @classmethod
    def _key(cls, uid):
        return f'{cls._PREFIX}:{uid}'

    def login(self, address: str, conn_id: str, params: Dict[str, str]):
        logging.info(f'{address} {conn_id} {params}')
        try:
            uid = int(params[const.CONTEXT_UID])
            token = params[const.CONTEXT_TOKEN]
            if token != "pass":
                raise ValueError("token")
            key = self._key(uid)

            def set_login_status(pipe: Pipeline):
                parser = Parser(pipe)
                online = parser.hget(key, mq_pb2.Online())
                pipe.multi()
                parser.hset(key, mq_pb2.Online(address=address, conn_id=conn_id))
                pipe.expire(key, self._TTL)
                Publisher(pipe).publish(mq_pb2.Login(uid=uid))
                return online

            old = self._redis.transaction(set_login_status, key, value_from_callable=True)
            if old.address and old.conn_id:
                logging.warning(f'kick conn {uid} {old}')
                with shared.gate_service.client(old.address) as client:
                    client.send_text(old.conn_id, f'login other device')
                    client.remove_conn(old.conn_id)
        except Exception as e:
            if isinstance(e, (KeyError, ValueError)):
                logging.warning(f'login fail {address} {conn_id} {params}')
            else:
                logging.exception(f'login error {address} {conn_id} {params}')
            with shared.gate_service.client(address) as client:
                client.send_text(conn_id, f'login fail {e}')
                client.remove_conn(conn_id)
        else:
            with shared.gate_service.client(address) as client:
                client.set_context(conn_id, {const.CONTEXT_UID: str(uid)})
                client.send_text(conn_id, f'login success')

    def ping(self, address: str, conn_id: str, context: Dict[str, str]):
        try:
            logging.debug(f'{address} {conn_id} {context}')
            uid = int(context[const.CONTEXT_UID])
            key = self._key(uid)
            online = self._parser.hget(key, mq_pb2.Online())
            if conn_id != online.conn_id:
                raise ValueError(f'{online} {conn_id}')
            self._redis.expire(key, self._TTL)
        except Exception as e:
            logging.warning(f'{address} {conn_id} {context} {e}')
            with shared.gate_service.client(address) as client:
                client.send_text(conn_id, f'not login')
                client.remove_conn(conn_id)

    def disconnect(self, address: str, conn_id: str, context: Dict[str, str]):
        logging.info(f'{address} {conn_id} {context}')
        if not context:
            return
        uid = int(context[const.CONTEXT_UID])
        key = self._key(uid)

        def unset_login_status(pipe: Pipeline):
            online = Parser(pipe).hget(key, mq_pb2.Online())
            if conn_id == online.conn_id:
                logging.info(f'clear {uid} {online}')
                pipe.multi()
                pipe.delete(key)
                Publisher(pipe).publish(mq_pb2.Logout(uid=uid))

        self._redis.transaction(unset_login_status, key)

    def recv_binary(self, address: str, conn_id: str, context: Dict[str, str], message: bytes):
        logging.debug(f'{address} {conn_id} {context} {message}')
        with shared.gate_service.client(address) as client:
            client.send_text(conn_id, f'can not read binary')

    def recv_text(self, address: str, conn_id: str, context: Dict[str, str], message: str):
        logging.debug(f'{address} {conn_id} {context} {message}')
        if not context:
            logging.warning(f'not login {address} {conn_id} {context} {message}')
            return
        uid = int(context[const.CONTEXT_UID])
        group = context.get(const.CONTEXT_GROUP)
        if message == 'join':
            with shared.gate_service.client(address) as client:
                client.join_group(conn_id, const.CHAT_ROOM)
                client.set_context(conn_id, {const.CONTEXT_GROUP: const.CHAT_ROOM})
            shared.gate_service.broadcast_text(const.CHAT_ROOM, [conn_id], f'sys: {uid} join')
        elif message == 'leave':
            with shared.gate_service.client(address) as client:
                client.leave_group(conn_id, const.CHAT_ROOM)
                client.unset_context(conn_id, {const.CONTEXT_GROUP})
            shared.gate_service.broadcast_text(const.CHAT_ROOM, [conn_id], f'sys: {uid} leave')
        else:
            if not group:
                with shared.gate_service.client(address) as client:
                    client.send_text(conn_id, f'not in group')
            else:
                shared.gate_service.broadcast_text(const.CHAT_ROOM, [conn_id], f'{uid}: {message}')

    def timeout(self, key, data):
        self._timer_dispatcher.dispatch(key, data)


def serve():
    handler = Handler(shared.redis, timer_dispatcher)
    processor = user.Processor(handler)
    transport = TSocket.TServerSocket(utils.wildcard, options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    g = gevent.spawn(server.serve)
    gevent.sleep(0.1)
    options.rpc_port = transport.handle.getsockname()[1]
    logging.info(f'Starting the server {options.host}:{options.rpc_port} ...')
    return g
