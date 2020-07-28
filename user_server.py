# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import time
import const
from tornado.options import options, define, parse_command_line
import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import gevent
from service import user
import common
from common import timer_dispatcher as dispatcher
from typing import Dict
from redis.client import Pipeline
from utils import LogSuppress
from redis import Redis
import utils

define("host", utils.ip_address(), str, "listen host")
define("rpc_port", 0, int, "rpc port")

parse_command_line()


class Handler:
    _PREFIX = 'online'
    _TTL = const.MISS_TIMES * const.PING_INTERVAL

    def __init__(self, redis: Redis, dispatcher: utils.Dispatcher):
        self._redis = redis
        self._dispatcher = dispatcher

    @classmethod
    def _key(cls, uid):
        return f'{cls._PREFIX}:{uid}'

    def login(self, address: str, conn_id: str, params: Dict[str, str]):
        logging.info(f'{address} {conn_id} {params}')
        try:
            uid = int(params.pop(const.CONTEXT_UID))
            token = params.pop(const.CONTEXT_TOKEN)
            if token != "pass":
                raise ValueError("token")
            key = self._key(uid)

            def set_login_status(pipe: Pipeline):
                values = pipe.hmget(key, const.ONLINE_CONN_ID, const.ONLINE_ADDRESS)
                pipe.multi()
                pipe.hmset(key, {const.ONLINE_ADDRESS: address, const.ONLINE_CONN_ID: conn_id})
                pipe.expire(key, self._TTL)
                return values

            old_conn_id, old_address = self._redis.transaction(set_login_status, key, value_from_callable=True)
            if old_conn_id and old_address:
                logging.warning(f'kick conn {uid} {old_conn_id} {old_address}')
                with LogSuppress(Exception):
                    with common.gate_service.client(old_address) as client:
                        client.send_text(old_conn_id, f'login other device')
                        client.remove_conn(old_conn_id)
        except Exception as e:
            if isinstance(e, (KeyError, ValueError)):
                logging.warning(f'login fail {address} {conn_id} {params}')
            else:
                logging.exception(f'login error {address} {conn_id} {params}')
            with common.gate_service.client(address) as client:
                client.send_text(conn_id, f'login fail {e}')
                client.remove_conn(conn_id)
        else:
            with common.gate_service.client(address) as client:
                client.set_context(conn_id, {const.CONTEXT_UID: str(uid)})
                client.send_text(conn_id, f'login success')

    def ping(self, address: str, conn_id: str, context: Dict[str, str]):
        try:
            logging.debug(f'{address} {conn_id} {context}')
            uid = int(context[const.CONTEXT_UID])
            key = self._key(uid)
            login_conn_id = self._redis.hget(key, const.ONLINE_CONN_ID)
            if login_conn_id != conn_id:
                raise ValueError(f'{login_conn_id}')
            self._redis.expire(key, self._TTL)
        except Exception as e:
            logging.warning(f'{address} {conn_id} {context} {e}')
            with common.gate_service.client(address) as client:
                client.send_text(conn_id, f'not login')
                client.remove_conn(conn_id)

    def disconnect(self, address: str, conn_id: str, context: Dict[str, str]):
        logging.info(f'{address} {conn_id} {context}')
        if not context:
            return
        uid = int(context[const.CONTEXT_UID])
        key = self._key(uid)

        def unset_login_status(pipe: Pipeline):
            old_conn_id = pipe.hget(key, const.ONLINE_CONN_ID)
            if old_conn_id == conn_id:
                logging.info(f'clear {uid} {old_conn_id}')
                pipe.multi()
                pipe.delete(key)

        self._redis.transaction(unset_login_status, key)

    def recv_binary(self, address: str, conn_id: str, context: Dict[str, str], message: bytes):
        logging.debug(f'{address} {conn_id} {context} {message}')
        with common.gate_service.client(address) as client:
            client.send_text(conn_id, f'can not read binary')

    def recv_text(self, address: str, conn_id: str, context: Dict[str, str], message: str):
        logging.debug(f'{address} {conn_id} {context} {message}')
        if not context:
            logging.warning(f'not login {address} {conn_id} {context} {message}')
            return
        uid = int(context[const.CONTEXT_UID])
        group = context.get(const.CONTEXT_GROUP)
        if message == 'join':
            with common.gate_service.client(address) as client:
                client.join_group(conn_id, const.CHAT_ROOM)
                client.set_context(conn_id, {const.CONTEXT_GROUP: const.CHAT_ROOM})
            common.gate_service.broadcast_text(const.CHAT_ROOM, [conn_id], f'sys: {uid} join')
        elif message == 'leave':
            with common.gate_service.client(address) as client:
                client.leave_group(conn_id, const.CHAT_ROOM)
                client.unset_context(conn_id, {const.CONTEXT_GROUP})
            common.gate_service.broadcast_text(const.CHAT_ROOM, [conn_id], f'sys: {uid} leave')
        else:
            if not group:
                with common.gate_service.client(address) as client:
                    client.send_text(conn_id, f'not in group')
            else:
                common.gate_service.broadcast_text(const.CHAT_ROOM, [conn_id], f'{uid}: {message}')

    def timeout(self, key, data):
        self._dispatcher.dispatch(key, data)


def init_timers():
    @dispatcher.handler('welcome')
    def welcome(data):
        logging.info(f'got timer {data}')

    @dispatcher.handler('notice')
    def notice(data):
        logging.info(f'got timer {data}')

    common.timer_service.call_repeat('welcome', const.RPC_USER, 'welcome', 3)
    common.timer_service.call_at('notice', const.RPC_USER, 'notice', time.time() + 10)


def main():
    logging.warning(f'worker id: {common.unique_id.generate()}')
    handler = Handler(common.redis, dispatcher)
    processor = user.Processor(handler)
    transport = TSocket.TServerSocket(utils.wildcard, options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    def register():
        options.rpc_port = transport.handle.getsockname()[1]
        logging.info(f'Starting the server {options.host}:{options.rpc_port} ...')
        common.registry.start({const.RPC_USER: f'{options.host}:{options.rpc_port}'})
        # init_timers()

    gevent.spawn_later(0.1, register)
    server.serve()


if __name__ == '__main__':
    main()
