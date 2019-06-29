# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import const
from tornado.options import options, define, parse_command_line
import logging
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from generated.service import user
import common
import json
from typing import Dict
from redis.client import Pipeline
from utils import LogSuppress
from redis import Redis

define("host", "127.0.0.1", str, "listen host")
define("port", 50001, int, "listen port")


class Handler:
    _PREFIX = 'online'
    _TTL = const.MISS_TIMES * const.PING_INTERVAL

    def __init__(self, redis: Redis):
        self._redis = redis

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
        except Exception as e:
            if isinstance(e, (KeyError, ValueError)):
                logging.warning(f'login fail {address} {conn_id} {params}')
            else:
                logging.exception(f'login error {address} {conn_id} {params}')
            with common.service_pools.address_gate_client(address) as client:
                client.send_text(conn_id, f'login fail {e}')
                client.remove_conn(conn_id)
        else:
            with common.service_pools.address_gate_client(address) as client:
                client.set_context(conn_id, json.dumps({const.CONTEXT_UID: uid}))
                client.send_text(conn_id, f'login success')

            key = self._key(uid)

            def set_login_status(pipe: Pipeline):
                status = pipe.hgetall(key)
                if status:
                    logging.warning(f'kick conn {uid} {status}')
                    with LogSuppress(Exception):
                        old_conn_id = status[const.ONLINE_CONN_ID]
                        with common.service_pools.address_gate_client(address) as client:
                            client.send_text(old_conn_id, f'login other device')
                            client.remove_conn(old_conn_id)
                pipe.multi()
                pipe.hmset(key, {const.ONLINE_ADDRESS: address, const.ONLINE_CONN_ID: conn_id})
                pipe.expire(key, self._TTL)

            self._redis.transaction(set_login_status, key)

    def ping(self, address: str, conn_id: str, context: str):
        d = json.loads(context)
        if d:
            logging.debug(f'{address} {conn_id} {context}')
            uid = d[const.CONTEXT_UID]
            self._redis.expire(self._key(uid), self._TTL)
        else:
            logging.warning(f'{address} {conn_id} {context}')

    def disconnect(self, address: str, conn_id: str, context: str):
        logging.info(f'{address} {conn_id}, {context}')
        d = json.loads(context)
        if d:
            uid = d[const.CONTEXT_UID]
            key = self._key(uid)

            def unset_login_status(pipe: Pipeline):
                status = pipe.hgetall(key)
                if status.get(const.ONLINE_CONN_ID) == conn_id:
                    logging.info(f'clear {uid} {status}')
                    pipe.multi()
                    pipe.delete(key)

            self._redis.transaction(unset_login_status, key)


def main():
    parse_command_line()
    common.service.register(const.SERVICE_USER, f'{options.host}:{options.port}')
    common.service.start()

    handler = Handler(common.redis)
    processor = user.Processor(handler)
    transport = TSocket.TServerSocket(options.host, options.port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    logging.info(f'Starting the server {options.host}:{options.port} ...')
    server.serve()


if __name__ == '__main__':
    main()
