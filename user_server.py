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

define("host", "127.0.0.1", str, "listen host")
define("port", 50001, int, "listen port")


class Handler:
    _PREFIX = 'online'
    _TTL = const.MISS_TIMES * const.PING_INTERVAL

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
                logging.error(f'login error {address} {conn_id} {params} {e}')
            common.service_pools.send_text(conn_id, f'login fail {e}')
            common.service_pools.remove_conn(conn_id)
        else:
            common.service_pools.set_context(conn_id, json.dumps({const.CONTEXT_UID: uid}))
            common.service_pools.send_text(conn_id, f'login success')

            key = self._key(uid)

            def set_login_status(pipe: Pipeline):
                status = pipe.hgetall(key)
                if status:
                    logging.warning(f'kick conn {uid} {status}')
                    with common.LogSuppress(Exception):
                        common.service_pools.send_text(status[const.ONLINE_CONN_ID], f'login other device')
                        common.service_pools.remove_conn(conn_id)
                pipe.multi()
                pipe.hmset(key, {const.ONLINE_ADDRESS: address, const.ONLINE_CONN_ID: conn_id})
                pipe.expire(key, self._TTL)

            common.redis.transaction(set_login_status, key)

    def ping(self, address: str, conn_id: str, context: str):
        logging.debug(f'{address} {conn_id}, {context}')
        d = json.loads(context)
        uid = d[const.CONTEXT_UID]
        common.redis.expire(self._key(uid), self._TTL)

    def disconnect(self, address: str, conn_id: str, context: str):
        logging.info(f'{address} {conn_id}, {context}')
        d = json.loads(context)
        if d:
            uid = d[const.CONTEXT_UID]
            key = self._key(uid)

            def unset_login_status(pipe: Pipeline):
                status = pipe.hgetall(key)
                if status.get(const.ONLINE_CONN_ID) == conn_id:
                    pipe.multi()
                    pipe.delete(key)

            common.redis.transaction(unset_login_status, key)


def main():
    parse_command_line()
    common.service.register(const.SERVICE_USER, f'{options.host}:{options.port}')
    common.service.start()

    handler = Handler()
    processor = user.Processor(handler)
    transport = TSocket.TServerSocket(options.host, options.port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    logging.info(f'Starting the server {options.host}:{options.port} ...')
    server.serve()


if __name__ == '__main__':
    main()
