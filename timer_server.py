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
import gevent
from generated.service import timer
import common
from typing import Dict
from redis.client import Pipeline
from utils import LogSuppress
from redis import Redis
import utils

define("host", utils.ip_address(), str, "listen host")
define("rpc_port", 0, int, "rpc port")

parse_command_line()


class Handler:
    _PREFIX = 'timer'
    _KEY = 'key'
    _SERVICE = 'service'
    _DATA = 'data'
    _DELAY = 'delay'
    _REPEAT = 'repeat'

    def __init__(self, redis: Redis):
        self._redis = redis

    @classmethod
    def key(cls, key):
        return f'{cls._PREFIX}:{key}'

    def call_later(self, key, service_name, data, delay, repeat):
        redis_key = self.key(key)
        self._redis.hmset(redis_key,
                          {self._KEY: key,
                           self._SERVICE: service_name,
                           self._DATA: data,
                           self._DELAY: delay,
                           self._REPEAT: repeat})

    def remove_timer(self, key):
        redis_key = self.key(key)
        self._redis.delete(redis_key)


def main():
    handler = Handler(common.redis)
    processor = timer.Processor(handler)
    transport = TSocket.TServerSocket(utils.addr_wildchar, options.rpc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    def register():
        options.rpc_port = transport.handle.getsockname()[1]
        logging.info(f'Starting the server {options.host}:{options.rpc_port} ...')
        common.registry.start({const.RPC_TIMER: f'{options.host}:{options.rpc_port}'})

    gevent.spawn_later(0.1, register)
    server.serve()


if __name__ == '__main__':
    main()
