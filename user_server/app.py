# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
from user_server.config import options
from user_server import shared, const, rpc
import time
import logging
import gevent
from user_server.shared import timer_dispatcher, app_name, app_id
from setproctitle import setproctitle
from common import mq_pb2


def init_timers():
    @timer_dispatcher.handler('welcome')
    def on_welcome(data):
        logging.info(f'got timer {data}')

    @timer_dispatcher.handler('notice')
    def on_notice(data):
        logging.info(f'got timer {data}')

    shared.timer_service.call_repeat('welcome', const.RPC_USER, 'welcome', 30)
    shared.timer_service.call_at('notice', const.RPC_USER, 'notice', time.time() + 10)
    shared.at_exit(lambda: shared.timer_service.remove_timer('welcome', const.RPC_USER))


def init_mq():
    from user_server.shared import receiver

    @receiver.group_handler(mq_pb2.Login)
    def on_login(id, data):
        logging.info(f'{id} {data}')

    @receiver.fanout_handler(f'{app_name}:logout')
    def on_logout(id, data):
        logging.info(f'{id} {data}')

    receiver.start()
    shared.at_exit(lambda: receiver.stop())


def main():
    logging.warning(f'app id: {app_id}')
    g = rpc.serve()
    shared.registry.start({const.RPC_USER: f'{options.host}:{options.rpc_port}'})
    setproctitle(f'{app_name}-{app_id}-{options.host}:{options.rpc_port}')
    init_timers()
    init_mq()
    gevent.joinall([g], raise_error=True)


if __name__ == '__main__':
    main()
