# -*- coding: utf-8 -*-
import logging
import time
from .shared import timer_dispatcher, app_name, receiver, timer_service, const, at_exit
from common import mq_pb2


@timer_dispatcher.handler('welcome')
def on_welcome(data):
    logging.info(f'got timer {data}')


@timer_dispatcher.handler('notice')
def on_notice(data):
    logging.info(f'got timer {data}')


@receiver.group_handler(mq_pb2.Login)
def on_login(id, data):
    logging.info(f'{id} {data}')


@receiver.fanout_handler(f'{app_name}:logout')
def on_logout(id, data):
    logging.info(f'{id} {data}')


def init():
    timer_service.call_repeat('welcome', const.RPC_USER, 'welcome', 30)
    timer_service.call_at('notice', const.RPC_USER, 'notice', time.time() + 10)
    at_exit(lambda: timer_service.remove_timer('welcome', const.RPC_USER))
