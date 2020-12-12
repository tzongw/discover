# -*- coding: utf-8 -*-
import logging
from common import mq_pb2
from common.timer import Timer
from .shared import timer_dispatcher, receiver, timer_service, const, at_exit, redis, registry


@timer_dispatcher.handler('welcome')
def on_welcome(data):
    logging.info(f'got timer {data}')


@timer_dispatcher.handler('notice')
def on_notice(data):
    logging.info(f'got timer {data}')


@receiver.group_handler(mq_pb2.Login)
def on_login(id, data: mq_pb2.Login):
    logging.info(f'{id} {data}')


@receiver.fanout_handler(mq_pb2.Logout)
def on_logout(id, data: mq_pb2.Logout):
    logging.info(f'{id} {data}')


@receiver.group_handler(mq_pb2.Alarm)
def on_alarm(id, data: mq_pb2.Alarm):
    logging.info(f'{id} {data}')


def init():
    if registry.addresses(const.RPC_TIMER):
        timer_service.call_later('notice', const.RPC_USER, 'notice', delay=10)
        timer_service.call_repeat('welcome', const.RPC_USER, 'welcome', interval=30)
        at_exit(lambda: timer_service.remove_timer('welcome', const.RPC_USER))
    timer = Timer(redis)
    timer.new_stream_timer(mq_pb2.Alarm(tip='one shot'), interval=1000)
    tid = timer.new_stream_timer(mq_pb2.Alarm(tip='loop'), interval=5000, loop=True)
    at_exit(lambda: timer.kill(tid))
