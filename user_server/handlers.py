# -*- coding: utf-8 -*-
import logging
from common.mq_pb2 import Login, Logout, Alarm
from shared import timer_dispatcher, receiver, timer_service, const, at_exit, redis, registry, timer, invalidator
from datetime import timedelta


@timer_dispatcher.handler('welcome')
def on_welcome(data):
    logging.info(f'got timer {data}')


@timer_dispatcher.handler('notice')
def on_notice(data):
    logging.info(f'got timer {data}')


@receiver.group(Login)
def on_login(id, data: Login):
    logging.info(f'{id} {data}')


@receiver.fanout(Logout)
def on_logout(id, data: Logout):
    logging.info(f'{id} {data}')


@receiver.group(Alarm)
def on_alarm(id, data: Alarm):
    logging.info(f'{id} {data}')


@invalidator.handler('session:')
def session_invalidate(key):
    logging.info(key)


def init():
    if registry.addresses(const.RPC_TIMER):
        timer_service.call_later('notice', const.RPC_USER, 'notice', delay=10)
        timer_service.call_repeat('welcome', const.RPC_USER, 'welcome', interval=30)
        at_exit(lambda: timer_service.remove_timer('welcome', const.RPC_USER))
    if redis.execute_command('MODULE LIST'):  # timer module loaded
        timer.new_stream_timer(Alarm(tip='one shot'), timedelta(seconds=10))
        tid = timer.new_stream_timer(Alarm(tip='loop'), timedelta(seconds=20), loop=True)
        at_exit(lambda: timer.kill(tid))
