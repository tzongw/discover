# -*- coding: utf-8 -*-
import logging
from common.mq_pb2 import Login, Logout, Alarm
from shared import timer_dispatcher, receiver, timer_service, const, at_exit, redis, registry, timer, invalidator, \
    async_task, app_id
from datetime import timedelta


@timer_dispatcher.handler('welcome')
def on_welcome(key, data):
    logging.info(f'got timer {key} {data}')


@timer_dispatcher.handler('notice')
def on_notice(key, data):
    logging.info(f'got timer {key} {data}')


@receiver.group(Login)
def on_login(id, data: Login):
    logging.info(f'{id} {data}')


@receiver.fanout(Logout)
def on_logout(id, data: Logout):
    logging.info(f'{id} {data}')


@receiver.group(Alarm)
def on_alarm(id, data: Alarm):
    logging.info(f'{id} {data}')


@invalidator.handler('session')
def session_invalidate(key):
    logging.info(key)


@async_task
def task(hello: str, repeat: int):
    logging.info(hello * repeat)
    async_task.cancel()


def init():
    if registry.addresses(const.RPC_TIMER):
        timer_service.call_later('notice:1', const.RPC_USER, 'one shot', delay=3)
        timer_service.call_repeat('welcome:2', const.RPC_USER, 'repeat', interval=5)
        at_exit(lambda: timer_service.remove_timer('welcome', const.RPC_USER))
    if redis.execute_command('MODULE LIST'):  # timer module loaded
        if redis.info('server')['redis_version'] == '255.255.255':
            timer.hint = app_id
        oneshot_id = timer.create(Alarm(tip='oneshot'), timedelta(seconds=2))
        at_exit(lambda: timer.kill(oneshot_id))
        loop_id = timer.create(Alarm(tip='loop'), timedelta(seconds=4), loop=True)
        at_exit(lambda: timer.kill(loop_id))
        task_id = async_task.post(task('hello', 3), timedelta(seconds=3), loop=True)
        at_exit(lambda: async_task.cancel(task_id))
        logging.info(f'{timer.exists(oneshot_id)}, {timer.exists(loop_id)}, {timer.exists(task_id)}')
