# -*- coding: utf-8 -*-
import logging
import const
from common.messages import Login, Logout, Alarm
from shared import dispatcher, receiver, timer_service, at_exit, redis, registry, timer, invalidator, \
    async_task, at_main, tick
from datetime import timedelta
from dao import Account


@dispatcher.handler('welcome')
def on_welcome(key, data):
    logging.info(f'got timer {key} {data}')


@dispatcher.handler('notice')
def on_notice(key, data):
    logging.info(f'got timer {key} {data}')


@dispatcher.handler(Account)
def on_register(account: Account):
    logging.info(f'{account}')


@receiver.group(Login)
def on_login(data: Login):
    logging.info(f'{data}')


@receiver.fanout(Logout)
def on_logout(data: Logout):
    logging.info(f'{data}')


@receiver.group(Alarm)
def on_alarm(data: Alarm):
    logging.info(f'{data}')


@invalidator.handler('session')
def session_invalidate(key):
    logging.info(key)


@tick.handler(timedelta(seconds=10))
def on_10s():
    logging.info('tick')


@async_task
def task(hello: str, repeat: int, interval: timedelta):
    logging.info(f'{hello * repeat} {interval}')
    async_task.cancel()


@at_main
def init():
    if registry.addresses(const.RPC_TIMER):
        timer_service.call_later('notice:1', const.RPC_USER, 'one shot', delay=3)
        timer_service.call_repeat('welcome:2', const.RPC_USER, 'repeat', interval=5)
        at_exit(lambda: timer_service.remove_timer('welcome:2', const.RPC_USER))
    if redis.execute_command('MODULE LIST'):  # timer module loaded
        oneshot_id = timer.create(Alarm(tip='oneshot'), timedelta(seconds=2))
        at_exit(lambda: timer.kill(oneshot_id))
        loop_id = timer.create(Alarm(tip='loop'), timedelta(seconds=4), loop=True)
        at_exit(lambda: timer.kill(loop_id))
        task_id = async_task.post(task('hello', 3, timedelta(seconds=1)), timedelta(seconds=3), loop=True)
        at_exit(lambda: async_task.cancel(task_id))
        logging.info(timer.info(oneshot_id))
        logging.info(timer.info(loop_id))
        logging.info(timer.info(task_id))
        timer.tick(const.TICK_TIMER, timedelta(seconds=1), const.TICK_TS, const.TICK_STREAM)
        at_exit(lambda: timer.kill(const.TICK_TIMER))
