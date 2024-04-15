# -*- coding: utf-8 -*-
import os
import logging
import const
from datetime import timedelta, datetime
from base import ip_address
from common.messages import Login, Logout, Alarm
from shared import dispatcher, receiver, timer_service, at_exit, registry, timer, invalidator, \
    async_task, at_main, tick, run_in_worker, app_name, app_id, parser, redis
from models import Runtime
from dao import Account


@dispatcher('welcome')
def on_welcome(key, data):
    logging.info(f'got timer {key} {data}')


@dispatcher('notice')
def on_notice(key, data):
    logging.info(f'got timer {key} {data}')


@dispatcher(Account)
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


@invalidator('session')
def session_invalidate(key):
    logging.info(key)


@invalidator('runtime')
def runtime_invalidate(key):
    if key != f'{app_name}:{app_id}':
        return
    runtime = parser.hget(f'runtime:{key}', Runtime)
    if not runtime:
        return
    logging.getLogger().setLevel(runtime.log_level)


@tick.periodic(timedelta(seconds=10))
def on_10s(dt: datetime):
    logging.info(f'tick {dt}')


@async_task
@run_in_worker
def task(hello: str, repeat: int, interval: timedelta):
    logging.info(f'{hello * repeat} {interval}')
    async_task.cancel('task:hello')


@tick.crontab(second=range(0, 60, 15))
def on_quarter(dt: datetime):
    logging.info(f'quarter {dt}')


@at_main
def init():
    if registry.addresses(const.RPC_TIMER):
        timer_service.call_later('notice:1', const.RPC_USER, 'one shot', delay=3)
        timer_service.call_repeat('welcome:2', const.RPC_USER, 'repeat', interval=5)
        at_exit(lambda: timer_service.remove_timer('welcome:2', const.RPC_USER))

    oneshot_id = 'timer:oneshot'
    timer.create(oneshot_id, Alarm(tip='oneshot'), timedelta(seconds=2))
    at_exit(lambda: timer.kill(oneshot_id))
    loop_id = 'timer:loop'
    timer.create(loop_id, Alarm(tip='loop'), timedelta(seconds=8), loop=True)
    at_exit(lambda: timer.kill(loop_id))
    task_id = 'task:hello'
    async_task.post(task_id, task('hello', 3, timedelta(seconds=1)), timedelta(seconds=3), loop=True)
    at_exit(lambda: async_task.cancel(task_id))
    logging.info(timer.info(oneshot_id))
    logging.info(timer.info(loop_id))
    logging.info(timer.info(task_id))
    timer.tick(const.TICK_TIMER, const.TICK_STREAM)
    at_exit(lambda: timer.kill(const.TICK_TIMER))
    log_level = logging.getLevelName(logging.getLogger().getEffectiveLevel())
    runtime = Runtime(address=ip_address(), pid=os.getpid(), log_level=log_level)
    key = f'runtime:{app_name}:{app_id}'
    parser.hset(key, runtime)
    at_exit(lambda: redis.delete(key))
