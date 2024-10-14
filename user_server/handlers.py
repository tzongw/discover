# -*- coding: utf-8 -*-
import os
import logging
import const
from datetime import timedelta, datetime
from base import ip_address
from common.messages import Login, Logout, Alarm
from shared import dispatcher, receiver, timer_service, at_exit, timer, invalidator, async_task, at_main, tick, \
    run_in_worker, app_name, app_id, parser, redis, ztimer, scheduler
from models import Runtime
from dao import Account
from config import options


@dispatcher('welcome')
def on_welcome(key, data):
    logging.info(f'got timer {key} {data}')


@dispatcher('notice')
def on_notice(key, data):
    logging.info(f'got timer {key} {data}')


@scheduler(timedelta(seconds=1))
def poll_timeout():
    for full_key, data in ztimer.poll().items():
        group, key = full_key.split(':', maxsplit=1)
        dispatcher.dispatch(group, key, data)


at_exit(poll_timeout.stop)


@dispatcher(Account)
def on_register(account: Account):
    logging.info(f'{account}')


@receiver(Login)
def on_login(data: Login):
    logging.info(f'{data}')


@receiver(Logout)
def on_logout(data: Logout):
    logging.info(f'{data}')


@receiver(Alarm)
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
    if options.init_timer == 'rpc':
        timer_service.call_later(const.RPC_USER, 'notice:1', 'one shot', delay=3)
        timer_service.call_repeat(const.RPC_USER, 'welcome:2', 'repeat', interval=5)
        at_exit(lambda: timer_service.remove_timer(const.RPC_USER, 'welcome:2'))
    elif options.init_timer == 'ztimer':
        ztimer.new('notice:1', 'one shot', timedelta(seconds=3))
        ztimer.new('welcome:2', 'repeat', timedelta(seconds=5), loop=True)
        at_exit(lambda: ztimer.kill('welcome:2'))
    elif options.init_timer == 'task':
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
