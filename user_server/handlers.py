# -*- coding: utf-8 -*-
import os
import logging
import const
from datetime import timedelta, datetime
from base import ip_address, LogSuppress
from base.scheduler import PeriodicCallback
from common.messages import Connect, Disconnect, Alarm
from shared import dispatcher, receiver, timer_service, at_exit, timer, invalidator, async_task, at_main, \
    time_dispatcher, run_in_worker, app_name, app_id, parser, redis, ztimer, scheduler, dispatch_timeout
from models import Runtime
from dao import Account
from config import options


@time_dispatcher('welcome')
def on_welcome(key, data):
    logging.info(f'got timer {key} {data}')


@time_dispatcher('notice')
def on_notice(key, data):
    logging.info(f'got timer {key} {data}')


def poll_timeout():
    for full_key, data in ztimer.poll().items():
        with LogSuppress():
            dispatch_timeout(full_key, data)


@dispatcher(Account)
def on_register(account: Account):
    logging.info(f'{account}')


@receiver(Connect)
def on_connect(data: Connect):
    logging.info(f'{data}')


@receiver(Disconnect)
def on_disconnect(data: Disconnect):
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


@time_dispatcher.periodic(timedelta(seconds=10))
def on_10s(dt: datetime):
    logging.info(f'tick {dt}')


@async_task
@run_in_worker
def task(hello: str, repeat: int, interval: timedelta):
    logging.info(f'{hello * repeat} {interval}')
    async_task.cancel('task:hello')


@time_dispatcher.crontab(second=range(0, 60, 15))
def on_quarter(dt: datetime):
    logging.info(f'quarter {dt}')


@at_main
def init():
    if options.init_timer == 'rpc':
        timer_service.call_later(const.RPC_USER, 'notice:1', 'one shot', delay=3)
        timer_service.call_repeat(const.RPC_USER, 'welcome:2', 'repeat', interval=5)
        at_exit(lambda: timer_service.remove_timer(const.RPC_USER, 'welcome:2'))
        timer_service.call_repeat(const.RPC_USER, const.TICK_TIMER, '', interval=1)
        at_exit(lambda: timer_service.remove_timer(const.RPC_USER, const.TICK_TIMER))
    elif options.init_timer == 'ztimer':
        ztimer.new('notice:1', 'one shot', timedelta(seconds=3))
        ztimer.new('welcome:2', 'repeat', timedelta(seconds=5), loop=True)
        at_exit(lambda: ztimer.kill('welcome:2'))
        ztimer.new(const.TICK_TIMER, '', timedelta(seconds=1), loop=True)
        at_exit(lambda: ztimer.kill(const.TICK_TIMER))
        pc = PeriodicCallback(scheduler, poll_timeout, timedelta(seconds=1))
        at_exit(pc.stop)
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
