# -*- coding: utf-8 -*-
import logging
from datetime import timedelta
from typing import Callable
from dataclasses import dataclass
from contextlib import suppress
from redis import Redis
from redis.lock import Lock, LockError
from .task import AsyncTask
from .defer import deferrable, defer_if, Result


@dataclass
class Config:
    handler: Callable
    interval: timedelta
    poll: Callable
    batch: int


class Poller:
    def __init__(self, redis: Redis, async_task: AsyncTask, timeout=timedelta(minutes=1)):
        self.configs = {}  # type: dict[str, Config]
        self.async_task = async_task

        @async_task
        @deferrable
        def poll_task(group: str, queue: str):
            config = self.configs.get(group)
            if not config:
                logging.info(f'no config, quit {queue}')  # deploying? other apps will poll again
                return
            task = poll_task(group, queue)
            defer_if(Result.TRUE, lambda: async_task.publish(task))  # without lock
            with suppress(LockError), Lock(redis, f'lock:{queue}', timeout=timeout.total_seconds(), blocking=False):
                if jobs := config.poll(redis, queue, config.batch):
                    config.handler(*jobs)
                    return True  # notify next
                logging.debug(f'no jobs, stop {queue}')
                task_id = self.task_id(queue)
                async_task.cancel(task_id)
                if redis.exists(queue):  # race
                    logging.info(f'new jobs, restart {queue}')
                    async_task.post(task_id, task, config.interval, loop=True)
                    return True  # notify next

        self.poll_task = poll_task

    @staticmethod
    def task_id(queue: str):
        return f'poll:{queue}'

    def notify(self, group: str, queue: str):
        config = self.configs[group]
        task = self.poll_task(group, queue)
        self.async_task.post(self.task_id(queue), task, config.interval, loop=True)
        self.async_task.publish(task)

    def handler(self, group, interval=timedelta(seconds=1), *, poll=Redis.lpop, batch=100):
        def decorator(f):
            assert group not in self.configs
            self.configs[group] = Config(f, interval, poll, batch)
            return f

        return decorator
