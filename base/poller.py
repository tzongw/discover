# -*- coding: utf-8 -*-
import logging
from datetime import timedelta
from typing import Callable, Union
from dataclasses import dataclass
from contextlib import suppress
from redis import Redis
from redis.lock import Lock, LockError
from .task import AsyncTask


@dataclass
class Config:
    handler: Callable
    interval: Union[int, timedelta]
    poll: Callable
    batch: int


class Poller:
    def __init__(self, redis: Redis, async_task: AsyncTask, timeout=timedelta(minutes=1)):
        self.configs = {}
        self.async_task = async_task

        @async_task
        def poll_task(queue: str):
            with suppress(LockError), Lock(redis, f'lock:{queue}', timeout=timeout.total_seconds(), blocking=False):
                config = self.configs[queue]
                if jobs := config.poll(redis, queue, config.batch):
                    config.handler(*jobs)
                    async_task.publish()
                    return
                logging.debug(f'no jobs, quit {queue}')
                async_task.cancel()
                if redis.exists(queue):  # race
                    logging.debug(f'new jobs, restart {queue}')
                    async_task.post(async_task.current_task, config.interval, loop=True, do_hint=False)

        self.poll_task = poll_task

    def notify(self, queue: str):
        config = self.configs[queue]
        task = self.poll_task(queue)
        self.async_task.post(task, config.interval, loop=True, do_hint=False)
        self.async_task.publish(task)

    def handler(self, queue, interval: Union[int, timedelta] = timedelta(seconds=1), *, poll=Redis.lpop, batch=100):
        def decorator(f):
            assert queue not in self.configs
            self.configs[queue] = Config(f, interval, poll, batch)
            return f

        return decorator

    def __contains__(self, queue: str):
        return queue in self.configs
