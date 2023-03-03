# -*- coding: utf-8 -*-
import logging
from datetime import timedelta
from enum import Enum, auto
from typing import Callable, Optional
from dataclasses import dataclass
from contextlib import suppress
from redis import Redis
from redis.lock import Lock, LockError
from .task import AsyncTask
from .defer import deferrable, defer_if, Result


@dataclass
class Config:
    poll: Callable
    interval: timedelta
    spawn: Optional[Callable]


class PollStatus(Enum):
    ASAP = auto()  # default
    YIELD = auto()
    DONE = auto()

    @classmethod
    def _missing_(cls, value):
        return cls.ASAP


class Poller:
    def __init__(self, redis: Redis, async_task: AsyncTask, timeout=timedelta(minutes=1)):
        self.configs = {}  # type: dict[str, Config]
        self.async_task = async_task

        @async_task
        def poll_task(group: str, queue: str):
            config = self.configs.get(group)
            if not config:
                logging.info(f'no config, quit {queue}')  # deploying? other apps will poll again
                return
            if spawn := config.spawn:
                spawn(do_poll, config, group, queue)
            else:
                do_poll(config, group, queue)

        @deferrable
        def do_poll(config: Config, group: str, queue: str):
            task = poll_task(group, queue)
            defer_if(Result.TRUE, lambda: async_task.publish(task))  # without lock, notify next if true
            with suppress(LockError), Lock(redis, f'lock:{queue}', timeout=timeout.total_seconds(), blocking=False):
                status = PollStatus(config.poll(queue))
                if status is not PollStatus.DONE:
                    return status is PollStatus.ASAP
                logging.debug(f'no jobs, stop {queue}')
                task_id = self.task_id(queue)
                async_task.cancel(task_id)
                status = PollStatus(config.poll(queue))
                if status is not PollStatus.DONE:  # race
                    logging.info(f'new jobs, restart {queue}')
                    async_task.post(task_id, task, config.interval, loop=True)
                    return status is PollStatus.ASAP

        self.poll_task = poll_task

    @staticmethod
    def task_id(queue: str):
        return f'poll:{queue}'

    def notify(self, group: str, queue: str):
        config = self.configs[group]
        task = self.poll_task(group, queue)
        self.async_task.post(self.task_id(queue), task, config.interval, loop=True)
        self.async_task.publish(task)

    def handler(self, group, interval=timedelta(seconds=1), spawn=None):
        def decorator(poll):
            assert group not in self.configs
            self.configs[group] = Config(poll, interval, spawn)
            return poll

        return decorator
