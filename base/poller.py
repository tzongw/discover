# -*- coding: utf-8 -*-
import logging
from datetime import timedelta
from enum import Enum, auto
from typing import Callable, Optional
from dataclasses import dataclass
from contextlib import suppress, ExitStack
from redis import Redis
from redis.lock import Lock
from redis.exceptions import LockError
from .task import AsyncTask, Task
from .utils import var_args


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
        def poll_task(group: str, queue: str) -> Task | None:
            config = self.configs.get(group)
            if not config:
                logging.info(f'no config, quit {queue}')  # deploying? other apps will poll again
                return
            if spawn := config.spawn:
                spawn(do_poll, config, group, queue)
            else:
                do_poll(config, group, queue)

        def do_poll(config: Config, group: str, queue: str):
            task = poll_task(group, queue)
            lock = Lock(redis, f'lock:{group}:{queue}', timeout=timeout.total_seconds(), blocking=False)
            with ExitStack() as stack, suppress(LockError), lock:
                status = PollStatus(config.poll(queue))
                stack.callback(lambda: status is PollStatus.ASAP and async_task.publish(task))  # without lock
                if status is not PollStatus.DONE:
                    return
                logging.debug(f'no jobs, stop {queue}')
                task_id = self._task_id(group, queue)
                async_task.cancel(task_id)
                status = PollStatus(config.poll(queue))
                if status is not PollStatus.DONE:  # race
                    logging.info(f'new jobs, restart {queue}')
                    async_task.post(task_id, task, config.interval, loop=True)

        self.poll_task = poll_task

    @staticmethod
    def _task_id(group: str, queue: str):
        return f'poll:{group}:{queue}'

    def notify(self, group: str, queue=''):
        task_id = self._task_id(group, queue)
        if self.async_task.exists(task_id):
            return
        config = self.configs[group]
        task = self.poll_task(group, queue)
        if self.async_task.post(task_id, task, config.interval, loop=True):
            self.async_task.publish(task)

    def __call__(self, group, interval=timedelta(seconds=1), spawn=None):
        def decorator(poll):
            assert group not in self.configs
            self.configs[group] = Config(var_args(poll), interval, spawn)
            return poll

        return decorator
