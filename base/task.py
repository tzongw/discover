# -*- coding: utf-8 -*-
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from importlib import import_module
from typing import TypeVar, Callable, Optional
import functools
import gevent
from redis import Redis, RedisCluster
from pydantic import BaseModel
from yaml import safe_dump as dumps
from yaml import safe_load as loads
from .mq import Receiver, Publisher
from .timer import Timer
from .utils import var_args, func_desc, stream_name


class Task(BaseModel):
    path: str
    args: str = dumps(())
    kwargs: str = dumps({})


@dataclass
class Info:
    remaining: timedelta
    interval: timedelta
    loop: bool


F = TypeVar('F', bound=Callable)
TASK_THRESHOLD = 16384


class _BaseTask:
    def __init__(self):
        self.paths = {}

    def add_path(self, f):
        path = func_desc(f)
        assert '<' not in path, 'CAN NOT be lambda or local function'
        assert path not in self.paths, 'duplicated path'
        self.paths[path] = var_args(f)
        return path


class AsyncTask(_BaseTask):
    """
    Make sure new version handler is compatible with old version arguments, that means:
    1. can not remove an argument, instead add a new handler
    2. add new argument at the end and set a default value
    """

    def __init__(self, timer: Timer, publisher: Publisher, receiver: Receiver, maxlen=4096):
        assert timer.redis is publisher.redis is receiver.redis
        super().__init__()
        self.timer = timer
        self.receiver = receiver
        self.publisher = publisher
        self.maxlen = maxlen

    @staticmethod
    def stream_name(task: Task):
        return f'{stream_name(task)}:{task.path}'

    def __call__(self, f: F) -> F:
        path = self.add_path(f)
        stream = self.stream_name(Task(path=path))
        vf = var_args(f)

        @self.receiver(Task, stream=stream)
        def handler(task: Task):
            args = loads(task.args)  # type: list
            kwargs = loads(task.kwargs)  # type: dict
            vf(*args, **kwargs)

        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> Task:
            task = Task(path=path, args=dumps(args), kwargs=dumps(kwargs))
            if len(task.args) + len(task.kwargs) > TASK_THRESHOLD:
                logging.warning(f'task parameters too big {task}')
            return task

        wrapper.__task_wrapped__ = f
        return wrapper

    def post(self, task_id: str, task: Task, interval: timedelta, *, loop=False):
        stream = self.stream_name(task)
        return self.timer.create(task_id, task, interval, loop=loop, maxlen=self.maxlen, stream=stream)

    def cancel(self, task_id: str):
        return self.timer.kill(task_id)

    def exists(self, task_id: str):
        return self.timer.exists(task_id)

    def info(self, task_id: str) -> Optional[Info]:
        res = self.timer.info(task_id)
        if res is None:
            return
        return Info(remaining=timedelta(milliseconds=res['remaining']),
                    interval=timedelta(milliseconds=res['interval']),
                    loop=res['loop'])

    def publish(self, task):
        stream = self.stream_name(task)
        self.publisher.publish(task, maxlen=self.maxlen, stream=stream)


class HeavyTask(_BaseTask):
    def __init__(self, redis: Redis | RedisCluster, key: str):
        super().__init__()
        self.redis = redis
        self._key = key
        self._waker = f'waker:{{{key}}}:{uuid.uuid4()}'
        self._stopped = True

    def __call__(self, f: F) -> F:
        path = self.add_path(f)
        assert not path.startswith('__main__'), '__main__ is different in another process'

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            task = Task(path=path, args=dumps(args), kwargs=dumps(kwargs))
            if len(task.args) + len(task.kwargs) > TASK_THRESHOLD:
                logging.warning(f'task parameters too big {task}')
            self.push(task)

        wrapper.__task_wrapped__ = f
        return wrapper

    def push(self, task: Task, front=False):
        value = task.json(exclude_defaults=True)
        if front:
            total = self.redis.lpush(self._key, value)
        else:
            total = self.redis.rpush(self._key, value)
        logging.info(f'+task {task} total {total}')

    def start(self, exec_func=None):
        logging.info(f'start {self._key}')
        self._stopped = False
        return [gevent.spawn(self._run, exec_func or self.exec, self._key, self._waker)]

    def stop(self):
        if self._stopped:
            return
        logging.info(f'stop {self._key}')
        self._stopped = True
        with self.redis.pipeline(transaction=True, shard_hint=self._waker) as pipe:
            pipe.rpush(self._waker, 'wake up')
            pipe.expire(self._waker, 10)
            pipe.execute()

    def _run(self, exec_func, key, waker):
        while not self._stopped:
            try:
                r = self.redis.blpop([key, waker])
                if r is None or r[0] == waker:
                    continue
                task = Task.parse_raw(r[1])
                exec_func(task)
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)

    def exec(self, task: Task):
        logging.info(f'doing task {task}')
        func = self.paths.get(task.path)
        if func is None:
            logging.info(f'adding path {task.path}')
            module_name, func_name = task.path.rsplit('.', maxsplit=1)
            import_module(module_name)  # will auto add_path
            func = self.paths[task.path]
        args = loads(task.args)  # type: list
        kwargs = loads(task.kwargs)  # type: dict
        start = time.time()
        r = func(*args, **kwargs)
        logging.info(f'done task {task} {time.time() - start}')
        return r
