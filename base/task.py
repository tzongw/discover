# -*- coding: utf-8 -*-
import logging
import time
from importlib import import_module
from typing import TypeVar, Callable
import functools
from gevent.local import local
from redis import Redis
from .mq import Receiver, Publisher
from .timer import Timer
from .utils import timer_name, var_args
from pydantic import BaseModel
from yaml import safe_dump as dumps
from yaml import safe_load as loads


class Task(BaseModel):
    path: str
    args: str = dumps(())
    kwargs: str = dumps({})


F = TypeVar('F', bound=Callable)
TASK_THRESHOLD = 16384


class _BaseTask:
    def __init__(self):
        self.paths = set()

    def validate(self, f):
        path = f'{f.__module__}.{f.__name__}'
        assert path not in self.paths
        self.paths.add(path)
        return path


class AsyncTask(_BaseTask):
    """
    Make sure new version handler is compatible with old version arguments, that means:
    1. can not remove an argument, instead add a new handler
    2. add new argument at the end and set a default value
    """

    def __init__(self, timer: Timer, receiver: Receiver, maxlen=4096):
        assert timer.redis is receiver.redis
        super().__init__()
        self.timer = timer
        self.receiver = receiver
        self.publisher = Publisher(receiver.redis, hint=timer.hint)
        self.maxlen = maxlen
        self.local = local()

    @staticmethod
    def stream_name(task: Task):
        from .utils import stream_name
        return f'{stream_name(task)}:{task.path}'

    @staticmethod
    def task_id(task: Task):
        return f'{timer_name(Task)}:{task}'

    def __call__(self, f: F) -> F:
        path = self.validate(f)
        stream = self.stream_name(Task(path=path))
        vf = var_args(f)

        @self.receiver.group(Task, stream)
        def handler(task: Task):
            args = loads(task.args)  # type: list
            kwargs = loads(task.kwargs)  # type: dict
            self.local.task = task

            try:
                vf(*args, **kwargs)
            finally:
                del self.local.task

        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> Task:
            task = Task(path=path, args=dumps(args), kwargs=dumps(kwargs))
            if len(task.args) + len(task.kwargs) > TASK_THRESHOLD:
                logging.warning(f'task parameters too big {task}')
            return task

        wrapper.wrapped = f
        return wrapper

    def post(self, task: Task, interval, *, loop=False, task_id=None, do_hint=True):
        key = task_id or self.task_id(task)
        stream = self.stream_name(task)
        return self.timer.create(task, interval, loop=loop, key=key, maxlen=self.maxlen, do_hint=do_hint, stream=stream)

    @property
    def current_task(self):
        return self.local.task

    def cancel(self, task_or_id=None):
        task_id = self.task_id(task_or_id) if isinstance(task_or_id, Task) else task_or_id
        return self.timer.kill(task_id or self.task_id(self.current_task))

    def publish(self, task=None, do_hint=True):
        task = task or self.current_task
        stream = self.stream_name(task)
        self.publisher.publish(task, maxlen=self.maxlen, do_hint=do_hint, stream=stream)


class HeavyTask(_BaseTask):
    def __init__(self, redis: Redis, key: str):
        super().__init__()
        self.redis = redis
        self.key = key

    def __call__(self, f: F) -> F:
        path = self.validate(f)
        assert not path.startswith('__main__')  # __main__ is different in another process

        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> Task:
            task = Task(path=path, args=dumps(args), kwargs=dumps(kwargs))
            if len(task.args) + len(task.kwargs) > TASK_THRESHOLD:
                logging.warning(f'task parameters too big {task}')
            return task

        wrapper.wrapped = f
        return wrapper

    def push(self, task: Task):
        logging.info(f'+task {task}')
        self.redis.rpush(self.key, task.json())

    def pop(self, *, timeout=0, block=True):
        r = self.redis.blpop(self.key, timeout) if block else self.redis.lpop(self.key)
        if r is None:
            return
        value = r[1] if isinstance(r, (list, tuple)) else r
        return self.parse(value)

    @staticmethod
    def parse(value):
        task = Task.parse_raw(value)
        return task

    @staticmethod
    def exec(task: Task):
        logging.info(f'doing task {task}')
        index = task.path.rindex('.')
        module = import_module(task.path[:index])
        func = getattr(module, task.path[index + 1:]).wrapped
        args = loads(task.args)  # type: list
        kwargs = loads(task.kwargs)  # type: dict
        start = time.time()
        r = func(*args, **kwargs)
        logging.info(f'done task {task} {time.time() - start}')
        return r
