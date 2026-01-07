# -*- coding: utf-8 -*-
import uuid
import logging
import functools
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from importlib import import_module
from typing import Optional
import gevent
from redis import Redis, RedisCluster
from pydantic import BaseModel
from yaml import safe_dump as dumps
from yaml import safe_load as loads
from .mq import Consumer, Producer
from .timer import Timer
from .utils import variadic_args, func_desc, stream_name


class Task(BaseModel):
    path: str
    args: str = dumps(())
    kwargs: str = dumps({})


@dataclass(frozen=True)
class Info:
    remaining: timedelta
    interval: timedelta
    loop: bool


TASK_THRESHOLD = 16384


class _BaseTask:
    def __init__(self):
        self.paths = {}

    def add_path(self, f):
        path = func_desc(f)
        assert '<' not in path, 'CAN NOT be lambda or local function'
        assert path not in self.paths, 'duplicated path'
        self.paths[path] = variadic_args(f)
        return path


class AsyncTask(_BaseTask):
    """
    Make sure new version handler is compatible with old version arguments, that means:
    1. can not remove an argument, instead add a new handler
    2. add new argument at the end and set a default value
    """

    def __init__(self, timer: Timer, producer: Producer, consumer: Consumer):
        assert timer.redis is producer.redis is consumer.redis
        super().__init__()
        self.timer = timer
        self.producer = producer
        self.consumer = consumer

    @staticmethod
    def stream_name(task: Task):
        return f'{stream_name(task)}:{task.path}'

    def __call__(self, f):
        path = self.add_path(f)
        vf = self.paths[path]
        stream = self.stream_name(Task(path=path))

        @self.consumer(Task, stream=stream)
        def handler(task: Task):
            args = loads(task.args)
            kwargs = loads(task.kwargs)
            vf(*args, **kwargs)

        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> Task:
            task = Task(path=path, args=dumps(args), kwargs=dumps(kwargs))
            if len(task.args) + len(task.kwargs) > TASK_THRESHOLD:
                logging.warning(f'task parameters too big {task}')
            return task

        wrapper.__task_wrapped__ = f
        return wrapper

    def create(self, task_id: str, task: Task, interval: timedelta, *, loop=False):
        stream = self.stream_name(task)
        return self.timer.create(task_id, task, interval, loop=loop, stream=stream)

    def cancel(self, task_id: str):
        return self.timer.kill(task_id)

    def exists(self, task_id: str):
        return self.timer.exists(task_id)

    def info(self, task_id: str) -> Optional[Info]:
        res = self.timer.info(task_id)
        if res is None:
            return None
        return Info(remaining=timedelta(milliseconds=res['remaining']),
                    interval=timedelta(milliseconds=res['interval']),
                    loop=res['loop'])

    def post(self, task):
        stream = self.stream_name(task)
        self.producer.post(task, stream=stream)


class Priority(StrEnum):
    HIGH = 'high'
    DEFAULT = 'default'
    LOW = 'low'


class HeavyTask(_BaseTask):
    def __init__(self, redis: Redis | RedisCluster, biz: str):
        super().__init__()
        self.redis = redis
        self._key = f'heavy_task:queue:{{{biz}}}'
        self._waker = f'heavy_task:waker:{{{biz}}}:{uuid.uuid4()}'
        self._stopped = True
        self._priorities = {}

    def __call__(self, func=None, *, priority=Priority.DEFAULT):
        def decorator(f):
            path = self.add_path(f)
            self._priorities[path] = priority
            assert not path.startswith('__main__'), '__main__ is different in another process'

            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                task = Task(path=path, args=dumps(args), kwargs=dumps(kwargs))
                if len(task.args) + len(task.kwargs) > TASK_THRESHOLD:
                    logging.warning(f'task parameters too big {task}')
                self.push(task)

            wrapper.__task_wrapped__ = f
            return wrapper

        return decorator(func) if func else decorator

    def _get_queue(self, task):
        priority = self._priorities[task.path]
        return f'{self._key}:{priority}'

    def push(self, task: Task):
        value = task.json(exclude_defaults=True)
        queue = self._get_queue(task)
        total = self.redis.rpush(queue, value)
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
        with self.redis.pipeline(transaction=False) as pipe:
            pipe.rpush(self._waker, 'wake up')
            pipe.delete(self._waker)
            pipe.execute()

    def _run(self, exec_func, key, waker):
        queues = [f'{key}:{priority}' for priority in Priority] + [waker]
        while not self._stopped:
            try:
                r = self.redis.blpop(queues)
                if r is None or r[0] == waker:
                    continue
                task = Task.parse_raw(r[1])
                exec_func(task)
            except Exception:
                logging.exception(f'')
                gevent.sleep(1)

    def _get_func(self, path):
        func = self.paths.get(path)
        if func is None:
            logging.info(f'adding path {path}')
            module_name, func_name = path.rsplit('.', maxsplit=1)
            import_module(module_name)  # will auto add_path
            func = self.paths[path]
        return func

    def exec(self, task: Task):
        func = self._get_func(task.path)
        args = loads(task.args)
        kwargs = loads(task.kwargs)
        return func(*args, **kwargs)
