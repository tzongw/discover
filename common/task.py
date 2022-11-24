# -*- coding: utf-8 -*-
import json
import logging
from typing import TypeVar, Callable
from gevent.local import local
from base.mq import Receiver, Publisher
from base.timer import Timer
from base.utils import timer_name, var_args
from .mq_pb2 import Task


class AsyncTask:
    """
    Make sure new version handler is compatible with old version arguments, that means:
    1. can not remove an argument, instead add a new handler
    2. add new argument at the end and set a default value
    """
    F = TypeVar('F', bound=Callable)

    def __init__(self, timer: Timer, receiver: Receiver, maxlen=4096):
        assert timer.redis is receiver.redis
        self.timer = timer
        self.receiver = receiver
        self.publisher = Publisher(receiver.redis, hint=timer.hint)
        self.maxlen = maxlen
        self.local = local()

    @staticmethod
    def stream_name(task: Task):
        from base.utils import stream_name
        return f'{stream_name(task)}:{task.path}'

    def __call__(self, f: F) -> F:
        path = f'{f.__module__}.{f.__name__}'
        stream = self.stream_name(Task(path=path))
        vf = var_args(f)

        @self.receiver.group(Task, stream)
        def handler(id, task: Task):
            logging.debug(f'got task {id} {task.id} {task.path}')
            args = json.loads(task.args)  # type: list
            kwargs = json.loads(task.kwargs)  # type: dict
            self.local.task = task

            try:
                vf(*args, **kwargs)
            finally:
                del self.local.task

        def wrapper(*args, **kwargs) -> Task:
            task = Task(path=path, args=json.dumps(args), kwargs=json.dumps(kwargs))
            task.id = f'{timer_name(task)}:{task.path}:{task.args}:{task.kwargs}'
            return task

        return wrapper

    def post(self, task: Task, interval, loop=False, do_hint=True):
        stream = self.stream_name(task)
        return self.timer.create(task, interval, loop, key=task.id, maxlen=self.maxlen, do_hint=do_hint, stream=stream)

    @property
    def current_task(self):
        return self.local.task

    def cancel(self, task_id=None):
        return self.timer.kill(task_id or self.current_task.id)

    def publish(self, task=None, do_hint=True):
        task = task or self.current_task
        stream = self.stream_name(task)
        self.publisher.publish(task, maxlen=self.maxlen, do_hint=do_hint, stream=stream)
