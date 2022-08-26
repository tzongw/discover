# -*- coding: utf-8 -*-
import json
import logging
from collections import namedtuple
from inspect import signature, Parameter
from typing import TypeVar, Callable

from gevent.local import local
from base.mq import Receiver, Publisher
from base.timer import Timer
from base.utils import stream_name, timer_name
from .mq_pb2 import Task


class AsyncTask:
    """
    Make sure new version handler is compatible with old version arguments, that means:
    1. can not remove an argument, instead add a new handler
    2. add new argument at the end and set a default value
    """
    Handler = namedtuple('Handler', ['func', 'params'])
    F = TypeVar('F', bound=Callable)

    def __init__(self, timer: Timer, receiver: Receiver, maxlen=16384):
        assert timer.redis is receiver.redis
        self.timer = timer
        self.receiver = receiver
        self.publisher = Publisher(receiver.redis, hint=timer.hint)
        self.maxlen = maxlen
        self.handlers = {}
        self.local = local()

        @receiver.group(Task)
        def handler(id, task: Task):
            logging.debug(f'got task {id} {task.id} {task.path}')
            h = self.handlers.get(task.path)
            if not h:
                task.ttl -= 1
                logging.warning(f'can not handle {id} {task.id} {task.path} {task.ttl}')
                if task.ttl > 0:
                    # throw back task, let other process handle it
                    self.publish(task, do_hint=False)
                return
            args = json.loads(task.args)  # type: list
            kwargs = json.loads(task.kwargs)  # type: dict
            self.local.task = task

            try:
                params = h.params
                func = h.func
                if not any(p.kind == Parameter.VAR_POSITIONAL for p in params.values()):
                    args = args[:len(params)]
                if not any(p.kind == Parameter.VAR_KEYWORD for p in params.values()):
                    kwargs = {k: v for k, v in kwargs.items() if k in params}
                func(*args, **kwargs)
            finally:
                del self.local.task

    def __call__(self, f: F) -> F:
        path = f'{f.__module__}.{f.__name__}'
        self.handlers[path] = AsyncTask.Handler(f, signature(f).parameters)

        def wrapper(*args, **kwargs) -> Task:
            task = Task(path=path, args=json.dumps(args), kwargs=json.dumps(kwargs), ttl=10)
            task.id = f'{timer_name(task)}:{task.path}:{task.args}:{task.kwargs}'
            return task

        return wrapper

    def post(self, task: Task, interval, loop=False, do_hint=True):
        return self.timer.create(task, interval, loop, key=task.id, maxlen=self.maxlen, do_hint=do_hint)

    @property
    def current_task(self):
        return self.local.task

    def cancel(self, task_id=None):
        return self.timer.kill(task_id or self.current_task.id)

    def publish(self, task=None, do_hint=True):
        self.publisher.publish(task or self.current_task, maxlen=self.maxlen,
                               do_hint=do_hint)
