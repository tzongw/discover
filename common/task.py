# -*- coding: utf-8 -*-
import pickle
import uuid
import logging
from base.mq import Receiver
from base.timer import Timer
from .mq_pb2 import Task
from gevent.local import local


class AsyncTask:
    def __init__(self, timer: Timer, maxlen=16384):
        self.timer = timer
        self.maxlen = maxlen
        self.handlers = {}
        self.task_data = local()

    def register(self, receiver: Receiver):
        @receiver.group(Task)
        def handler(id, task: Task):
            logging.debug(f'got task {id} {task.task_id}')
            args = pickle.loads(task.args)
            kwargs = pickle.loads(task.kwargs)
            f = self.handlers[task.path]
            self.task_data.task_id = task.task_id
            f(*args, **kwargs)
            self.task_data.task_id = None

    def __call__(self, f):
        path = f'{f.__module__ }.{f.__name__}'
        self.handlers[path] = f

        def wrapper(*args, **kwargs):
            task = Task(task_id=str(uuid.uuid4()), path=path, args=pickle.dumps(args), kwargs=pickle.dumps(kwargs))
            return task

        return wrapper

    def post(self, task: Task, interval, loop=False, task_id=None):
        if task_id:
            task.task_id = task_id
        return self.timer.create(task, interval, loop, key=task.task_id, maxlen=self.maxlen)

    def cancel(self, task_id=None):
        assert task_id or self.task_data.task_id
        return self.timer.kill(task_id or self.task_data.task_id)
