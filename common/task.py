# -*- coding: utf-8 -*-
import pickle
import uuid
import logging
from base.mq import Receiver
from base.timer import Timer
from .mq_pb2 import Task


class AsyncTask:
    def __init__(self, timer: Timer, receiver: Receiver):
        self.timer = timer
        self.handlers = {}

        @receiver.group(Task)
        def handler(id, task: Task):
            logging.debug(f'got task {id} {task.task_id}')
            args = pickle.loads(task.args)
            kwargs = pickle.loads(task.kwargs)
            f = self.handlers[task.path]
            f(*args, **kwargs)

    def __call__(self, f):
        path = f.__module__ + f.__name__
        self.handlers[path] = f

        def wrapper(*args, **kwargs):
            task = Task(task_id=str(uuid.uuid4()), path=path, args=pickle.dumps(args), kwargs=pickle.dumps(kwargs))
            return task

        return wrapper

    def post(self, task: Task, delay=0):
        return self.timer.create(task, delay, key=task.task_id)
