# -*- coding: utf-8 -*-
import pickle
import uuid
import logging
from base.mq import Receiver, Publisher
from base.timer import Timer
from base.utils import stream_name
from .mq_pb2 import Task
from gevent.local import local


class AsyncTask:
    def __init__(self, timer: Timer, receiver: Receiver, maxlen=16384):
        self.timer = timer
        self.maxlen = maxlen
        self.handlers = {}
        self.current_task = local()

        @receiver.group(Task)
        def handler(id, task: Task):
            logging.debug(f'got task {id} {task.id} {task.path}')
            receiver.redis.xtrim(stream_name(task), minid=id)
            f = self.handlers.get(task.path)
            if not f:  # versioning problem? throw back task, let new version process handle it
                logging.warning(f'can not handle {id} {task.id} {task.path}, stop receive Task')
                receiver.remove(Task)
                Publisher(receiver.redis).publish(task, maxlen=self.maxlen)
                return
            args = pickle.loads(task.args)
            kwargs = pickle.loads(task.kwargs)
            self.current_task.id = task.id
            f(*args, **kwargs)
            self.current_task.id = None

    def __call__(self, f):
        path = f'{f.__module__}.{f.__name__}'
        self.handlers[path] = f

        def wrapper(*args, **kwargs):
            task = Task(path=path, args=pickle.dumps(args), kwargs=pickle.dumps(kwargs))
            return task

        wrapper.wrapped = f
        return wrapper

    def post(self, task: Task, interval, loop=False, task_id=None):
        if task_id:
            task.id = task_id
        elif not task.id:
            task.id = f'{stream_name(task)}:{task.path}' if loop else str(uuid.uuid4())
        return self.timer.create(task, interval, loop, key=task.id, maxlen=self.maxlen)

    def cancel(self, task_id=None):
        assert task_id or self.current_task.id
        return self.timer.kill(task_id or self.current_task.id)
