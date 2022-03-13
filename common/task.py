# -*- coding: utf-8 -*-
import json
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
            args = json.loads(task.args)
            kwargs = json.loads(task.kwargs)
            self.current_task.id = task.id
            f(*args, **kwargs)
            self.current_task.id = None

    def __call__(self, f):
        path = f'{f.__module__}.{f.__name__}'
        self.handlers[path] = f

        def wrapper(*args, **kwargs):
            task = Task(path=path, args=json.dumps(args), kwargs=json.dumps(kwargs))
            return task

        wrapper.wrapped = f
        return wrapper

    def post(self, task: Task, interval, loop=False, task_id=None, do_hint=True):
        if task_id:
            task.id = task_id
        elif not task.id:
            task.id = f'{stream_name(task)}:{task.path}:{task.args}:{task.kwargs}'
        return self.timer.create(task, interval, loop, key=task.id, maxlen=self.maxlen, do_hint=do_hint)

    def cancel(self, task_id=None):
        assert task_id or self.current_task.id
        return self.timer.kill(task_id or self.current_task.id)
