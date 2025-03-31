# -*- coding: utf-8 -*-
from config import define, options, ctx
import gevent
from base import base62
from base import WaitGroup
import shared

define('concurrency', 10, int, 'number of workers')
define('slow_time', 600, int, 'time threshold for slow task')


def handle_task(task):
    ctx.trace = base62.encode(shared.id_generator.gen())
    if shared.status.exiting:  # return back, exit asap
        shared.heavy_task.push(task)
    else:
        shared.heavy_task.exec(task)


def main():
    wg = WaitGroup(max_workers=options.concurrency, slow_time=options.slow_time)
    workers = shared.heavy_task.start(exec_func=lambda task: wg.submit(handle_task, task))
    gevent.joinall(workers, raise_error=True)
    wg.join()
