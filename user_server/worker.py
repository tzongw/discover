# -*- coding: utf-8 -*-
import gevent
from config import define, options, ctx
from base import base62
from base import WaitGroup
import shared

define('concurrency', 10, int, 'number of workers')
define('slow_time', 600, int, 'time threshold for slow task')


def exec_func(task):
    ctx.trace = base62.encode(shared.id_generator.gen())
    shared.heavy_task.exec(task)


def main():
    wg = WaitGroup(max_workers=options.concurrency, slow_time=options.slow_time)
    workers = shared.heavy_task.start(exec_func=lambda task: wg.submit(exec_func, task))
    gevent.joinall(workers, raise_error=True)
    wg.join()
