# -*- coding: utf-8 -*-
import gevent
from base import Base62
from base import WaitGroup
from config import define, options, ctx
import shared

define('concurrency', 10, int, 'number of workers')
define('slow_time', 60, int, 'time threshold for slow task')


def handle_task(task):
    ctx.trace = Base62.encode(shared.snowflake.gen())
    shared.heavy_task.exec(task)


def main():
    wg = WaitGroup(max_workers=options.concurrency, slow_time=options.slow_time)
    workers = shared.heavy_task.start(exec_func=lambda task: wg.submit(handle_task, task))
    gevent.joinall(workers, raise_error=True)
    wg.join()
