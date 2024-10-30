# -*- coding: utf-8 -*-
import gevent
from config import define, options
from base.executor import WaitGroup
import shared

define('concurrency', 10, int, 'number of workers')
define('slow_time', 600, int, 'time threshold for slow task')


def main():
    wg = WaitGroup(max_workers=options.concurrency, slow_time=options.slow_time)
    workers = shared.heavy_task.start(exec_func=lambda task: wg.submit(shared.heavy_task.exec, task))
    gevent.joinall(workers, raise_error=True)
    wg.join()
