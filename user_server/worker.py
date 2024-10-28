# -*- coding: utf-8 -*-
import gevent
from config import define, options
from base.executor import WaitGroup
import shared

define('concurrency', 10, int, 'number of workers')
define('slow_time', 600, int, 'time threshold for slow task')


def main():
    shared.at_exit(shared.heavy_task.stop)
    wg = WaitGroup(max_workers=options.concurrency, slow_time=options.slow_time)
    while not shared.status.exiting:
        if task := shared.heavy_task.pop():
            wg.submit(shared.heavy_task.exec, task)
    wg.join()
    while not shared.status.exited:
        gevent.sleep(0.1)
