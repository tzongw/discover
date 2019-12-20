# -*- coding: utf-8 -*-
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import Future


class Executor(ThreadPoolExecutor):
    def __init__(self, max_workers=None, thread_name_prefix='',
                 initializer=None, initargs=()):
        super().__init__(max_workers=max_workers, thread_name_prefix=thread_name_prefix, initializer=initializer,
                         initargs=initargs)
        self._unfinished = 0

    def submit(self, fn, *args, **kwargs) -> Future:
        self._unfinished += 1

        def dec(_):
            self._unfinished -= 1

        fut = super().submit(fn, *args, **kwargs)
        fut.add_done_callback(dec)
        return fut

    def _adjust_thread_count(self):
        if self._unfinished > len(self._threads):
            super()._adjust_thread_count()

    def gather(self, *fns):
        return [fut.result() for fut in [self.submit(fn) for fn in fns]]
