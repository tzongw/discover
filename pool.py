import abc
import contextlib
import logging
import time
from gevent.queue import Queue
import gevent


class Pool:
    def __init__(self, maxsize=128, timeout=3, idle=0, acceptable=lambda e: False):
        self._maxsize = maxsize
        self._timeout = timeout
        self._idle = idle
        self._pool = Queue()
        self._size = 0
        self._acceptable = acceptable
        if idle > 0:
            gevent.spawn(self._idle_check)

    def __del__(self):
        self.close_all()

    @abc.abstractmethod
    def create_connection(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def close_connection(self, conn):
        raise NotImplementedError()

    def close_all(self):
        self._maxsize = 0
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            self._size -= 1
            self.close_connection(conn)

    def _get(self):
        pool = self._pool
        if not pool.empty() or self._size >= self._maxsize:
            return pool.get(self._timeout)

        self._size += 1
        try:
            new_item = self.create_connection()
        except Exception:
            logging.exception(f'')
            self._size -= 1
            raise
        return new_item

    def _put(self, item):
        if self._idle > 0:
            item.__last_used = time.time()
        self._pool.put(item)

    def _idle_check(self):
        logging.debug(f'idle check start')
        t = min(10.0, self._idle / 10)
        while self._maxsize > 0:
            gevent.sleep(t)
            while not self._pool.empty():
                oldest = self._pool.peek_nowait()
                if time.time() - oldest.__last_used > self._idle:
                    conn = self._pool.get_nowait()
                    self._size -= 1
                    self.close_connection(conn)
                    logging.debug(f'close idle {self._size}')
                else:
                    break
        logging.debug(f'idle check end')

    @contextlib.contextmanager
    def connection(self):
        conn = self._get()

        def close_conn():
            self.close_connection(conn)
            self._size -= 1

        def return_conn():
            if self._pool.qsize() < self._maxsize:
                self._put(conn)
            else:
                close_conn()

        try:
            yield conn
        except Exception as e:
            logging.exception(f'')
            if self._acceptable(e):
                return_conn()
            else:
                close_conn()
            raise
        else:
            return_conn()
