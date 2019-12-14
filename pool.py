import abc
import contextlib
import logging

from gevent.queue import Queue

from utils import LogSuppress


class Pool:
    def __init__(self, maxsize=128, timeout=3, acceptable=lambda e: False):
        self._maxsize = maxsize
        self._timeout = timeout
        self._pool = Queue()
        self._size = 0
        self._acceptable = acceptable

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
            with LogSuppress(Exception):
                self.close_connection(conn)

    def get(self):
        pool = self._pool
        if self._size >= self._maxsize or pool.qsize():
            return pool.get(self._timeout)

        self._size += 1
        try:
            new_item = self.create_connection()
        except Exception:
            logging.exception(f'')
            self._size -= 1
            raise
        return new_item

    def put(self, item):
        self._pool.put(item)

    @contextlib.contextmanager
    def connection(self):
        conn = self.get()

        def close_conn():
            self.close_connection(conn)
            self._size -= 1

        def return_conn():
            if self._pool.qsize() < self._maxsize:
                self.put(conn)
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
