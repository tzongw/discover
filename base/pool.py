import abc
import contextlib
from gevent.queue import Queue


class Pool:
    def __init__(self, maxsize=64, timeout=1, acceptable=lambda e: False):
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
            self.close_connection(conn)

    def _get(self):
        pool = self._pool
        if not pool.empty() or self._size >= self._maxsize:
            return pool.get(timeout=self._timeout)

        self._size += 1
        try:
            new_item = self.create_connection()
        except Exception:
            self._size -= 1
            raise
        return new_item

    def _put(self, item):
        self._pool.put(item)

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
            if self._acceptable(e):
                return_conn()
            else:
                close_conn()
            raise
        else:
            return_conn()
