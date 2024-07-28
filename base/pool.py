import abc
import contextlib
from gevent.queue import Queue


class Pool(metaclass=abc.ABCMeta):
    def __init__(self, maxsize=128, timeout=3):
        self._maxsize = maxsize
        self._timeout = timeout
        self._pool = Queue()
        self._size = 0

    def __del__(self):
        self.close_all()

    @abc.abstractmethod
    def create_connection(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close_connection(self, conn):
        raise NotImplementedError

    @staticmethod
    def biz_exception(e: Exception):
        return False

    def close_all(self):
        self._maxsize = 0  # _return_conn will close using conns
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            self._close_conn(conn)

    def _get_conn(self):
        pool = self._pool
        if not pool.empty() or self._size >= self._maxsize:
            return pool.get(timeout=self._timeout)

        self._size += 1
        try:
            conn = self.create_connection()
        except Exception:
            self._size -= 1
            raise
        return conn

    def _close_conn(self, conn):
        self._size -= 1
        self.close_connection(conn)

    def _return_conn(self, conn):
        if self._pool.qsize() < self._maxsize:
            self._pool.put(conn)
        else:
            self._close_conn(conn)

    @contextlib.contextmanager
    def connection(self):
        conn = self._get_conn()
        try:
            yield conn
        except Exception as e:
            if self.biz_exception(e):
                self._return_conn(conn)
            else:
                self._close_conn(conn)
            raise
        else:
            self._return_conn(conn)
