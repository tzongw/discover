import abc
import contextlib
from functools import partial
from gevent.queue import Queue
from .utils import LogSuppress


class Pool(metaclass=abc.ABCMeta):
    def __init__(self, maxsize=64, timeout=3):
        self._maxsize = maxsize
        self._timeout = timeout
        self._idle = Queue()
        self._size = 0

    @abc.abstractmethod
    def create_conn(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close_conn(self, conn):
        raise NotImplementedError

    @staticmethod
    def biz_exception(e: Exception):
        return False

    def close_all(self):
        self._maxsize = 0  # _return_conn will close using conns
        while not self._idle.empty():
            conn = self._idle.get_nowait()
            with LogSuppress():
                self._close_conn(conn)

    def _get_conn(self):
        if not self._idle.empty() or self._size >= self._maxsize:
            return self._idle.get(timeout=self._timeout)

        self._size += 1
        try:
            return self.create_conn()
        except Exception:
            self._size -= 1
            raise

    def _close_conn(self, conn):
        self._size -= 1
        self.close_conn(conn)

    def _return_conn(self, conn):
        if self._idle.qsize() < self._maxsize:
            self._idle.put(conn)
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

    def _invoke(self, name, *args, **kwargs):
        with self.connection() as conn:
            return getattr(conn, name)(*args, **kwargs)

    def __getattr__(self, name):
        return partial(self._invoke, name)
