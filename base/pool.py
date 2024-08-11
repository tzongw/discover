import abc
import time
import contextlib
from functools import partial
import gevent
from gevent.queue import Queue
from .utils import LogSuppress


class Pool(metaclass=abc.ABCMeta):
    def __init__(self, maxsize=64, timeout=3, stale=None):
        self._maxsize = maxsize
        self._timeout = timeout
        self._idle = Queue()
        self._size = 0
        self._stale = stale
        self._reaping = False

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
        while not self._idle.empty():
            conn = self._idle.get_nowait()[0]
            with LogSuppress():
                self._close_conn(conn)

    def _get_conn(self):
        if not self._idle.empty() or self._size >= self._maxsize:
            return self._idle.get(timeout=self._timeout)[0]

        self._size += 1
        try:
            conn = self.create_connection()
        except Exception:
            self._size -= 1
            raise
        return conn

    def _reap_stale(self):
        while self._idle.qsize():
            _, expire = self._idle.peek_nowait()
            interval = expire - time.time()
            if interval > 0:
                gevent.sleep(interval)
            else:
                conn, _ = self._idle.get_nowait()
                self._close_conn(conn)
        self._reaping = False

    def _close_conn(self, conn):
        self._size -= 1
        self.close_connection(conn)

    def _return_conn(self, conn):
        if self._idle.qsize() < self._maxsize:
            if self._stale is None:
                item = (conn, None)
            else:
                item = (conn, time.time() + self._stale)
                if not self._reaping:
                    self._reaping = True
                    gevent.spawn_later(self._stale, self._reap_stale)
            self._idle.put(item)
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
