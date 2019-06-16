import contextlib
from gevent.queue import Queue
import logging
import abc


class Pool:
    def __init__(self, maxsize=64, timeout=5, idle=None):
        self.maxsize = maxsize
        self.timeout = timeout
        self.pool = Queue()
        self.size = 0
        if idle is None:
            idle = maxsize
        self.idle = idle

    @abc.abstractmethod
    def create_connection(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def close_connection(self, conn):
        raise NotImplementedError()

    def close_all(self):
        self.idle = 0
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            self.size -= 1
            try:
                self.close_connection(conn)
            except Exception as e:
                logging.error(f'error: {e}')

    def get(self):
        pool = self.pool
        if self.size >= self.maxsize or pool.qsize():
            return pool.get(self.timeout)

        self.size += 1
        try:
            new_item = self.create_connection()
        except Exception as e:
            logging.error(f'error: {e}')
            self.size -= 1
            raise
        return new_item

    def put(self, item):
        self.pool.put(item)

    @contextlib.contextmanager
    def connection(self):
        conn = self.get()

        def close_conn():
            self.close_connection(conn)
            self.size -= 1

        try:
            yield conn
        except Exception as e:
            logging.error(f'error: {e}')
            close_conn()
            raise
        else:
            if self.pool.qsize() < self.idle:
                self.put(conn)
            else:
                close_conn()

