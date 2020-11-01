import contextlib
import logging
import socket
from functools import lru_cache
import sys


class LogSuppress(contextlib.suppress):
    def __exit__(self, exctype, excinst, exctb):
        if excinst:
            logging.exception(f'')
        return super().__exit__(exctype, excinst, exctb)


@lru_cache()
def ip_address():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(('8.8.8.8', 9))
        return sock.getsockname()[0]


wildcard = '' if sys.platform == 'darwin' else '*'


class Dispatcher:
    def __init__(self):
        self._handlers = {}

    def dispatch(self, key: str, *args, **kwargs):
        handler = self._handlers.get(key)
        if handler:
            return handler(*args, **kwargs)
        logging.warning(f'not handle {args} {kwargs}')

    def handler(self, pattern):
        def wrapper(f):
            assert pattern not in self._handlers
            self._handlers[pattern] = f
            return f

        return wrapper

    @property
    def handlers(self):
        return self._handlers
