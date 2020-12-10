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
    def __init__(self, sep=None):
        self._handlers = {}
        self._sep = sep

    def dispatch(self, key: str, *args, **kwargs):
        if self._sep is not None:
            key = key.split(self._sep, maxsplit=1)[0]
        handler = self._handlers.get(key)
        if handler:
            with LogSuppress(Exception):
                return handler(*args, **kwargs)
        else:
            logging.warning(f'not handle {args} {kwargs}')

    def handler(self, key: str):
        def decorator(f):
            assert key not in self._handlers
            self._handlers[key] = f
            return f

        return decorator

    @property
    def handlers(self):
        return self._handlers
