import contextlib
import logging
import socket
from functools import lru_cache
from contextlib import closing
import sys


class LogSuppress(contextlib.suppress):
    def __exit__(self, exctype, excinst, exctb):
        if excinst:
            logging.exception(f'')
        return super().__exit__(exctype, excinst, exctb)


@lru_cache()
def ip_address():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as sock:
        sock.connect(('8.8.8.8', 9))
        return sock.getsockname()[0]


addr_wildchar = '' if sys.platform == 'darwin' else '*'
