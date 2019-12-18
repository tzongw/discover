import socket
from functools import lru_cache
from contextlib import closing
import sys


@lru_cache()
def ip_address():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as sock:
        sock.connect(('8.8.8.8', 9))
        return sock.getsockname()[0]


addr_wildchar = '' if sys.platform == 'darwin' else '*'
