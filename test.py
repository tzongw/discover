from gevent import monkey
monkey.patch_all()
import redis
import service
import gevent
import os
from tornado import options
options.parse_command_line()
from contextlib import contextmanager
import logging

@contextmanager
def f():
    logging.info('before f')
    yield 10
    logging.info('after f')


def main():
    r = redis.Redis()
    s = service.Service(r)
    s.register(os.urandom(4).hex(), os.urandom(4).hex())
    s.start()
    gevent.sleep(20)
    s.stop()


@contextmanager
def g():
    with f() as r:
        logging.info('before g')
        yield r
        logging.info('after g')


def k():
    try:
        with g() as r:
            logging.info('block k')
            return r
    finally:
        logging.info('finally')


if __name__ == "__main__":
    s = k()


