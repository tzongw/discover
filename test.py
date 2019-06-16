from gevent import monkey
monkey.patch_all()
import redis
import service
import gevent
import os
from tornado import options
options.parse_command_line()


def main():
    r = redis.Redis()
    s = service.Service(r)
    s.register(os.urandom(4).hex(), os.urandom(4).hex())
    s.start()
    gevent.sleep(20)
    s.stop()


if __name__ == "__main__":
    main()


