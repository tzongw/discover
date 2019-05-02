from gevent import monkey
monkey.patch_all()
import redis
import service
import gevent
import os

r = redis.Redis()
s = service.Service(r, [(os.urandom(4).hex(), os.urandom(4).hex())])
s.start()
gevent.sleep(50)
s.stop()
gevent.sleep(10)

