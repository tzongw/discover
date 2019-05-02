from gevent import monkey
monkey.patch_all()
import redis
import service
import gevent

r = redis.Redis()
s = service.Service(r, [('awesome', 'myhome')])
s.start()
gevent.sleep(5)
s.stop()
gevent.sleep(5)

