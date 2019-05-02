from gevent import monkey
monkey.patch_all()
import redis

if __name__ == '__main__':
    r = redis.Redis()
    r.set('aa', 'bb')
    k = r.get('aa')
    print(f'hello {k}')
