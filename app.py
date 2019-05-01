import redis

if __name__ == '__main__':
    r = redis.Redis()
    r.set('aa', 'bb')
    k = r.get('aa')
    print(f'hello {k}')
