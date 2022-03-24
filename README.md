# A simple microservice skeleton
1. service register and discover via redis
2. rpc functionality via thrift
3. a gateway server demo([gate_server](gate_server)), an app server demo([user_server](user_server)), a distributed timer server demo([timer_server](timer_server)), a graceful reload server tool([auto_reload](auto_reload))
4. some other functionality like message queue via redis stream, etc.

# How to run
1. pip install -r requirements.txt
2. run redis server on port 6379
3. PYTHONPATH="common/gen-py:." venv/bin/python user_server/main.py
4. other server demo is similar

# Related projects
1. [redis-timer](https://github.com/tzongw/redis-timer), a distributed timer via redis module
2. [registry](https://github.com/tzongw/registry), a gateway server implemented in golang
