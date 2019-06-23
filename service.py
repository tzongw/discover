import logging
import time
import uuid
from collections import defaultdict
from collections import namedtuple
from contextlib import closing
from typing import Set, DefaultDict

import gevent
from redis import Redis

import const

Node = namedtuple('Node', ['name', 'address', 'node_id'])


class Service:
    _PREFIX = 'service'
    _INTERVAL = 3

    @classmethod
    def _key(cls, node: Node):
        return f'{cls._PREFIX}:{node.name}:{node.node_id}'

    @classmethod
    def _name(cls, key: str):
        assert key.startswith(cls._PREFIX)
        _, name, _ = key.split(sep=':', maxsplit=2)
        return name

    def __init__(self, redis: Redis):
        self._redis = redis
        self._services = set()  # type: Set[Node]
        self._runner = None
        self._addresses = defaultdict(set)  # type: DefaultDict[str, Set[str]]

    def register(self, service_name, address):
        assert self._runner is None
        self._services.add(Node(service_name, address, str(uuid.uuid4())))

    def start(self):
        logging.info(f'start')
        if not self._runner:
            self._refresh()
            self._runner = gevent.spawn(self._run)

    def stop(self):
        logging.info(f'stop')
        if self._runner:
            gevent.kill(self._runner)
            self._runner = None
            keys = []
            for node in self._services:
                key = self._key(node)
                keys.append(key)
            self._redis.delete(*keys)
            self._redis.publish(self._PREFIX, 'unregister')

    def addresses(self, name) -> Set[str]:
        return self._addresses.get(name) or set()

    def _refresh(self):
        keys = set(self._redis.scan_iter(match=f'{self._PREFIX}*'))
        with self._redis.pipeline() as pipe:
            for key in keys:
                pipe.get(key)
            values = pipe.execute()
        before = self._addresses.copy()
        self._addresses.clear()
        for key, value in zip(keys, values):
            try:
                name = self._name(key)
                if value:
                    self._addresses[name].add(value)
                else:
                    logging.warning(f'invalid {key} {value}')
            except Exception:
                logging.exception(f'')
        if before != self._addresses:
            logging.info(f'{before} -> {self._addresses}')

    def _run(self):
        published = False
        while True:
            try:
                if self._services:
                    with self._redis.pipeline() as pipe:
                        for node in self._services:
                            key = self._key(node)
                            pipe.set(key, node.address, const.MISS_TIMES * self._INTERVAL)
                        pipe.execute()
                    if not published:
                        logging.info(f'publish {self._services}')
                        self._redis.publish(self._PREFIX, 'register')
                        published = True
                with closing(self._redis.pubsub()) as sub:
                    sub.subscribe(self._PREFIX)
                    self._refresh()
                    timeout = self._INTERVAL
                    while timeout > 0:
                        before = time.time()
                        if sub.get_message(ignore_subscribe_messages=True, timeout=timeout):
                            break
                        timeout -= time.time() - before
            except Exception:
                logging.exception(f'')
                gevent.sleep(self._INTERVAL)
