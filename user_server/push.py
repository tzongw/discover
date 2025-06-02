# -*- coding: utf-8 -*-
from base import create_parser
from shared import online_key, gate_service, redis, parser
from models import Online
from base import LogSuppress


def send(uid=None, message=None, mapping: dict = None):
    assert uid is not None or mapping
    if mapping is None:
        mapping = {}
    if uid is not None:
        mapping[uid] = message
    with redis.pipeline(transaction=False) as pipe:
        parser = create_parser(pipe)
        for uid in mapping.keys():
            parser.hgetall(online_key(uid), Online)
        for user_conns, message in zip(pipe.execute(), mapping.values()):
            for conn_id, online in user_conns.items():
                with LogSuppress(), gate_service.client(online.address) as client:
                    if isinstance(message, str):
                        client.send_text(conn_id, message)
                    else:
                        client.send_binary(conn_id, message)


def kick(uid, message=None, *, session_id=None):
    conns = parser.hgetall(online_key(uid), Online)
    for conn_id, online in conns.items():
        if session_id and online.session_id != session_id:
            continue
        with LogSuppress(), gate_service.client(online.address) as client:
            if isinstance(message, str):
                client.send_text(conn_id, message)
            elif isinstance(message, bytes):
                client.send_binary(conn_id, message)
            client.remove_conn(conn_id)


def join(uid, group, *, session_id=None):
    conns = parser.hgetall(online_key(uid), Online)
    for conn_id, online in conns.items():
        if session_id and online.session_id != session_id:
            continue
        with LogSuppress(), gate_service.client(online.address) as client:
            client.join_group(conn_id, group)


def leave(uid, group, *, session_id=None):
    conns = parser.hgetall(online_key(uid), Online)
    for conn_id, online in conns.items():
        if session_id and online.session_id != session_id:
            continue
        with LogSuppress(), gate_service.client(online.address) as client:
            client.leave_group(conn_id, group)


def broadcast(group, message, *, exclude=()):
    if exclude:
        with redis.pipeline(transaction=False) as pipe:
            for uid in exclude:
                pipe.hkeys(online_key(uid))
            exclude = sum(pipe.execute(), [])
    if isinstance(message, str):
        gate_service.broadcast_text(group, exclude, message)
    else:
        gate_service.broadcast_binary(group, exclude, message)
