# -*- coding: utf-8 -*-
from base import create_parser
from shared import online_key, gate_service, redis, parser
from models import Online


def send(uid_or_uids, message):
    uids = [uid_or_uids] if isinstance(uid_or_uids, (int, str)) else uid_or_uids
    with redis.pipeline(transaction=False) as pipe:
        parser = create_parser(pipe)
        for uid in uids:
            parser.hgetall(online_key(uid), Online)
        for user_conns in pipe.execute():
            for conn_id, online in user_conns.items():
                with gate_service.client(online.address) as client:
                    if isinstance(message, str):
                        client.send_text(conn_id, message)
                    else:
                        client.send_binary(conn_id, message)


def kick(uid, message=None, token=None):
    conns = parser.hgetall(online_key(uid), Online)
    for conn_id, online in conns.items():
        if token and online.token != token:
            continue
        with gate_service.client(online.address) as client:
            if isinstance(message, str):
                client.send_text(conn_id, message)
            elif isinstance(message, bytes):
                client.send_binary(conn_id, message)
            client.remove_conn(conn_id)


def join(uid, group, token=None):
    conns = parser.hgetall(online_key(uid), Online)
    for conn_id, online in conns.items():
        if token and online.token != token:
            continue
        with gate_service.client(online.address) as client:
            client.join_group(conn_id, group)


def leave(uid, group, token=None):
    conns = parser.hgetall(online_key(uid), Online)
    for conn_id, online in conns.items():
        if token and online.token != token:
            continue
        with gate_service.client(online.address) as client:
            client.leave_group(conn_id, group)


def broadcast(group, message, *, exclude=()):
    if exclude:
        with redis.pipeline(transaction=False) as pipe:
            for uid in exclude:
                pipe.hkeys(online_key(uid))
            exclude = []
            for conn_ids in pipe.execute():
                exclude += conn_ids
    if isinstance(message, str):
        gate_service.broadcast_text(group, exclude, message)
    else:
        gate_service.broadcast_binary(group, exclude, message)
