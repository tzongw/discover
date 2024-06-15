# -*- coding: utf-8 -*-
from shared import online_key, gate_service, redis
from models import Online


def send(uid_or_uids, message):
    uids = [uid_or_uids] if isinstance(uid_or_uids, int) else uid_or_uids
    with redis.pipeline(transaction=False) as pipe:
        for uid in uids:
            pipe.hgetall(online_key(uid))
        user_conns = pipe.execute()
    for conns in user_conns:
        for conn_id, json_value in conns.items():
            online = Online.parse_raw(json_value)
            with gate_service.client(online.address) as client:
                if isinstance(message, str):
                    client.send_text(conn_id, message)
                else:
                    client.send_binary(conn_id, message)


def kick(uid, token=None, message=None):
    conns = redis.hgetall(online_key(uid))
    for conn_id, json_value in conns.items():
        online = Online.parse_raw(json_value)
        if token and online.token != token:
            continue
        with gate_service.client(online.address) as client:
            if isinstance(message, str):
                client.send_text(conn_id, message)
            elif isinstance(message, bytes):
                client.send_binary(conn_id, message)
            client.remove_conn(conn_id)


def broadcast(group, message, *, exclude=()):
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


def join(uid, token, group):
    conns = redis.hgetall(online_key(uid))
    for conn_id, json_value in conns.items():
        online = Online.parse_raw(json_value)
        if online.token != token:
            continue
        with gate_service.client(online.address) as client:
            client.join_group(conn_id, group)


def leave(uid, token, group):
    conns = redis.hgetall(online_key(uid))
    for conn_id, json_value in conns.items():
        online = Online.parse_raw(json_value)
        if online.token != token:
            continue
        with gate_service.client(online.address) as client:
            client.leave_group(conn_id, group)
