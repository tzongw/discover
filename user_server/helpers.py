# -*- coding: utf-8 -*-
from .shared import online_key, parser, gate_service
from .hash_pb2 import Online


def send(uid_or_uids, message):
    uids = [uid_or_uids] if isinstance(uid_or_uids, int) else uid_or_uids
    keys = [online_key(uid) for uid in uids]
    for online in parser.hget_batch(keys, Online(), return_none=True):
        if not online:
            continue
        with gate_service.client(online.address) as client:
            if isinstance(message, str):
                client.send_text(online.conn_id, message)
            else:
                client.send_binary(online.conn_id, message)


def kick(uid, message=None):
    key = online_key(uid)
    online = parser.hget(key, Online(), return_none=True)
    if not online:
        return
    with gate_service.client(online.address) as client:
        if isinstance(message, str):
            client.send_text(online.conn_id, message)
        elif isinstance(message, bytes):
            client.send_binary(online.conn_id, message)
        client.remove_conn(online.conn_id)


def broadcast(group, message, exclude=None):
    if exclude is None:
        exclude = []
    keys = [online_key(uid) for uid in exclude]
    onlines = parser.hget_batch(keys, Online(), return_none=True)
    exclude = {online.conn_id for online in onlines if online}
    if isinstance(message, str):
        gate_service.broadcast_text(group, exclude, message)
    else:
        gate_service.broadcast_binary(group, exclude, message)


def join(uid, group):
    key = online_key(uid)
    online = parser.hget(key, Online(), return_none=True)
    if not online:
        return
    with gate_service.client(online.address) as client:
        client.join_group(online.conn_id, group)


def leave(uid, group):
    key = online_key(uid)
    online = parser.hget(key, Online(), return_none=True)
    if not online:
        return
    with gate_service.client(online.address) as client:
        client.leave_group(online.conn_id, group)
