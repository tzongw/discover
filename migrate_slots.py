# -*- coding: utf-8 -*-
import logging
import os
import sys

from redis.cluster import ClusterNode
from tornado.options import define, options
from redis import RedisCluster
from base import Addr

define('source', Addr(':7001'), Addr, 'slot source')
define('target', Addr(':7002'), Addr, 'slot target')
define('slot', -1, int, 'slot')


def main():
    options.parse_command_line()
    source: Addr = options.source
    target: Addr = options.target
    slot = options.slot
    if not 0 <= slot < 16348:
        raise ValueError(f'slot({slot}) out of range')
    if source == target:
        raise ValueError(f'source({source}) == target({target})')
    redis = RedisCluster(host=source.host, port=source.port, decode_responses=True)
    source_node: ClusterNode = redis.nodes_manager.get_node(host=source.host, port=source.port)
    if not source_node or source_node.server_type != 'primary':
        raise ValueError(f'source({source}) not exists or not primary')
    target_node: ClusterNode = redis.nodes_manager.get_node(host=target.host, port=target.port)
    if not target_node or target_node.server_type != 'primary':
        raise ValueError(f'target({target}) not exists or not primary')
    if redis.nodes_manager.get_node_from_slot(slot) != source_node:
        raise ValueError(f'source({source}) not own slot({slot})')
    source_id = redis.cluster_myid(target_node=source_node)
    target_id = redis.cluster_myid(target_node=target_node)
    logging.info(f'source: {source_id} target: {target_id}')
    count = redis.cluster_countkeysinslot(slot)
    logging.info(f'{count} keys in slot({slot}, continue?(Y/n)')
    answer = sys.stdin.readline()
    if answer.strip() != 'Y':
        return
    logging.info(f'migrate slot({slot} source({source}) -> target({target}) begin')
    redis.cluster_setslot(target_node=target_node, node_id=source_id, slot_id=slot, state='IMPORTING')
    redis.cluster_setslot(target_node=source_node, node_id=target_id, slot_id=slot, state='MIGRATING')
    migrating = 0
    while keys := redis.cluster_get_keys_in_slot(slot=slot, num_keys=100):
        migrating += len(keys)
        logging.info(f'migrating keys {migrating} / {count}...')
        redis.migrate(host=target.host, port=target.port, keys=keys, destination_db=0,
                      timeout=len(keys) * 1000)
    redis.cluster_setslot(target_node=target_node, node_id=target_id, slot_id=slot, state='NODE')
    redis.cluster_setslot(target_node=source_node, node_id=target_id, slot_id=slot, state='NODE')
    for node in redis.get_primaries():
        redis.cluster_setslot(target_node=node, node_id=target_id, slot_id=slot, state='NODE')
    logging.info(f'migrate slot({slot} source({source}) -> target({target}) done')


if __name__ == '__main__':
    main()
