# -*- coding: utf-8 -*-
import logging
import sys
from enum import Enum, auto
from redis.cluster import ClusterNode, PRIMARY
from tornado.options import define, options
from redis import RedisCluster
from base import Addr

define('source', Addr(':7001'), Addr, 'slot source')
define('target', None, Addr, 'slot target')
define('slot', -1, int, 'slot')


class Confirm(Enum):
    ALWAYS = auto()
    NEVER = auto()
    SOME = auto()


def migrate(redis: RedisCluster, source: Addr, target: Addr, slot, confirm=Confirm.SOME):
    source_node: ClusterNode = redis.nodes_manager.get_node(host=source.host, port=source.port)
    if not source_node or source_node.server_type != 'primary':
        raise ValueError(f'source({source}) not exists or not primary')
    target_node: ClusterNode = redis.nodes_manager.get_node(host=target.host, port=target.port)
    if not target_node or target_node.server_type != 'primary':
        raise ValueError(f'target({target}) not exists or not primary')
    if redis.nodes_manager.get_node_from_slot(slot) != source_node:
        raise ValueError(f'source({source}) not own slot({slot})')
    count = redis.cluster_countkeysinslot(slot)
    logging.info(f'migrate slot({slot} source({source}) -> target({target}) begin')
    if confirm is Confirm.ALWAYS or confirm is Confirm.SOME and count > 0:
        logging.info(f'{count} keys in slot({slot}), continue? (Y/n)')
        answer = sys.stdin.readline()
        if answer.strip() != 'Y':
            logging.info(f'migrate break!!')
            sys.exit(0)
    source_id = redis.cluster_myid(target_node=source_node)
    target_id = redis.cluster_myid(target_node=target_node)
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
        if node in [source_node, target_node]:
            continue
        redis.cluster_setslot(target_node=node, node_id=target_id, slot_id=slot, state='NODE')
    logging.info(f'migrate slot({slot} source({source}) -> target({target}) done')


def rebalance(redis: RedisCluster, slots: set):
    expect = len(slots) // len(redis.get_primaries())
    owners = {node.name: [] for node in redis.get_primaries()}
    for slot in slots:
        node = redis.nodes_manager.get_node_from_slot(slot)
        owners[node.name].append(slot)
    with redis.pipeline(transaction=False) as pipe:
        for slot in slots:
            pipe.cluster_countkeysinslot(slot)
        count_keys = dict(zip(slots, pipe.execute()))
    for node, owned in owners.items():
        owned.sort(key=lambda slot: (count_keys[slot], slot))
        logging.info(f'node: {node} slots: {len(owned)}')
    nodes = list(owners)
    while True:
        nodes.sort(key=lambda n: (len(owners[n]), n))
        low: ClusterNode = redis.get_node(node_name=nodes[0])
        high: ClusterNode = redis.get_node(node_name=nodes[-1])
        need = expect - len(owners[low.name])
        can = len(owners[high.name]) - expect
        assert need >= 0 and can >= 0
        if need == 0 and can <= 1:
            logging.info('rebalance done!!')
            break
        count = min(need, can) or 1
        for _ in range(count):
            slot = owners[high.name].pop(0)
            owners[low.name].append(slot)  # not sorted
            migrate(redis, source=Addr(high.name), target=Addr(low.name), slot=slot)
    for node, owned in owners.items():
        logging.info(f'node: {node} slots: {len(owned)}')


def main():
    options.parse_command_line()
    source = options.source
    target = options.target
    slot = options.slot
    redis = RedisCluster(host=source.host, port=source.port, decode_responses=True)
    for name, info in redis.cluster_nodes().items():
        if 'master' in info['flags'] and not redis.get_node(node_name=name):  # primaries with no slots
            addr = Addr(name)
            redis.nodes_manager.nodes_cache[name] = ClusterNode(host=addr.host, port=addr.port, server_type=PRIMARY)
    if target:
        if not -1 <= slot < 16384:
            raise ValueError(f'slot({slot}) out of range')
        if source == target:
            raise ValueError(f'source({source}) == target({target})')
        if slot == -1:
            for slots, node in redis.cluster_slots().items():
                addr = node['primary']
                if addr != (source.host, source.port):
                    continue
                for slot in range(slots[0], slots[1] + 1):
                    migrate(redis, source, target, slot)
        else:
            migrate(redis, source, target, slot)
    else:
        if slot not in (0, 16384):
            raise ValueError(f'slot({slot}) not support')
        primaries = len(redis.get_primaries())
        slots = {redis.keyslot(str(i)) for i in range(primaries)}
        if len(slots) != primaries:
            raise ValueError(f'slots overlap slots: {len(slots)} primaries: {primaries}')
        if slot > 0:
            slots = set(range(slot)) - slots
        rebalance(redis, slots)


if __name__ == '__main__':
    main()
