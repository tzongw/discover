from datetime import datetime
import time

# twitter's snowflake parameters
twepoch = 1288834974657
datacenter_id_bits = 5
worker_id_bits = 10
sequence_id_bits = 7
timestamp_bits = 63 - datacenter_id_bits - worker_id_bits - sequence_id_bits

max_timestamp = 1 << timestamp_bits
timestamp_mask = max_timestamp - 1
max_datacenter_id = 1 << datacenter_id_bits
datacenter_id_mask = max_datacenter_id - 1
max_worker_id = 1 << worker_id_bits
worker_id_mask = max_worker_id - 1
max_sequence_id = 1 << sequence_id_bits
sequence_id_mask = max_sequence_id - 1


def make(timestamp_ms: int, datacenter_id: int, worker_id: int, sequence_id: int):
    """generate a twitter-snowflake id, based on 
    https://github.com/twitter/snowflake/blob/master/src/main/scala/com/twitter/service/snowflake/IdWorker.scala
    :param: timestamp_ms time since UNIX epoch in milliseconds"""
    if timestamp_ms >= max_timestamp or datacenter_id >= max_datacenter_id or \
            worker_id >= max_worker_id or sequence_id >= max_sequence_id:
        raise ValueError(f'overflow {timestamp_ms} {datacenter_id} {worker_id} {sequence_id}')
    sid = (timestamp_ms - twepoch) & timestamp_mask
    sid = (sid << datacenter_id_bits) | (datacenter_id & datacenter_id_mask)
    sid = (sid << worker_id_bits) | (worker_id & worker_id_mask)
    sid = (sid << sequence_id_bits) | (sequence_id & sequence_id_mask)

    return sid


def melt(snowflake_id):
    """inversely transform a snowflake id back to its parts."""
    sequence_id = snowflake_id & sequence_id_mask
    snowflake_id >>= sequence_id_bits
    worker_id = snowflake_id & worker_id_mask
    snowflake_id >>= worker_id_bits
    datacenter_id = snowflake_id & datacenter_id_mask
    snowflake_id >>= datacenter_id_bits
    timestamp_ms = snowflake_id & timestamp_mask
    timestamp_ms += twepoch

    return timestamp_ms, datacenter_id, worker_id, sequence_id


def local_datetime(timestamp_ms):
    """convert millisecond timestamp to local datetime object."""
    return datetime.fromtimestamp(timestamp_ms / 1000)


def extract_datetime(snowflake_id):
    return local_datetime(melt(snowflake_id)[0])


def from_datetime(dt: datetime):
    timestamp_ms = int(dt.timestamp() * 1000)
    return make(timestamp_ms, 0, 0, 0)


class IdGenerator:
    def __init__(self, datacenter_id: int, worker_id: int):
        self._datacenter_id = datacenter_id
        self._worker_id = worker_id
        self._last_ms = 0
        self._sequence_id = 0

    def gen(self) -> int:
        cur_ms = time.time_ns() // 1_000_000
        if cur_ms < self._last_ms:
            cur_ms = self._last_ms
        if cur_ms == self._last_ms:
            self._sequence_id += 1
            if self._sequence_id >= max_sequence_id:
                self._last_ms = cur_ms = cur_ms + 1  # borrow next ms
                self._sequence_id = 0
        else:
            self._last_ms = cur_ms
            self._sequence_id = 0
        return make(cur_ms, self._datacenter_id, self._worker_id, self._sequence_id)


if __name__ == '__main__':
    t0 = twepoch + 1234
    print(local_datetime(t0))
    args = (t0, 12, 23, 34)
    assert melt(make(*args)) == args
    g = IdGenerator(datacenter_id=1, worker_id=2)
    for _ in range(1000):
        uid = g.gen()
        args = melt(uid)
        print(uid, local_datetime(args[0]), args)
