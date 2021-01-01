import datetime
import time

# twitter's snowflake parameters
twepoch = 1288834974657
datacenter_id_bits = 5
worker_id_bits = 5
sequence_id_bits = 12
timestamp_bits = 63 - datacenter_id_bits - worker_id_bits - sequence_id_bits

max_timestamp = 1 << timestamp_bits
timestamp_mask = max_timestamp - 1
max_datacenter_id = 1 << datacenter_id_bits
datacenter_id_mask = max_datacenter_id - 1
max_worker_id = 1 << worker_id_bits
worker_id_mask = max_worker_id - 1
max_sequence_id = 1 << sequence_id_bits
sequence_id_mask = max_sequence_id - 1


def make_snowflake(timestamp_ms: int, datacenter_id: int, worker_id: int, sequence_id: int):
    """generate a twitter-snowflake id, based on 
    https://github.com/twitter/snowflake/blob/master/src/main/scala/com/twitter/service/snowflake/IdWorker.scala
    :param: timestamp_ms time since UNIX epoch in milliseconds"""

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
    return datetime.datetime.fromtimestamp(timestamp_ms // 1000)


class IdGenerator:
    def __init__(self, datacenter_id: int, worker_id: int):
        self._datacenter_id = datacenter_id
        self._worker_id = worker_id
        self._timestamp = 0
        self._sequence_id = 0

    def gen(self) -> int:
        timestamp = time.time_ns() // 1_000_000
        while timestamp < self._timestamp:
            raise ValueError(f'clock go backwards {timestamp} < {self._timestamp}')
        if timestamp == self._timestamp:
            self._sequence_id += 1
        else:
            self._sequence_id = 0
        self._timestamp = timestamp
        return make_snowflake(timestamp, self._datacenter_id, self._worker_id, self._sequence_id)


if __name__ == '__main__':
    t0 = twepoch + 1234
    print(local_datetime(t0))
    args = (t0, 12, 23, 34)
    assert melt(make_snowflake(*args)) == args
