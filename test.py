from gevent import monkey

monkey.patch_all()
import common
from tornado import options


def main():
    options.parse_command_line()
    common.service.start()
    common.service_pools.ping("address", "conn_id", "context")


if __name__ == '__main__':
    main()
