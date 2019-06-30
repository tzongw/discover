from gevent import monkey

monkey.patch_all()
import gevent
from tornado.options import options, define, parse_command_line
import websocket

import time

define("uid", 0, int, "uid start")

parse_command_line()


def on_message(ws, message):
    print(message, ws.__uid)


def on_error(ws, error):
    print(error, ws.__uid)


def on_close(ws):
    print("### closed ###", ws.__uid)


def on_open(ws):
    def run(*args):
        for i in range(1000):
            time.sleep(10)
            ws.send("Hello %d" % i)
        time.sleep(1)
        ws.close()
        print("thread terminating...")

    gevent.spawn(run)


def worker(i, wait):
    uid = options.uid + i
    gevent.sleep(wait)
    for _ in range(10):
        ws = websocket.WebSocketApp(f'ws://dev.xc:35010/ws?token=pass&uid={uid}',
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.on_open = on_open
        ws.__uid = uid
        ws.run_forever()
        gevent.sleep(10)


def main():
    # websocket.enableTrace(True)
    workers = []
    for i in range(100):
        workers.append(gevent.spawn(worker, i, 0.1*i))
    for i in range(100):
        workers.append(gevent.spawn(worker, i, 0.1*i+10))
    gevent.joinall(workers)


if __name__ == '__main__':
    main()
