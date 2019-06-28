from gevent import monkey

monkey.patch_all()
import gevent
from tornado.options import options, define, parse_command_line
import websocket

import time


define("uid", 0, int, "uid start")

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


def worker(i):
    uid = options.uid + i
    gevent.sleep(0.1 * i)
    for _ in range(10):
        ws = websocket.WebSocketApp(f'ws://dev.xc:35010/ws?token=pass&uid={uid}',
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.on_open = on_open
        ws.__uid = uid
        ws.run_forever()


def main():
    options.parse_command_line()
    #websocket.enableTrace(True)
    workers = []
    for i in range(1000):
        workers.append(gevent.spawn(worker, i))
    gevent.joinall(workers)


if __name__ == '__main__':
    main()
