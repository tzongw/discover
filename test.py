from gevent import monkey

monkey.patch_all()
import gevent
from tornado import options
import websocket

import time


def on_message(ws, message):
    print(message)


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    def run(*args):
        for i in range(3):
            time.sleep(1)
            ws.send("Hello %d" % i)
        time.sleep(1)
        ws.close()
        print("thread terminating...")

    gevent.spawn(run)


def worker(uid):
    for _ in range(1000):
        ws = websocket.WebSocketApp(f'ws://dev.xc:35010/ws?token=pass&uid={uid}',
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.on_open = on_open
        ws.run_forever()


def main():
    options.parse_command_line()
    #websocket.enableTrace(True)
    workers = []
    for i in range(100):
        workers.append(gevent.spawn(worker, i))
    gevent.joinall(workers)


if __name__ == '__main__':
    main()
