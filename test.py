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
    print("### open ###", ws.__uid)


def worker(i, wait):
    uid = options.uid + i
    gevent.sleep(wait)
    for _ in range(1):
        ws = websocket.WebSocketApp(f'ws://tx:3389/ws?token=pass&uid={uid}',
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.on_open = on_open
        ws.__uid = uid
        ws.run_forever()
        gevent.sleep(10)
        print('hahahahahah')


def main():
    # websocket.enableTrace(True)
    workers = []
    for i in range(5000):
        workers.append(gevent.spawn(worker, i, 0.01 * i))
    gevent.joinall(workers)


if __name__ == '__main__':
    main()
