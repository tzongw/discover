#!/usr/bin/env python

# WS client example

import asyncio
from common import const
import websockets
from tornado.options import options, define, parse_command_line

define("uid", 0, int, "uid start")

parse_command_line()

one_reader = False



async def worker(i):
    uid = options.uid + i
    await asyncio.sleep(0.01 * i)
    async with websockets.connect(
            f'ws://tx:3389/ws?token=pass&uid={uid}', ping_interval=const.PING_INTERVAL) as websocket:  # type: websockets.WebSocketClientProtocol
        resp = await websocket.recv()  # login resp
 #       await websocket.send('join')
        print(f"+++{resp} {uid}")
        ws.append((websocket, uid))
        while True:
            r = await websocket.recv()
            if not r:
                print(f'---exit {i}')
                break
            if i == 1:
                print(r)

ws = []
async def sender():
    from random import choice
    print('sending')
    while True:
        if ws:
            s, uid = choice(ws)
            await s.send(f'i am {uid}')
        await asyncio.sleep(0.1)
    print('exiting')

fs = []
for i in range(1000):
    f = asyncio.ensure_future(worker(i))
    fs.append(f)

# asyncio.ensure_future(sender())
asyncio.get_event_loop().run_until_complete(asyncio.wait(fs))
