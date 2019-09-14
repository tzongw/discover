#!/usr/bin/env python

# WS client example

import asyncio
import const
import websockets
from tornado.options import options, define, parse_command_line

define("uid", 0, int, "uid start")

parse_command_line()

one_reader = False


async def worker(i):
    uid = options.uid + i
    await asyncio.sleep(0.1 * i)
    async with websockets.connect(
            f'ws://dev.xc/ws?token=pass&uid={uid}') as websocket:  # type: websockets.WebSocketClientProtocol
        resp = await websocket.recv()  # login resp
        print(f"{resp} {uid}")
        while True:
            websocket.ping()
            await asyncio.sleep(const.PING_INTERVAL)



fs = []
for i in range(5000):
    f = asyncio.ensure_future(worker(i))
    fs.append(f)

asyncio.get_event_loop().run_until_complete(asyncio.wait(fs))
