#!/usr/bin/env python

# WS client example

import asyncio

import websockets
from tornado.options import options, define, parse_command_line

define("uid", 0, int, "uid start")

parse_command_line()


async def worker(i):
    uid = options.uid + i
    await asyncio.sleep(0.1 * i)
    async with websockets.connect(
            f'ws://dev.xc:35010/ws?token=pass&uid={uid}') as websocket:  # type: websockets.WebSocketClientProtocol
        resp = await websocket.recv()
        print(f"< {resp} {uid}")
        for i in range(10000):
            await asyncio.sleep(10)
            await websocket.ping()


fs = []
for i in range(5000):
    f = asyncio.ensure_future(worker(i))
    fs.append(f)

asyncio.get_event_loop().run_until_complete(asyncio.wait(fs))
