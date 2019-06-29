#!/usr/bin/env python

# WS client example

import asyncio

import websockets


async def worker(uid):
    async with websockets.connect(f'ws://dev.xc:35010/ws?token=pass&uid={uid}') as websocket:  # type: websockets.WebSocketClientProtocol
        resp = await websocket.recv()
        print(f"< {resp}")
        for i in range(10000):
            await asyncio.sleep(10)
            await websocket.ping()


fs = []
for i in range(5000):
    f = asyncio.ensure_future(worker(i))
    fs.append(f)

asyncio.get_event_loop().run_until_complete(asyncio.wait(fs))
