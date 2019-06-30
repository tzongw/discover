#!/usr/bin/env python

# WS client example

import asyncio

import websockets
from tornado.options import options, define, parse_command_line

define("uid", 0, int, "uid start")

parse_command_line()

one_reader = False


async def worker(i):
    uid = options.uid + i
    await asyncio.sleep(0.1 * i)
    async with websockets.connect(
            f'ws://dev.xc:35010/ws?token=pass&uid={uid}') as websocket:  # type: websockets.WebSocketClientProtocol
        resp = await websocket.recv()  # login resp
        print(f"{resp} {uid}")

        async def reader():
            global one_reader
            output = not one_reader
            one_reader = True
            while not websocket.closed:
                kk = await websocket.recv()
                if output:
                    print(f"{uid} recv {kk}")

        asyncio.ensure_future(reader())

        joined = False
        for _ in range(10000):
            if not joined:
                await websocket.send('join')
                joined = True
            else:
                await websocket.send(f'{i}')
            await asyncio.sleep(20)
        await websocket.send('leave')


fs = []
for i in range(200):
    f = asyncio.ensure_future(worker(i))
    fs.append(f)

asyncio.get_event_loop().run_until_complete(asyncio.wait(fs))
