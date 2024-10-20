from enum import Enum

PING_INTERVAL = 20
RPC_PING_STEP = 10
SLOW_WORKER = 30

APP_USER = 'user'
APP_GATE = 'gate'
APP_TIMER = 'timer'
APP_RELOAD = 'reload'

TICK_STREAM = 'stream:tick'
TICK_TIMER = 'timer:tick'

RPC_USER = f'rpc_{APP_USER}'
HTTP_USER = f'http_{APP_USER}'
RPC_TIMER = f'rpc_{APP_TIMER}'
RPC_GATE = f'rpc_{APP_GATE}'
WS_GATE = f'ws_{APP_GATE}'

SERVICES = [RPC_USER, HTTP_USER, RPC_TIMER, RPC_GATE, WS_GATE]


class Environment(Enum):
    DEV = 'dev'
    TEST = 'test'
    STAGING = 'staging'
    PROD = 'prod'
