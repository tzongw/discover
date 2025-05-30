from enum import StrEnum

PING_INTERVAL = 45
WS_TIMEOUT = 60
RPC_PING_STEP = 10
ONLINE_TTL = 3 * PING_INTERVAL * RPC_PING_STEP

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
HTTP_GATE = f'http_{APP_GATE}'

SERVICES = [RPC_USER, HTTP_USER, RPC_TIMER, RPC_GATE, HTTP_GATE]


class Environment(StrEnum):
    DEV = 'dev'
    TEST = 'test'
    STAGING = 'staging'
    PROD = 'prod'
