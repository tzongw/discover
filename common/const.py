from enum import Enum

PING_INTERVAL = 20
CLIENT_TTL = 3 * PING_INTERVAL
SLOW_WORKER = 30

APP_USER = 'user'
APP_GATE = 'gate'
APP_TIMER = 'timer'

RPC_USER = f'rpc_{APP_USER}'
HTTP_USER = f'http_{APP_USER}'
RPC_TIMER = f'rpc_{APP_TIMER}'
RPC_GATE = f'rpc_{APP_GATE}'
WS_GATE = f'ws_{APP_GATE}'


class Environment(Enum):
    DEV = 'dev'
    TEST = 'test'
    STAGING = 'staging'
    PROD = 'prod'
