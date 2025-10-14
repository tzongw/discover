# -*- coding: utf-8 -*-
# noinspection PyUnresolvedReferences
from common.const import *
from enum import IntEnum

CTX_UID = 'uid'
CTX_TOKEN = 'token'
CTX_GROUP = 'group'

ROOM = 'room'

MAX_SESSIONS = 3


class ProcessStatus(IntEnum):
    INIT = 0
    APPROVE = 1
    REJECT = -1
    CANCEL = -2
    PROCESSING = 2
    SUCCESS = 3
    FAIL = -3


class SwitchStatus(IntEnum):
    OFF = 0
    ON = 1
