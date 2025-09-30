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
    FAIL = -1
    SUCCESS = 1


class SwitchStatus(IntEnum):
    OFF = 0
    ON = 1
