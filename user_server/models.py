# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, date
from pydantic import BaseModel


class Session(BaseModel):
    session_id: int


class Online(BaseModel):
    session_id: int
    address: str


class Runtime(BaseModel):
    address: str
    pid: int
    log_level: str


class QueueConfig(BaseModel):
    limit = 1024
    regions = 10


class SmsConfig(BaseModel):
    cooldown = timedelta(minutes=1)
    invalid_date = date(2000, 1, 1)


ConfigModels = QueueConfig | SmsConfig
