# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, date
from pydantic import BaseModel


class Session(BaseModel):
    id: int
    create_time: datetime


class Online(BaseModel):
    session_id: int
    address: str


class Runtime(BaseModel):
    address: str
    pid: int
    log_level: str


class QueueConfig(BaseModel):
    limit: int = 1024
    regions: int = 10


class SmsConfig(BaseModel):
    cooldown: timedelta = timedelta(minutes=1)
    invalid_date: date = date(2000, 1, 1)


ConfigModels = QueueConfig | SmsConfig
