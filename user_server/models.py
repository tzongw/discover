# -*- coding: utf-8 -*-
from datetime import datetime
from pydantic import BaseModel


class Session(BaseModel):
    create_time: datetime


class Online(BaseModel):
    token: str
    address: str


class Runtime(BaseModel):
    address: str
    pid: int
    log_level: str
