# -*- coding: utf-8 -*-
from pydantic import BaseModel


class Session(BaseModel):
    expire: float


class Online(BaseModel):
    token: str
    address: str


class Runtime(BaseModel):
    address: str
    pid: int
    log_level: str
