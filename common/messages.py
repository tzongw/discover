# -*- coding: utf-8 -*-
from pydantic import BaseModel


class Connect(BaseModel):
    uid: int
    session_id: int
    count: int


class Disconnect(BaseModel):
    uid: int
    session_id: int
    count: int


class Alarm(BaseModel):
    tip: str
