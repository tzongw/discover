# -*- coding: utf-8 -*-
from pydantic import BaseModel


class Connect(BaseModel):
    uid: int


class Disconnect(BaseModel):
    uid: int


class Alarm(BaseModel):
    tip: str
