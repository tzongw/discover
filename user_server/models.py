# -*- coding: utf-8 -*-
from pydantic import BaseModel


class Online(BaseModel):
    address: str
    conn_id: str


class Session(BaseModel):
    token: str
