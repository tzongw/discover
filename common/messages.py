# -*- coding: utf-8 -*-
from pydantic import BaseModel


class Login(BaseModel):
    uid: int


class Logout(BaseModel):
    uid: int


class Alarm(BaseModel):
    tip: str
