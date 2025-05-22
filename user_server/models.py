# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, date
from typing import Type
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


config_models: dict[str, Type['ConfigModels']] = {}


def config_model(model):
    assert model.__name__ not in config_models
    config_models[model.__name__] = model
    return model


@config_model
class QueueConfig(BaseModel):
    limit = 1024
    regions = 10


@config_model
class SmsConfig(BaseModel):
    cooldown = timedelta(minutes=1)
    invalid_date = date(2000, 1, 1)


ConfigModels = QueueConfig | SmsConfig
