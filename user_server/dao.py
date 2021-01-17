# -*- coding: utf-8 -*-
from dataclasses import dataclass
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import BigInteger
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('sqlite:///db.sqlite3', echo=True)

Base = declarative_base()


@dataclass
class User(Base):
    __tablename__ = "accounts"

    id: int
    username: str
    password: str

    id = Column(BigInteger, primary_key=True)
    username = Column(String(40), unique=True, nullable=False)
    password = Column(String(40), nullable=False)


Base.metadata.create_all(engine)
