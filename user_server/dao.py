# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import BigInteger
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

engine = create_engine('sqlite:///db.sqlite3', echo=True)

Base = declarative_base()


class User(Base):
    __tablename__ = "accounts"
    id = Column(BigInteger, primary_key=True)
    username = Column(String(40), unique=True, nullable=False)
    password = Column(String(40), nullable=False)

    def __repr__(self):
        return '<User %r>' % self.username

    def __init__(self, username, password):
        self.username = username
        self.password = password

    @classmethod
    def create(cls, username, password):
        with Session(engine) as session:
            user = User(username, password)
            session.add(user)
            return user


Base.metadata.create_all(engine)
