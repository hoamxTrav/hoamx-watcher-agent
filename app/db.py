from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

def make_engine(database_url: str) -> Engine:
    # pool_pre_ping helps with serverless cold starts / dropped connections
    return create_engine(database_url, pool_pre_ping=True, future=True)

def make_session_factory(engine: Engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def get_session(session_factory) -> Session:
    return session_factory()
