from __future__ import annotations
from sqlalchemy import (
    Table, Column, BigInteger, Text, TIMESTAMP, JSON, MetaData, Boolean,
    UniqueConstraint
)

metadata = MetaData()

# Agent log: every agent should write here (watcher/router/email/text, etc.)
agent_log = Table(
    "agent_log", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("ts", TIMESTAMP(timezone=True), nullable=False),
    Column("agent_name", Text, nullable=False),
    Column("env", Text, nullable=True),
    Column("request_id", Text, nullable=True),
    Column("event_type", Text, nullable=True),
    Column("event_id", Text, nullable=True),
    Column("tenant", Text, nullable=True),
    Column("contact_message_id", BigInteger, nullable=True),
    Column("action", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("detail", JSON, nullable=True),
    Column("error", Text, nullable=True),
)

watcher_state = Table(
    "watcher_state", metadata,
    Column("watcher_name", Text, primary_key=True),
    Column("tenant", Text, primary_key=True),
    Column("last_seen_id", BigInteger, nullable=False),
    Column("last_run_at", TIMESTAMP(timezone=True), nullable=True),
    Column("last_result", JSON, nullable=True),
)

event_outbox = Table(
    "event_outbox", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("event_id", Text, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("tenant", Text, nullable=False),
    Column("contact_message_id", BigInteger, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("status", Text, nullable=False),  # NEW, DISPATCHED, ERROR
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("dispatched_at", TIMESTAMP(timezone=True), nullable=True),
    Column("last_error", Text, nullable=True),
    UniqueConstraint("event_id", name="uq_event_outbox_event_id"),
)
