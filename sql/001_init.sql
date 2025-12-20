-- HOAMX Watcher Agent schema
-- Run this against the same database that contains hoamx_com.contact_messages

BEGIN;

CREATE TABLE IF NOT EXISTS agent_log (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  agent_name TEXT NOT NULL,
  env TEXT NULL,
  request_id TEXT NULL,
  event_type TEXT NULL,
  event_id TEXT NULL,
  tenant TEXT NULL,
  contact_message_id BIGINT NULL,
  action TEXT NOT NULL,
  status TEXT NOT NULL,
  detail JSONB NULL,
  error TEXT NULL
);

CREATE TABLE IF NOT EXISTS watcher_state (
  watcher_name TEXT NOT NULL,
  tenant TEXT NOT NULL,
  last_seen_id BIGINT NOT NULL DEFAULT 0,
  last_run_at TIMESTAMPTZ NULL,
  last_result JSONB NULL,
  PRIMARY KEY (watcher_name, tenant)
);

CREATE TABLE IF NOT EXISTS event_outbox (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  tenant TEXT NOT NULL,
  contact_message_id BIGINT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'NEW', -- NEW, DISPATCHED, ERROR
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  dispatched_at TIMESTAMPTZ NULL,
  last_error TEXT NULL,
  CONSTRAINT uq_event_outbox_event_id UNIQUE (event_id)
);

COMMIT;
