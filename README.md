# HOAMX Watcher Agent

A Cloud Run–friendly **Watcher Agent** that polls PostgreSQL for new rows in `hoamx_com.contact_messages`,
writes an auditable log to Postgres, and emits events to downstream APIs (router/email/sms/etc.).

This repo is intentionally small and reusable: observe → decide → record → emit.

## What it does

On each run:

1. Reads `watcher_state` to learn the **high-water mark** (`last_seen_id`)
2. Claims up to `BATCH_SIZE` new `contact_messages` rows (by `id`)
3. Creates deterministic `event_id`s (`contact.created:{tenant}:{contact_message_id}`)
4. Inserts events into `event_outbox` (idempotency via unique `event_id`)
5. Emits events to one or more downstream API endpoints
6. Records everything to `agent_log`
7. Advances `watcher_state.last_seen_id` if work was observed

## Design goals

- **Deterministic & auditable:** every run writes `agent_log` (start/end + observation)
- **Idempotent:** `event_outbox.event_id` unique constraint prevents duplicate emission
- **Concurrency-safe:** row claiming via `FOR UPDATE SKIP LOCKED` (optional, recommended)
- **Re-usable:** emits generic event payloads for downstream agents

## Quick start (local)

### 1) Create a venv and install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Set environment variables

Copy `.env.example` to `.env` and set values.

```bash
cp .env.example .env
```

### 3) Initialize tables

Run the SQL migration in `sql/001_init.sql` against your database.

### 4) Run the API

```bash
uvicorn app.main:app --reload --port 8080
```

### 5) Trigger a poll

```bash
curl -X POST "http://localhost:8080/poll" \
  -H "x-agent-key: your-dev-key" \
  -H "content-type: application/json" \
  -d '{"tenant":"hoamx_com"}'
```

## Deploy to Cloud Run

This is a standard containerized FastAPI service.

```bash
gcloud run deploy hoamx-watcher-agent \
  --source . \
  --region us-central1 \
  --set-env-vars WATCHER_AGENT_KEY=... \
  --set-env-vars DATABASE_URL=... \
  --set-env-vars DOWNSTREAM_URLS=https://router-xyz.a.run.app/events \
  --set-env-vars DOWNSTREAM_AUTH_HEADER=x-agent-key \
  --set-env-vars DOWNSTREAM_AUTH_VALUE=... \
  --set-env-vars BATCH_SIZE=50
```

### Network lockdown (typical)

- Put Postgres behind private networking (Cloud SQL private IP or VPC access)
- Use Serverless VPC Access connector for DB
- Set Cloud Run ingress to internal or require IAM auth (optional)
- Keep `WATCHER_AGENT_KEY` for additional application-level protection

## Event payload

The watcher emits JSON like:

```json
{
  "event_id": "contact.created:hoamx_com:1234",
  "event_type": "contact.created",
  "tenant": "hoamx_com",
  "contact_message_id": 1234,
  "observed_at": "2025-12-19T00:00:00Z",
  "data": {
    "name": "…",
    "email": "…",
    "phone": "…",
    "message": "…"
  }
}
```

`data` fields are configurable; consider sending only `contact_message_id` if you want downstream agents to fetch full data.

## Repo layout

- `app/` FastAPI service + watcher logic
- `sql/` DB schema + migration
- `Dockerfile` Cloud Run container
- `cloudrun/` example deployment notes

## Notes

- This repo does **not** depend on Pub/Sub. You can add it later.
- Default polling endpoint is `POST /poll` for Scheduler or internal calls.

