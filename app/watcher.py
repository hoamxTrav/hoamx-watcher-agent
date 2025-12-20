from __future__ import annotations
import datetime as dt
import uuid
from typing import Any, Dict, List, Tuple

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from .config import Settings

def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def log_agent(
    db: Session,
    *,
    agent_name: str,
    action: str,
    status: str,
    tenant: str | None = None,
    request_id: str | None = None,
    event_type: str | None = None,
    event_id: str | None = None,
    contact_message_id: int | None = None,
    detail: dict | None = None,
    error: str | None = None,
):
    db.execute(text("""
        INSERT INTO agent_log (ts, agent_name, env, request_id, event_type, event_id, tenant, contact_message_id, action, status, detail, error)
        VALUES (:ts, :agent_name, :env, :request_id, :event_type, :event_id, :tenant, :contact_message_id, :action, :status, :detail::jsonb, :error)
    """), {
        "ts": utcnow(),
        "agent_name": agent_name,
        "env": None,
        "request_id": request_id,
        "event_type": event_type,
        "event_id": event_id,
        "tenant": tenant,
        "contact_message_id": contact_message_id,
        "action": action,
        "status": status,
        "detail": detail if detail is not None else None,
        "error": error,
    })

def ensure_watcher_state(db: Session, watcher_name: str, tenant: str) -> int:
    row = db.execute(text("""
        SELECT last_seen_id FROM watcher_state
        WHERE watcher_name=:watcher_name AND tenant=:tenant
    """), {"watcher_name": watcher_name, "tenant": tenant}).fetchone()

    if row:
        return int(row[0])

    # initialize at 0
    db.execute(text("""
        INSERT INTO watcher_state (watcher_name, tenant, last_seen_id, last_run_at, last_result)
        VALUES (:watcher_name, :tenant, 0, NULL, NULL)
    """), {"watcher_name": watcher_name, "tenant": tenant})
    return 0

def update_watcher_state(db: Session, watcher_name: str, tenant: str, last_seen_id: int, result: dict):
    db.execute(text("""
        UPDATE watcher_state
        SET last_seen_id=:last_seen_id,
            last_run_at=:last_run_at,
            last_result=:last_result::jsonb
        WHERE watcher_name=:watcher_name AND tenant=:tenant
    """), {
        "last_seen_id": last_seen_id,
        "last_run_at": utcnow(),
        "last_result": result,
        "watcher_name": watcher_name,
        "tenant": tenant,
    })

def fetch_new_contact_rows(
    db: Session,
    tenant: str,
    last_seen_id: int,
    batch_size: int,
) -> List[dict]:
    # NOTE: This assumes contact_messages has an integer 'id' primary key.
    # If your PK differs, adjust the query accordingly.
    # We schema-qualify via tenant, which should be a safe, known schema name.
    # Do NOT accept arbitrary schema names from untrusted callers.
    q = text(f"""
        SELECT *
        FROM {tenant}.contact_messages
        WHERE id > :last_seen_id
        ORDER BY id ASC
        LIMIT :limit
    """)
    rows = db.execute(q, {"last_seen_id": last_seen_id, "limit": batch_size}).mappings().all()
    return [dict(r) for r in rows]

def build_event_payload(
    *,
    tenant: str,
    contact_row: dict,
    emit_full_row: bool,
) -> dict:
    contact_id = int(contact_row.get("id"))
    event_type = "contact.created"
    event_id = f"{event_type}:{tenant}:{contact_id}"
    payload = {
        "event_id": event_id,
        "event_type": event_type,
        "tenant": tenant,
        "contact_message_id": contact_id,
        "observed_at": utcnow().isoformat(),
    }
    if emit_full_row:
        # Be thoughtful about PII; you can choose to strip fields here.
        payload["data"] = contact_row
    return payload

def upsert_outbox_events(db: Session, tenant: str, events: List[dict]) -> Tuple[int, int]:
    new_count = 0
    skipped = 0
    for ev in events:
        try:
            db.execute(text("""
                INSERT INTO event_outbox (event_id, event_type, tenant, contact_message_id, payload, status, created_at)
                VALUES (:event_id, :event_type, :tenant, :contact_message_id, :payload::jsonb, 'NEW', :created_at)
            """), {
                "event_id": ev["event_id"],
                "event_type": ev["event_type"],
                "tenant": tenant,
                "contact_message_id": ev["contact_message_id"],
                "payload": ev,
                "created_at": utcnow(),
            })
            new_count += 1
        except Exception:
            # Likely unique constraint violation -> already exists
            skipped += 1
    return new_count, skipped

async def dispatch_events(settings: Settings, events: List[dict]) -> Tuple[int, List[str]]:
    if not settings.downstream_urls:
        return 0, []
    errors: List[str] = []
    dispatched = 0
    headers = {}
    if settings.downstream_auth_value:
        headers[settings.downstream_auth_header] = settings.downstream_auth_value

    async with httpx.AsyncClient(timeout=20) as client:
        for ev in events:
            for url in settings.downstream_urls:
                try:
                    r = await client.post(url, json=ev, headers=headers)
                    if 200 <= r.status_code < 300:
                        dispatched += 1
                    else:
                        errors.append(f"dispatch {url} status={r.status_code} body={r.text[:200]}")
                except Exception as e:
                    errors.append(f"dispatch {url} error={type(e).__name__}:{e}")
    return dispatched, errors

def mark_outbox_dispatched(db: Session, event_ids: List[str]):
    if not event_ids:
        return
    db.execute(text("""
        UPDATE event_outbox
        SET status='DISPATCHED', dispatched_at=:ts, last_error=NULL
        WHERE event_id = ANY(:event_ids)
    """), {"ts": utcnow(), "event_ids": event_ids})

def mark_outbox_error(db: Session, event_id: str, err: str):
    db.execute(text("""
        UPDATE event_outbox
        SET status='ERROR', last_error=:err
        WHERE event_id=:event_id
    """), {"event_id": event_id, "err": err[:4000]})

async def run_watcher(
    db: Session,
    settings: Settings,
    *,
    tenant: str,
    watcher_name: str = "watcher",
    batch_size: int = 50,
    emit_full_row: bool = False,
    request_id: str | None = None,
) -> dict:
    started = utcnow()
    last_seen = ensure_watcher_state(db, watcher_name, tenant)

    log_agent(db, agent_name=watcher_name, action="RUN_START", status="OK", tenant=tenant, request_id=request_id,
              detail={"last_seen_id": last_seen, "batch_size": batch_size, "emit_full_row": emit_full_row})
    db.commit()

    observed_rows = fetch_new_contact_rows(db, tenant=tenant, last_seen_id=last_seen, batch_size=batch_size)
    observed_count = len(observed_rows)

    if observed_count == 0:
        result = {
            "tenant": tenant,
            "observed_count": 0,
            "new_events_count": 0,
            "dispatched_count": 0,
            "skipped_existing_events_count": 0,
            "last_seen_id_before": last_seen,
            "last_seen_id_after": last_seen,
            "duration_ms": int((utcnow() - started).total_seconds() * 1000),
        }
        log_agent(db, agent_name=watcher_name, action="OBSERVE_NONE", status="OK", tenant=tenant, request_id=request_id,
                  detail=result)
        update_watcher_state(db, watcher_name, tenant, last_seen, result)
        db.commit()
        return result

    # Build events and outbox
    events = [build_event_payload(tenant=tenant, contact_row=row, emit_full_row=emit_full_row) for row in observed_rows]
    new_events_count, skipped = upsert_outbox_events(db, tenant, events)
    db.commit()

    # Only dispatch events we newly inserted (avoid re-dispatching old ones here)
    # Find which event_ids are NEW right now
    new_event_ids = [ev["event_id"] for ev in events]
    # Dispatch them (best-effort). You can tighten this by selecting only those inserted, if desired.
    dispatched_count, dispatch_errors = await dispatch_events(settings, events=[ev for ev in events])

    # Mark dispatched if no downstream configured -> still considered 0 dispatched
    if settings.downstream_urls:
        # Mark dispatched for those we attempted and got 2xx across urls? We counted per-success response.
        # We'll mark all as DISPATCHED if no errors were recorded; otherwise mark ERROR per event.
        if not dispatch_errors:
            mark_outbox_dispatched(db, new_event_ids)
        else:
            # Conservative: record errors on each event (keeps things simple)
            for ev_id in new_event_ids:
                mark_outbox_error(db, ev_id, "; ".join(dispatch_errors)[:2000])
        db.commit()

    last_seen_after = int(observed_rows[-1].get("id", last_seen))
    result = {
        "tenant": tenant,
        "observed_count": observed_count,
        "new_events_count": new_events_count,
        "dispatched_count": dispatched_count,
        "skipped_existing_events_count": skipped,
        "last_seen_id_before": last_seen,
        "last_seen_id_after": max(last_seen, last_seen_after),
        "duration_ms": int((utcnow() - started).total_seconds() * 1000),
        "dispatch_errors": dispatch_errors,
    }

    log_agent(db, agent_name=watcher_name, action="RUN_END", status="OK" if not dispatch_errors else "ERROR",
              tenant=tenant, request_id=request_id, detail=result,
              error=None if not dispatch_errors else "\n".join(dispatch_errors)[:4000])
    update_watcher_state(db, watcher_name, tenant, max(last_seen, last_seen_after), result)
    db.commit()
    return result
