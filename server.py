import os
import uuid
import json
import datetime as dt
from typing import Optional, List, Dict, Any, Tuple

import httpx
import psycopg
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field


class PollRequest(BaseModel):
    batch_size: int = Field(default=50, ge=1, le=500)
    emit_full_row: bool = Field(default=False)


class PollResponse(BaseModel):
    observed_count: int
    new_events_count: int
    dispatched_count: int
    skipped_existing_events_count: int
    last_seen_id_before: int
    last_seen_id_after: int
    errors: List[str] = []


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def get_settings() -> Dict[str, Any]:
    return {
        "DATABASE_URL": os.getenv("DATABASE_URL", ""),
        "WATCHER_AGENT_KEY": os.getenv("WATCHER_AGENT_KEY", ""),
        "BATCH_SIZE": int(os.getenv("BATCH_SIZE", "50")),
        "DOWNSTREAM_URLS": [u.strip() for u in (os.getenv("DOWNSTREAM_URLS", "") or "").split(",") if u.strip()],
        "DOWNSTREAM_AUTH_HEADER": os.getenv("DOWNSTREAM_AUTH_HEADER", "x-agent-key"),
        "DOWNSTREAM_AUTH_VALUE": os.getenv("DOWNSTREAM_AUTH_VALUE", ""),
        "WATCHER_NAME": os.getenv("WATCHER_NAME", "watcher"),
        "TENANT_LABEL": os.getenv("TENANT_LABEL", "hoamx_com"),
    }


def require_agent_key(x_agent_key: Optional[str], settings: Dict[str, Any]) -> None:
    expected = settings.get("WATCHER_AGENT_KEY") or ""
    if not expected:
        raise HTTPException(status_code=500, detail="server misconfigured: WATCHER_AGENT_KEY missing")
    if not x_agent_key or x_agent_key != expected:
        raise HTTPException(status_code=404, detail="not found")


def connect(db_url: str) -> psycopg.Connection:
    if not db_url:
        raise RuntimeError("DATABASE_URL is required")
    return psycopg.connect(db_url)


def log_agent(conn: psycopg.Connection, *, agent_name: str, request_id: str, action: str, status: str,
              detail: Optional[Dict[str, Any]] = None, error: Optional[str] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.agent_log (ts, agent_name, request_id, action, status, detail, error)
            VALUES (now(), %s, %s, %s, %s, %s::jsonb, %s)
            """,
            (agent_name, request_id, action, status, json.dumps(detail or {}), error),
        )


def ensure_watcher_state(conn: psycopg.Connection, watcher_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT last_seen_id FROM public.watcher_state WHERE watcher_name=%s", (watcher_name,))
        row = cur.fetchone()
        if row:
            return int(row[0])
        cur.execute("INSERT INTO public.watcher_state (watcher_name, last_seen_id) VALUES (%s, 0)", (watcher_name,))
        return 0


def update_watcher_state(conn: psycopg.Connection, watcher_name: str, last_seen_id: int, result: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.watcher_state
            SET last_seen_id=%s, last_run_at=now(), last_result=%s::jsonb
            WHERE watcher_name=%s
            """,
            (last_seen_id, json.dumps(result), watcher_name),
        )


def fetch_new_contacts(conn: psycopg.Connection, last_seen_id: int, limit: int) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.contact_messages
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (last_seen_id, limit),
        )
        return list(cur.fetchall())


def outbox_insert(conn: psycopg.Connection, event: Dict[str, Any]) -> bool:
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO public.event_outbox
                    (event_id, event_type, tenant, contact_message_id, payload, status, created_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, 'NEW', now())
                """,
                (
                    event["event_id"],
                    event["event_type"],
                    event["tenant"],
                    event["contact_message_id"],
                    json.dumps(event),
                ),
            )
            return True
        except psycopg.errors.UniqueViolation:
            return False


def outbox_mark_dispatched(conn: psycopg.Connection, event_ids: List[str]) -> None:
    if not event_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.event_outbox
            SET status='DISPATCHED', dispatched_at=now(), last_error=NULL
            WHERE event_id = ANY(%s)
            """,
            (event_ids,),
        )


def outbox_mark_error(conn: psycopg.Connection, event_ids: List[str], err: str) -> None:
    if not event_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.event_outbox
            SET status='ERROR', last_error=%s
            WHERE event_id = ANY(%s)
            """,
            (err[:4000], event_ids),
        )


def build_event(contact_row: Dict[str, Any], tenant_label: str, emit_full_row: bool) -> Dict[str, Any]:
    contact_id = int(contact_row["id"])
    event_type = "contact.created"
    event_id = f"{event_type}:{tenant_label}:{contact_id}"
    ev: Dict[str, Any] = {
        "event_id": event_id,
        "event_type": event_type,
        "tenant": tenant_label,
        "contact_message_id": contact_id,
        "observed_at": utcnow().isoformat(),
    }
    if emit_full_row:
        ev["data"] = contact_row
    return ev


async def dispatch(events: List[Dict[str, Any]], settings: Dict[str, Any]) -> Tuple[int, List[str]]:
    urls = settings.get("DOWNSTREAM_URLS") or []
    if not urls:
        return 0, []

    hdr_name = settings.get("DOWNSTREAM_AUTH_HEADER") or "x-agent-key"
    hdr_val = settings.get("DOWNSTREAM_AUTH_VALUE") or ""
    headers = {hdr_name: hdr_val} if hdr_val else {}

    dispatched = 0
    errors: List[str] = []
    async with httpx.AsyncClient(timeout=20) as client:
        for ev in events:
            for url in urls:
                try:
                    r = await client.post(url, json=ev, headers=headers)
                    if 200 <= r.status_code < 300:
                        dispatched += 1
                    else:
                        errors.append(f"{url} status={r.status_code} body={r.text[:200]}")
                except Exception as e:
                    errors.append(f"{url} error={type(e).__name__}:{e}")
    return dispatched, errors


app = FastAPI(title="HOAMX Watcher Agent", version="1.0.0")


@app.post("/poll", response_model=PollResponse)
async def poll(req: PollRequest, request: Request, x_agent_key: Optional[str] = Header(default=None)):
    settings = get_settings()
    require_agent_key(x_agent_key, settings)

    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    watcher_name = settings["WATCHER_NAME"]
    tenant_label = settings["TENANT_LABEL"]
    batch_size = int(req.batch_size or settings["BATCH_SIZE"])

    started = utcnow()

    try:
        with connect(settings["DATABASE_URL"]) as conn:
            conn.autocommit = False

            last_seen = ensure_watcher_state(conn, watcher_name)
            log_agent(conn, agent_name=watcher_name, request_id=request_id, action="RUN_START", status="OK",
                      detail={"last_seen_id": last_seen, "batch_size": batch_size, "emit_full_row": req.emit_full_row})

            rows = fetch_new_contacts(conn, last_seen_id=last_seen, limit=batch_size)
            observed_count = len(rows)

            events = [build_event(r, tenant_label=tenant_label, emit_full_row=req.emit_full_row) for r in rows]

            new_events = 0
            skipped = 0
            inserted_ids: List[str] = []

            for ev in events:
                if outbox_insert(conn, ev):
                    new_events += 1
                    inserted_ids.append(ev["event_id"])
                else:
                    skipped += 1

            last_seen_after = last_seen if not rows else int(rows[-1]["id"])

            # Persist state + outbox first
            state_result = {
                "observed_count": observed_count,
                "new_events_count": new_events,
                "skipped_existing_events_count": skipped,
                "ts": utcnow().isoformat(),
            }
            update_watcher_state(conn, watcher_name, max(last_seen, last_seen_after), state_result)
            conn.commit()

        dispatched_count = 0
        errors: List[str] = []

        if inserted_ids:
            to_send = [ev for ev in events if ev["event_id"] in set(inserted_ids)]
            dispatched_count, errors = await dispatch(to_send, settings)

            with connect(settings["DATABASE_URL"]) as conn2:
                conn2.autocommit = False
                if errors:
                    outbox_mark_error(conn2, inserted_ids, "; ".join(errors)[:2000])
                    log_agent(conn2, agent_name=watcher_name, request_id=request_id, action="DISPATCH", status="ERROR",
                              detail={"dispatched_count": dispatched_count, "errors": errors[:10]},
                              error="; ".join(errors)[:4000])
                else:
                    outbox_mark_dispatched(conn2, inserted_ids)
                    log_agent(conn2, agent_name=watcher_name, request_id=request_id, action="DISPATCH", status="OK",
                              detail={"dispatched_count": dispatched_count})
                conn2.commit()

        with connect(settings["DATABASE_URL"]) as conn3:
            conn3.autocommit = False
            end_result = {
                "observed_count": observed_count,
                "new_events_count": new_events,
                "dispatched_count": dispatched_count,
                "skipped_existing_events_count": skipped,
                "last_seen_id_before": last_seen,
                "last_seen_id_after": max(last_seen, last_seen_after),
                "duration_ms": int((utcnow() - started).total_seconds() * 1000),
                "errors": errors[:10],
            }
            log_agent(conn3, agent_name=watcher_name, request_id=request_id, action="RUN_END",
                      status="OK" if not errors else "ERROR", detail=end_result,
                      error=None if not errors else "; ".join(errors)[:4000])
            conn3.commit()

        return PollResponse(
            observed_count=observed_count,
            new_events_count=new_events,
            dispatched_count=dispatched_count,
            skipped_existing_events_count=skipped,
            last_seen_id_before=last_seen,
            last_seen_id_after=max(last_seen, last_seen_after),
            errors=errors,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"watcher error: {type(e).__name__}: {e}")
