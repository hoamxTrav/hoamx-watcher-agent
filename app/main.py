from __future__ import annotations
import logging
import uuid
from fastapi import FastAPI, Depends, Request, Header
from dotenv import load_dotenv

from .config import load_settings, Settings
from .db import make_engine, make_session_factory, get_session
from .security import require_agent_key
from .schemas import PollRequest, PollResult
from .watcher import run_watcher

load_dotenv()

#settings: Settings = load_settings()
#logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
#engine = make_engine(settings.database_url)
#SessionFactory = make_session_factory(engine)

settings = None
engine = None
SessionFactory = None

@app.on_event("startup")
def startup():
    global settings, engine, SessionFactory
    try:
        settings = load_settings()
        logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
        engine = make_engine(settings.database_url)
        SessionFactory = make_session_factory(engine)
    except Exception as e:
        # Log clearly but keep the server alive so Cloud Run can route traffic
        logging.exception("Watcher misconfigured on startup: %s", e)


app = FastAPI(title="HOAMX Watcher Agent", version="0.1.0")

def db_dep():
    db = get_session(SessionFactory)
    try:
        yield db
    finally:
        db.close()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/poll", response_model=PollResult, dependencies=[Depends(lambda x_agent_key=Header(default=None): require_agent_key(settings, x_agent_key))])
async def poll(req: PollRequest, request: Request, db=Depends(db_dep)):
    # Defensive: tenant should be a known, safe schema name. You can enforce a whitelist here.
    if settings is None or SessionFactory is None:
        raise HTTPException(status_code=500, detail="server misconfigured: missing secrets/env")

    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())

    bs = req.batch_size or settings.batch_size
    result = await run_watcher(
        db,
        settings,
        tenant=req.tenant,
        batch_size=bs,
        emit_full_row=req.emit_full_row,
        request_id=request_id,
    )
    # Map to response schema
    return {
        "tenant": result["tenant"],
        "observed_count": result["observed_count"],
        "new_events_count": result["new_events_count"],
        "dispatched_count": result["dispatched_count"],
        "skipped_existing_events_count": result["skipped_existing_events_count"],
        "last_seen_id_before": result["last_seen_id_before"],
        "last_seen_id_after": result["last_seen_id_after"],
        "errors": result.get("dispatch_errors", []),
    }
