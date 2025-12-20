from __future__ import annotations
from fastapi import Header, HTTPException
from .config import Settings

def require_agent_key(settings: Settings, x_agent_key: str | None = Header(default=None)):
    if not x_agent_key or x_agent_key != settings.watcher_agent_key:
        raise HTTPException(status_code=401, detail="unauthorized")
    return True
