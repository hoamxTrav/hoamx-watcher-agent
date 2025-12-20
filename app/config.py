from __future__ import annotations
import os
from dataclasses import dataclass

def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default

@dataclass(frozen=True)
class Settings:
    database_url: str
    watcher_agent_key: str

    tenant_default: str
    batch_size: int
    poll_timeout_seconds: int

    downstream_urls: list[str]
    downstream_auth_header: str
    downstream_auth_value: str | None

    log_level: str

def load_settings() -> Settings:
    database_url = _env("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    watcher_agent_key = _env("WATCHER_AGENT_KEY")
    if not watcher_agent_key:
        raise RuntimeError("WATCHER_AGENT_KEY is required")

    tenant_default = _env("TENANT_DEFAULT", "hoamx_com") or "hoamx_com"
    batch_size = int(_env("BATCH_SIZE", "50") or "50")
    poll_timeout_seconds = int(_env("POLL_TIMEOUT_SECONDS", "25") or "25")

    downstream_urls_raw = _env("DOWNSTREAM_URLS", "") or ""
    downstream_urls = [u.strip() for u in downstream_urls_raw.split(",") if u.strip()]

    downstream_auth_header = _env("DOWNSTREAM_AUTH_HEADER", "x-agent-key") or "x-agent-key"
    downstream_auth_value = _env("DOWNSTREAM_AUTH_VALUE")

    log_level = _env("LOG_LEVEL", "INFO") or "INFO"

    return Settings(
        database_url=database_url,
        watcher_agent_key=watcher_agent_key,
        tenant_default=tenant_default,
        batch_size=batch_size,
        poll_timeout_seconds=poll_timeout_seconds,
        downstream_urls=downstream_urls,
        downstream_auth_header=downstream_auth_header,
        downstream_auth_value=downstream_auth_value,
        log_level=log_level,
    )
