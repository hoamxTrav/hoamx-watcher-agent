from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Any, Optional, Dict, List

class PollRequest(BaseModel):
    tenant: str = Field(default="hoamx_com")
    batch_size: int | None = Field(default=None, ge=1, le=500)
    emit_full_row: bool = Field(default=False, description="If true, include contact row fields in event.data")

class PollResult(BaseModel):
    tenant: str
    observed_count: int
    new_events_count: int
    dispatched_count: int
    skipped_existing_events_count: int
    last_seen_id_before: int
    last_seen_id_after: int
    errors: List[str] = []
