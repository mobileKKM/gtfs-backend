"""Response models: health."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FeedHealth(BaseModel):
    feed_id: str
    version: int | None = None
    static_checked_at: str | None = None
    realtime: dict[str, dict | None]
    problems: list[str] = Field(default_factory=list)  # why the feed's data isn't fresh; empty when it is


class Health(BaseModel):
    ok: bool  # Postgres and Redis answer (else 503)
    fresh: bool  # ok, and no feed has problems
    database: bool
    redis: bool
    feeds: list[FeedHealth]
