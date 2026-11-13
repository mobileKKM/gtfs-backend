"""What an import takes and returns. Kept apart from load.py so the worker process can use them without importing the
import's heavy dependencies (only the import process loads those)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SourceMeta:
    sha256: str
    etag: str | None = None
    last_modified: str | None = None
    archive_path: str | None = None


@dataclass
class ImportResult:
    feed_version_id: str  # `kmk_t_v3`
    feed_id: str
    version: int  # per-feed version number
    status: str
    row_counts: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    error: str | None = None
    retired: list[int] = field(default_factory=list)  # version numbers of the same feed
    seconds: float = 0.0
