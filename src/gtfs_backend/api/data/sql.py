"""Query helpers for the version-partitioned GTFS tables."""

from __future__ import annotations

from typing import Any


def union_per_version(block: str, versions: list[dict[str, Any]], shared: dict[str, Any] | None = None) -> tuple[str, dict]:
    """One copy of `block` per entry, joined with UNION ALL. Returns (sql, params).

    In `block`, `{i}` is the copy's index: write `%(fv{i})s` for an entry's `fv`. Each block then names its
    feed version as a parameter of its own, so Postgres reads one partition per block. Joining feed_version_id
    from another table instead defeats partition pruning (a 20 ms departure board took 1.5 s).
    """
    blocks, params = [], dict(shared or {})
    for i, values in enumerate(versions):
        blocks.append(block.replace("{i}", str(i)))
        params |= {f"{k}{i}": v for k, v in values.items()}
    return " UNION ALL ".join(blocks), params
