"""Sync feeds.yml into core.feed (feed_version rows reference it)."""

from __future__ import annotations

import psycopg
from psycopg.types.json import Jsonb

from gtfs_backend.feeds import FeedsFile


def sync_feeds(conn: psycopg.Connection, feeds: FeedsFile) -> None:
    """Upsert configured feeds; feeds removed from the YAML (or disabled) become inactive, never deleted."""
    with conn.transaction():
        cur = conn.cursor()
        for feed in feeds.feeds:
            cur.execute(
                """
                INSERT INTO core.feed (feed_id, stop_namespace, active, config, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (feed_id) DO UPDATE
                SET stop_namespace = excluded.stop_namespace, active = excluded.active, config = excluded.config,
                    updated_at = now()
                """,
                (feed.feed_id, feed.namespace, feed.enabled, Jsonb(feed.model_dump(mode="json"))),
            )
        cur.execute(
            "UPDATE core.feed SET active = false, updated_at = now() WHERE feed_id <> ALL(%s)",
            ([f.feed_id for f in feeds.feeds],),
        )
