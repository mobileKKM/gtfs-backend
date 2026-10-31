"""One partition of every GTFS table per feed version, keyed by feed_version_id and named readably
(`stop_times__kmk_t__v3`, see spec.partition_name).

A version's partitions are created standalone, filled and then attached, which only takes a weak lock on the
parent: readers keep reading during an import.
"""

from __future__ import annotations

from typing import NamedTuple

import psycopg
from psycopg import sql

from gtfs_backend.worker.static.spec import TABLES, partition_name


class FeedVersion(NamedTuple):
    id: str  # core.feed_version.id (`kmk_t_v3`), the key every table joins on
    feed_id: str
    version: int  # 1, 2, 3, ... per feed

    def partition(self, table: str) -> str:
        return partition_name(table, self.feed_id, self.version)


def create_version_partition(cur: psycopg.Cursor, table: str, fv: FeedVersion) -> sql.Identifier:
    """A standalone table shaped like the GTFS table, to be filled and then attached."""
    part = sql.Identifier("gtfs", fv.partition(table))
    cur.execute(
        sql.SQL(
            "CREATE TABLE {part} (LIKE {parent} INCLUDING DEFAULTS INCLUDING GENERATED, "
            # matches the partition bound, so ATTACH doesn't need to scan the rows
            "CONSTRAINT version_chk CHECK (feed_version_id = {id}))"
        ).format(part=part, parent=sql.Identifier("gtfs", table), id=sql.Literal(fv.id))
    )
    return part


def attach_version_partition(cur: psycopg.Cursor, table: str, part: sql.Identifier, fv: FeedVersion) -> None:
    cur.execute(
        sql.SQL("ALTER TABLE {parent} ATTACH PARTITION {part} FOR VALUES IN ({id})").format(
            parent=sql.Identifier("gtfs", table), part=part, id=sql.Literal(fv.id)
        )
    )


def drop_version_partitions(conn: psycopg.Connection, fv: FeedVersion) -> None:
    cur = conn.cursor()
    for name in TABLES:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("gtfs", fv.partition(name))))
