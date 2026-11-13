"""The archive of static zips: one file per feed and content hash, kept for ARCHIVE_RETENTION_DAYS after its version is
retired (or failed), then removed by prune()."""

from __future__ import annotations

import hashlib
import shutil
import time
import zipfile
from pathlib import Path

import psycopg

# (version, status) of a zip this feed has seen before
KNOWN_VERSION_SQL = "SELECT version, status FROM core.feed_version WHERE feed_id = %s AND sha256 = %s"


def validate_zip(path: Path) -> None:
    if not zipfile.is_zipfile(path):
        raise ValueError("not a zip file")
    with zipfile.ZipFile(path) as zf:
        bad = zf.testzip()
        if bad is not None:
            raise ValueError(f"corrupt member in zip: {bad}")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def archive_path(archive_dir: Path, feed_id: str, sha256: str) -> Path:
    return archive_dir / feed_id / f"{sha256}.zip"


def put(src: Path, dest: Path, *, move: bool) -> None:
    """Store `src` as `dest` unless it is already there (same name = same content)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return
    if move:
        shutil.move(src, dest)
    else:
        shutil.copyfile(src, dest)


TMP_MAX_AGE_S = 86400  # downloads left in archive/tmp (a crash mid-download) are removed after this


def prune(conn: psycopg.Connection, archive_dir: Path, days: int) -> tuple[int, int]:
    """Remove the zips of versions retired more than `days` ago, and of failed ones downloaded that long ago; their
    archive_path becomes NULL. Never those of ready or importing versions; `days` 0 keeps everything. Also clears old
    files in archive/tmp. Returns (files removed, bytes). A version imported again stores its zip again first."""
    if days <= 0:
        return 0, 0
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, feed_id, sha256 FROM core.feed_version
        WHERE archive_path IS NOT NULL
          AND ((status = 'retired' AND retired_at < now() - make_interval(days => %(d)s))
               OR (status = 'failed' AND downloaded_at < now() - make_interval(days => %(d)s)))
        """,
        {"d": days},
    )
    files = size = 0
    for fv_id, feed_id, sha in cur.fetchall():
        path = archive_path(archive_dir, feed_id, sha)  # from the hash, not the stored path: always inside archive_dir
        try:
            size += path.stat().st_size
            path.unlink()
            files += 1
        except FileNotFoundError:
            pass
        cur.execute("UPDATE core.feed_version SET archive_path = NULL WHERE id = %s", (fv_id,))
    tmp = archive_dir / "tmp"
    if tmp.is_dir():
        for leftover in tmp.iterdir():
            if leftover.is_file() and time.time() - leftover.stat().st_mtime > TMP_MAX_AGE_S:
                size += leftover.stat().st_size
                leftover.unlink(missing_ok=True)
                files += 1
    return files, size
