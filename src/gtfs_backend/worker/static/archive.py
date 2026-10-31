"""The archive of static zips: one file per feed and content hash, kept after its version is retired."""

from __future__ import annotations

import hashlib
import shutil
import zipfile
from pathlib import Path

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
