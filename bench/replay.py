"""Record the realtime feeds once, then replay them as often as needed, so benchmark runs see the same data.

    uv run python bench/replay.py record bench/out/rec --minutes 30     # polls the real feeds (as the worker does)
    uv run python bench/replay.py feeds bench/out/rec -o bench/out/feeds.replay.yml
    uv run python bench/replay.py serve bench/out/rec                   # prints the CLOCK_OFFSET to run with

A recording is every distinct body each realtime file served, with the time it arrived, plus each feed's static zip at
the start (`manifest.json` lists them with their sha256). The server answers like the operators do: the file that was
current at the replayed moment, with an ETag (304 for If-None-Match), 404 before a file's first version. It replays at
real speed from the start of the recording (or `--skip` seconds into it): the worker and the API run with
CLOCK_OFFSET = recording time - now, so the data meets the timetable day it was recorded on. Details:
docs/benchmark.md.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import contextlib
import hashlib
import json
import signal
import sys
import time
import zipfile
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import NamedTuple

import httpx
import yaml
from google.transit import gtfs_realtime_pb2 as pb

from gtfs_backend.feeds import load_feeds

MANIFEST = "manifest.json"
DEFAULT_INTERVAL_S = 10  # REALTIME_POLL_INTERVAL's default
FETCH_TIMEOUT_S = 15


# ------------------------------------------------------------------------------------------------- downloads
# (as the worker takes them: only complete files; retried while the operators replace a file)

USER_AGENT = "mobileKKM-gtfs-backend/bench"
RETRY_STATUSES = frozenset({404, 408, 429})


def parse_feed(body: bytes) -> pb.FeedMessage:
    msg = pb.FeedMessage()
    msg.ParseFromString(body)
    if not msg.header.gtfs_realtime_version:
        raise ValueError("feed message without header")
    return msg


def validate_zip(path: Path) -> None:
    with zipfile.ZipFile(path) as zf:
        if (bad := zf.testzip()) is not None:
            raise ValueError(f"corrupt member in zip: {bad}")


def make_client(timeout: float, connect_timeout: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=connect_timeout), follow_redirects=True, headers={"User-Agent": USER_AGENT}
    )


class Fetched(NamedTuple):
    not_modified: bool
    value: object = None
    etag: str | None = None
    last_modified: str | None = None


async def _retrying(attempt, retries: int, retry_delay: float):
    for n in range(1, retries + 1):
        try:
            return await attempt()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if (status < 500 and status not in RETRY_STATUSES) or n == retries:
                raise
        except Exception:
            if n == retries:
                raise
        await asyncio.sleep(retry_delay)


async def fetch_bytes(client, url, parse, *, etag=None, last_modified=None, retries=3, retry_delay=1.0, timeout=None):
    headers = {k: v for k, v in (("If-None-Match", etag), ("If-Modified-Since", last_modified)) if v}

    async def attempt():
        resp = await client.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 304:
            return Fetched(True, None, etag, last_modified)
        resp.raise_for_status()
        expected = resp.headers.get("Content-Length")
        if expected and not resp.headers.get("Content-Encoding") and int(expected) != len(resp.content):
            raise ValueError(f"truncated: {len(resp.content)} of {expected} bytes")
        return Fetched(False, parse(resp.content), resp.headers.get("ETag"), resp.headers.get("Last-Modified"))

    return await _retrying(attempt, retries, retry_delay)


async def fetch_to_file(client, url, dest: Path, validate, *, retries=3, retry_delay=1.0) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    async def attempt():
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            with dest.open("wb") as fh:
                async for chunk in resp.aiter_bytes():
                    fh.write(chunk)
        validate(dest)

    await _retrying(attempt, retries, retry_delay)


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


# ------------------------------------------------------------------------------------------------- record


async def _record_file(client, feed_id: str, kind: str, url: str, interval: float, out: Path, entries: list, stop) -> None:
    """Poll one realtime file until `stop` is set; save every body that differs from the last one."""
    folder = out / feed_id / kind
    folder.mkdir(parents=True, exist_ok=True)
    etag = last_modified = last_sha = None
    while not stop.is_set():
        started = time.monotonic()
        try:
            fetched = await fetch_bytes(
                client,
                url,
                lambda body: (parse_feed(body), body),  # only complete files, as the worker takes them
                etag=etag,
                last_modified=last_modified,
                retries=3,
                retry_delay=1,
                timeout=max(interval, 5),
            )
            if not fetched.not_modified:
                msg, body = fetched.value
                etag, last_modified = fetched.etag, fetched.last_modified
                if (sha := _sha(body)) != last_sha:
                    t = time.time()
                    name = f"{t:.3f}.pb"
                    (folder / name).write_bytes(body)
                    entries.append(
                        {"t": t, "file": f"{feed_id}/{kind}/{name}", "sha256": sha, "header_ts": int(msg.header.timestamp)}
                    )
                    last_sha = sha
        except Exception as exc:  # noqa: BLE001 - keep recording; the gap is part of the recording
            print(f"{feed_id}/{kind}: {type(exc).__name__}: {exc}", file=sys.stderr)
        with contextlib.suppress(TimeoutError):  # the next poll is due
            await asyncio.wait_for(stop.wait(), max(0.0, interval - (time.monotonic() - started)))


async def record(out: Path, feeds_path: Path, minutes: float, only: set[str]) -> None:
    feeds = [f for f in load_feeds(feeds_path).enabled if not only or f.feed_id in only]
    if not feeds:
        raise SystemExit("no feeds to record")
    out.mkdir(parents=True, exist_ok=True)
    manifest: dict = {"feeds_config": str(feeds_path), "feeds": {}}
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    async with make_client(FETCH_TIMEOUT_S, 10) as rt_client, make_client(300, 10) as static_client:
        for feed in feeds:  # the timetables the realtime refers to, first
            dest = out / feed.feed_id / "static.zip"
            await fetch_to_file(static_client, feed.static_url, dest, validate_zip, retries=3, retry_delay=2)
            manifest["feeds"][feed.feed_id] = {
                "static": f"{feed.feed_id}/static.zip",
                "static_sha256": _sha(dest.read_bytes()),
                "realtime": {},
            }
            print(f"{feed.feed_id}: static.zip ({dest.stat().st_size} bytes)")
        manifest["started"] = time.time()
        tasks = []
        for feed in feeds:
            interval = feed.realtime_poll_interval or DEFAULT_INTERVAL_S
            for kind, url in feed.realtime.items():
                entries: list = []
                manifest["feeds"][feed.feed_id]["realtime"][kind.value] = entries
                tasks.append(_record_file(rt_client, feed.feed_id, kind.value, url, interval, out, entries, stop))
        runner = asyncio.gather(*tasks)
        print(f"recording {len(tasks)} files for {minutes:g} min (Ctrl-C stops early)")
        with contextlib.suppress(TimeoutError):  # the recording's time is up
            await asyncio.wait_for(stop.wait(), minutes * 60)
        stop.set()
        await runner
    manifest["ended"] = time.time()
    (out / MANIFEST).write_text(json.dumps(manifest, indent=1))
    n = sum(len(e) for f in manifest["feeds"].values() for e in f["realtime"].values())
    print(f"recorded {n} files over {(manifest['ended'] - manifest['started']) / 60:.1f} min into {out}")


# ------------------------------------------------------------------------------------------------- serve


class Recording:
    def __init__(self, folder: Path):
        self.folder = folder
        self.manifest = json.loads((folder / MANIFEST).read_text())
        self.started, self.ended = self.manifest["started"], self.manifest["ended"]
        # (feed, kind) -> (arrival times, entries), in order
        self.files: dict[tuple[str, str], tuple[list[float], list[dict]]] = {}
        for feed_id, feed in self.manifest["feeds"].items():
            for kind, entries in feed["realtime"].items():
                entries = sorted(entries, key=lambda e: e["t"])
                self.files[(feed_id, kind)] = ([e["t"] for e in entries], entries)

    def at(self, feed_id: str, kind: str, t: float) -> dict | None:
        """The version of a file current at recording time `t` (None before its first one)."""
        times, entries = self.files.get((feed_id, kind), ([], []))
        i = bisect.bisect_right(times, t)
        return entries[i - 1] if i else None


def make_handler(rec: Recording, offset: float):
    statics = {feed_id: (f["static"], f["static_sha256"]) for feed_id, f in rec.manifest["feeds"].items()}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):  # noqa: A002 - quiet: the worker polls every few seconds
            pass

        def _send(self, status: HTTPStatus, body: bytes = b"", etag: str | None = None, content_type: str = "text/plain"):
            self.send_response(status)
            if etag:
                self.send_header("ETag", etag)
            if status == HTTPStatus.OK:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _file(self, path: str, sha: str, content_type: str):
            etag = f'"{sha[:32]}"'
            if etag in [t.strip() for t in self.headers.get("If-None-Match", "").split(",")]:
                return self._send(HTTPStatus.NOT_MODIFIED, etag=etag)
            return self._send(HTTPStatus.OK, (rec.folder / path).read_bytes(), etag, content_type)

        def do_GET(self):  # noqa: N802
            parts = self.path.split("?", 1)[0].strip("/").split("/")
            if parts == ["clock_offset"]:
                return self._send(HTTPStatus.OK, f"{offset:.3f}".encode())
            if len(parts) == 2 and parts[1] == "static.zip" and parts[0] in statics:
                path, sha = statics[parts[0]]
                return self._file(path, sha, "application/zip")
            if len(parts) == 2 and parts[1].endswith(".pb"):
                entry = rec.at(parts[0], parts[1].removesuffix(".pb"), time.time() + offset)
                if entry is not None:
                    return self._file(entry["file"], entry["sha256"], "application/octet-stream")
            return self._send(HTTPStatus.NOT_FOUND, b"not found")

        do_HEAD = do_GET

    return Handler


def serve(folder: Path, host: str, port: int, skip: float, offset: float | None) -> None:
    rec = Recording(folder)
    if offset is None:
        offset = rec.started + skip - time.time()
    server = ThreadingHTTPServer((host, port), make_handler(rec, offset))
    remaining = rec.ended - (time.time() + offset)
    print(f"replaying {folder} on http://{host}:{port} ({remaining / 60:.1f} min of data left)")
    print(f"CLOCK_OFFSET={offset:.3f}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


# ------------------------------------------------------------------------------------------------- feeds


def replay_feeds(folder: Path, base: Path, url: str) -> dict:
    """`base` (feeds.yml) with the recorded feeds' URLs pointing at the replay server; feeds not recorded disabled."""
    manifest = json.loads((folder / MANIFEST).read_text())
    data = yaml.safe_load(base.read_text(encoding="utf-8"))
    url = url.rstrip("/")
    for feed in data["feeds"]:
        recorded = manifest["feeds"].get(feed["feed_id"])
        if recorded is None:
            feed["enabled"] = False
            continue
        feed["static"] = f"{url}/{feed['feed_id']}/static.zip"
        feed["realtime"] = {kind: f"{url}/{feed['feed_id']}/{kind}.pb" for kind in recorded["realtime"]}
    return data


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="bench/replay.py", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("record", help="poll the real feeds and save every change")
    p.add_argument("out", type=Path)
    p.add_argument("--feeds", type=Path, default=Path("feeds.yml"))
    p.add_argument("--minutes", type=float, default=30)
    p.add_argument("--only", default="", help="comma-separated feed ids (default: all enabled)")
    p = sub.add_parser("serve", help="replay a recording over HTTP")
    p.add_argument("recording", type=Path)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--skip", type=float, default=0, help="start this many seconds into the recording")
    p.add_argument("--offset", type=float, help="reuse a CLOCK_OFFSET (e.g. after restarting the server)")
    p = sub.add_parser("feeds", help="write a feeds.yml whose URLs point at the replay server")
    p.add_argument("recording", type=Path)
    p.add_argument("--base", type=Path, default=Path("feeds.yml"))
    p.add_argument("--url", default="http://127.0.0.1:8765")
    p.add_argument("-o", "--out", type=Path, required=True)
    args = parser.parse_args(argv)

    if args.command == "record":
        asyncio.run(record(args.out, args.feeds, args.minutes, {f for f in args.only.split(",") if f}))
    elif args.command == "serve":
        serve(args.recording, args.host, args.port, args.skip, args.offset)
    else:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(yaml.safe_dump(replay_feeds(args.recording, args.base, args.url), sort_keys=False))
        load_feeds(args.out)  # fails here, not in the worker, if the result isn't valid
        print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
