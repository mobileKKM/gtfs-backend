"""gtfs-worker command line (the HTTP API has its own: gtfs-api).

gtfs-worker run                                  # the worker: static + realtime tasks
gtfs-worker import-static --feed kmk_a           # download now and import if new (feed_id from feeds.yml)
gtfs-worker import-static --feed kmk_a --file x.zip
gtfs-worker import-static --all                  # every enabled feed, shared-stop providers first
gtfs-worker import-static --feed kmk_a --retry   # also import the zip again if its import failed before
gtfs-worker fetch-osm                            # download and process OSM stop poles
gtfs-worker retire [--feed kmk_a]                # retire versions no longer needed
gtfs-worker rebuild-service-days [--feed kmk_a]

Heavy modules are imported inside the commands, so `--help` stays fast.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from gtfs_backend.db import async_pool, connect
from gtfs_backend.feeds import FeedConfig, load_feeds
from gtfs_backend.logs import log, setup_logging
from gtfs_backend.worker.feed_sync import sync_feeds
from gtfs_backend.worker.settings import WorkerSettings, get_worker_settings


def _report(feed_id: str, result) -> int:
    error = f" - {result.error}" if result.error else ""
    print(f"{feed_id}: {result.status} (version {result.version}, {result.seconds}s){error}")
    return 0 if result.status == "ready" else 1


def _import_file(settings: WorkerSettings, feed: FeedConfig, path: Path) -> int:
    from gtfs_backend.worker.static import archive
    from gtfs_backend.worker.static.load import import_zip
    from gtfs_backend.worker.static.results import SourceMeta

    archive.validate_zip(path)
    sha = archive.sha256_file(path)
    stored = archive.archive_path(settings.archive_dir, feed.feed_id, sha)
    archive.put(path, stored, move=False)
    with connect(settings.database_url) as conn:
        known = conn.execute(archive.KNOWN_VERSION_SQL, (feed.feed_id, sha)).fetchone()
        if known and known[1] == "ready":
            log.info("already_imported", feed=feed.feed_id, version=known[0])
            return 0
        if known and known[1] in ("importing", "failed"):  # leftover from a crash or a failure: allow a manual retry
            conn.execute(
                "DELETE FROM core.feed_version WHERE feed_id = %s AND version = %s AND status <> 'ready'",
                (feed.feed_id, known[0]),
            )
            conn.commit()
        result = import_zip(
            conn,
            feed.feed_id,
            stored,
            SourceMeta(sha, archive_path=str(stored)),
            feed.namespace,
            feed.share_stops_with,
            feed.stop_code_pattern,
        )
    return _report(feed.feed_id, result)


async def _download_and_import(settings: WorkerSettings, feeds: list[FeedConfig], retry: bool = False) -> int:
    from gtfs_backend.worker.http import make_client
    from gtfs_backend.worker.static.fetch import PREVIOUSLY_FAILED, check_static

    pool = async_pool(settings.database_url, max_size=settings.db_pool_max_size)
    await pool.open()
    code = 0
    try:
        async with make_client(settings.static_download_timeout, settings.http_connect_timeout) as client:
            with ThreadPoolExecutor(max_workers=1) as executor:
                for feed in feeds:
                    result = await check_static(feed, settings, pool, client, executor, conditional=False, retry_failed=retry)
                    if result is None:
                        print(f"{feed.feed_id}: unchanged")
                    elif result.status == PREVIOUSLY_FAILED:
                        print(f"{feed.feed_id}: its import failed before (version {result.version}); --retry imports it again")
                        code = 1
                    else:
                        code |= _report(feed.feed_id, result)
    finally:
        await pool.close()
    return code


def _feed_ids(settings: WorkerSettings, feed: str | None) -> list[str]:
    return [feed] if feed else [f.feed_id for f in load_feeds(settings.feeds_config).feeds]


def cmd_import_static(settings: WorkerSettings, args: argparse.Namespace) -> int:
    from gtfs_backend.worker.static.fetch import import_order

    feeds = load_feeds(settings.feeds_config)
    with connect(settings.database_url) as conn:
        sync_feeds(conn, feeds)
    if args.file:
        if not args.feed:
            sys.exit("--file needs --feed")
        if args.retry:
            sys.exit("--retry is for downloads; --file always imports")
        return _import_file(settings, feeds.get(args.feed), Path(args.file))
    selected = import_order(feeds) if args.all else [feeds.get(args.feed)] if args.feed else None
    if not selected:
        sys.exit("give --feed FEED_ID or --all")
    return asyncio.run(_download_and_import(settings, selected, args.retry))


def cmd_fetch_osm(settings: WorkerSettings, args: argparse.Namespace) -> int:
    from gtfs_backend.worker.static.stops.osm import fetch_osm_poles

    count = fetch_osm_poles(settings.osm_stops_file)
    print(f"{count} poles written to {settings.osm_stops_file}")
    return 0


def cmd_retire(settings: WorkerSettings, args: argparse.Namespace) -> int:
    from gtfs_backend.worker.static.versions import RetirementBusy, retire_versions

    code = 0
    with connect(settings.database_url) as conn, conn.transaction():
        for feed_id in _feed_ids(settings, args.feed):
            try:
                print(f"{feed_id}: retired versions {retire_versions(conn, feed_id)}")
            except RetirementBusy:
                print(f"{feed_id}: busy (an import holds the tables), try again later")
                code = 1
    return code


def cmd_rebuild_service_days(settings: WorkerSettings, args: argparse.Namespace) -> int:
    from gtfs_backend.worker.static.versions import recompute_service_day_version

    with connect(settings.database_url) as conn, conn.transaction():
        for feed_id in _feed_ids(settings, args.feed):
            print(f"{feed_id}: {recompute_service_day_version(conn, feed_id)} service days")
    return 0


def cmd_run(settings: WorkerSettings, args: argparse.Namespace) -> int:
    from gtfs_backend.worker.runner import Worker

    return asyncio.run(Worker(settings).run())


def main() -> None:
    settings = get_worker_settings()
    setup_logging(settings.log_level)
    parser = argparse.ArgumentParser(prog="gtfs-worker", description="mobileKKM GTFS ingestion")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="run the worker (static + realtime tasks)").set_defaults(func=cmd_run)

    p = sub.add_parser("import-static", help="import a static feed now")
    p.add_argument("--feed", metavar="FEED_ID", help="feed_id from feeds.yml")
    p.add_argument("--all", action="store_true", help="all enabled feeds")
    p.add_argument("--file", help="import this local zip instead of downloading")
    p.add_argument("--retry", action="store_true", help="import the downloaded zip even if its import failed before")
    p.set_defaults(func=cmd_import_static)

    p = sub.add_parser("fetch-osm", help="download the OpenStreetMap extract (Geofabrik) and keep its stop poles")
    p.set_defaults(func=cmd_fetch_osm)

    p = sub.add_parser("retire", help="retire versions that no longer serve any day from yesterday on")
    p.add_argument("--feed")
    p.set_defaults(func=cmd_retire)

    p = sub.add_parser("rebuild-service-days", help="recompute which version serves which service day")
    p.add_argument("--feed")
    p.set_defaults(func=cmd_rebuild_service_days)

    args = parser.parse_args()
    sys.exit(args.func(settings, args))


if __name__ == "__main__":
    main()
