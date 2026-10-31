"""Reachability of Postgres and Redis, static versions and realtime poll stats per feed, and whether the data is fresh.

`ok` only says the stores answer (the container healthcheck: 503 otherwise). `fresh` says the data is current: every
feed has a timetable, its static feed is checked and its last import didn't fail, and every realtime file is being
polled, isn't failing and (except alerts, whose files can stay unchanged for long) has recent data. Each feed lists
what is wrong in `problems`; monitoring alerts on `fresh: false`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from gtfs_backend.api.context import Context
from gtfs_backend.api.data import realtime
from gtfs_backend.api.schemas import FeedHealth, Health
from gtfs_backend.feeds import FeedConfig, RealtimeKind

META_FIELDS = ("ok", "last_error", "last_poll", "last_change", "age_s", "consecutive_failures")
DB_TIMEOUT_S = 3  # /health answers even when the pool is exhausted
FAILING_POLLS = 5  # consecutive failed polls before a file counts as failing (ZTP fails one or two while replacing)


@dataclass(frozen=True)
class StaticState:
    version: int | None  # newest ready version
    checked_at: str | None
    checked_age_s: float | None
    latest_status: str | None  # of the newest version row (ready, failed, retired)
    latest_version: int | None


def _ago(seconds: float) -> str:
    return f"{seconds / 3600:.1f} h" if seconds >= 3600 else f"{seconds / 60:.0f} min"


def _epoch(iso: str | None) -> float | None:
    try:
        return datetime.fromisoformat(iso).timestamp() if iso else None
    except ValueError:
        return None


def feed_problems(
    feed: FeedConfig,
    static: StaticState | None,
    metas: dict[str, dict | None] | None,
    now: float,
    *,
    realtime_max_age: int,
    static_max_age: int,
) -> list[str]:
    """What is wrong with one feed. `static`/`metas` None: that store couldn't be read (reported as down already)."""
    problems: list[str] = []
    if static is not None:
        if static.version is None:
            problems.append("static: no timetable loaded")
        if static.latest_status == "failed":
            problems.append(f"static: the import of version {static.latest_version} failed")
        if static.checked_age_s is None:
            problems.append("static: never checked")
        elif static.checked_age_s > static_max_age:
            problems.append(f"static: not checked for {_ago(static.checked_age_s)}")

    if metas is None:
        return problems
    max_age = max(realtime_max_age, feed.realtime_max_age or 0)
    for kind, _ in feed.realtime.items():
        meta = metas.get(kind.value)
        if meta is None:
            problems.append(f"{kind.value}: never polled")
            continue
        last_poll = _epoch(meta.get("last_poll"))
        if last_poll is None or now - last_poll > max_age:
            problems.append(
                f"{kind.value}: not polled for {_ago(now - last_poll)}" if last_poll else f"{kind.value}: never polled"
            )
            continue
        if not meta.get("ok") and (meta.get("consecutive_failures") or 0) >= FAILING_POLLS:
            problems.append(f"{kind.value}: failing ({meta.get('consecutive_failures')} polls): {meta.get('last_error')}")
        if kind == RealtimeKind.SERVICE_ALERTS:
            continue
        feed_ts = meta.get("feed_timestamp")
        if feed_ts is None:
            started = _epoch(meta.get("started_at"))
            if started is None or now - started > max_age:
                problems.append(f"{kind.value}: no data since the worker started")
        elif now - feed_ts > max_age:
            problems.append(f"{kind.value}: data is {_ago(now - feed_ts)} old")
    return problems


async def _static_states(ctx: Context) -> dict[str, StaticState] | None:
    try:
        async with ctx.pool.connection(timeout=DB_TIMEOUT_S) as conn:
            cur = await conn.execute(
                """
                SELECT f.feed_id,
                       (SELECT max(version) FROM core.feed_version v WHERE v.feed_id = f.feed_id AND v.status = 'ready'),
                       f.static_checked_at::text, extract(epoch FROM now() - f.static_checked_at),
                       l.status, l.version
                FROM core.feed f
                LEFT JOIN LATERAL (
                    SELECT status, version FROM core.feed_version v WHERE v.feed_id = f.feed_id ORDER BY version DESC LIMIT 1
                ) l ON true
                WHERE f.feed_id = ANY(%s)
                """,
                (ctx.feed_ids,),
            )
            return {
                feed: StaticState(version, checked, float(age) if age is not None else None, status, latest)
                for feed, version, checked, age, status, latest in await cur.fetchall()
            }
    except Exception:  # noqa: BLE001
        return None


async def check(ctx: Context) -> Health:
    """Never raises: a store that can't be reached is reported as down."""
    states = await _static_states(ctx)
    keys = [(f.feed_id, kind.value) for f in ctx.feeds.enabled for kind, _ in f.realtime.items()]
    try:
        await ctx.redis.ping()
        metas = await realtime.meta_json(ctx.redis, keys)
    except Exception:  # noqa: BLE001
        metas = None

    s, now = ctx.settings, ctx.clock()
    feeds = []
    for f in ctx.feeds.enabled:
        static = (states or {}).get(f.feed_id, StaticState(None, None, None, None, None)) if states is not None else None
        feed_metas = {kind.value: (metas or {}).get((f.feed_id, kind.value)) for kind, _ in f.realtime.items()}
        problems = feed_problems(
            f,
            static,
            feed_metas if metas is not None else None,
            now,
            realtime_max_age=s.health_realtime_max_age,
            static_max_age=s.health_static_max_age,
        )
        rt = {kind: ({k: meta.get(k) for k in META_FIELDS} if meta else None) for kind, meta in feed_metas.items()}
        feeds.append(
            FeedHealth(
                feed_id=f.feed_id,
                version=static.version if static else None,
                static_checked_at=static.checked_at if static else None,
                realtime=rt,
                problems=problems,
            )
        )
    db_ok, redis_ok = states is not None, metas is not None
    ok = db_ok and redis_ok
    return Health(ok=ok, fresh=ok and not any(f.problems for f in feeds), database=db_ok, redis=redis_ok, feeds=feeds)
