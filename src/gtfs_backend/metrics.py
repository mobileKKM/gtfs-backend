"""Histograms of request latency (API) and realtime data age (worker), added up across processes in Redis.

Each process counts into fixed log-scale buckets in memory (`Recorder.observe`: one dict increment) and adds its counts
to the current minute's Redis hash every FLUSH_S (`Recorder.run`, a background task). Histograms of the same buckets
add up, so percentiles over any set of processes and minutes are exact up to the bucket width: a value is reported as
the upper bound of its bucket, at most 2^(1/BUCKETS_PER_OCTAVE) - 1 (about 9 %) above it.

Redis (layout of its own, not live data):

    metrics:{epoch minute}   HASH "{name}|{bucket}" -> count, and "{name}|{label}" -> count (e.g. `status:2xx`; a
                             label is never all digits); expires after RETENTION_S

Names: `api {METHOD} {route template}` (milliseconds, labels: status classes), `age {feed_id} {kind}` (seconds: now
minus the feed header timestamp of the newest data, sampled at every poll). Nothing about clients is recorded.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from collections import Counter
from dataclasses import dataclass, field

from redis.asyncio import Redis

from gtfs_backend.logs import log

BUCKETS_PER_OCTAVE = 8
MIN_BUCKET = -10 * BUCKETS_PER_OCTAVE  # 2^-10: values at or below ~0.001 (or negative ones) count here
FLUSH_S = 10
RETENTION_S = 2 * 3600
MAX_WINDOW_S = 3600
KEY_PREFIX = "metrics:"

API = "api"
AGE = "age"


MAX_BUCKET = 30 * BUCKETS_PER_OCTAVE  # 2^30: anything larger (or infinite) counts here


def bucket(value: float) -> int:
    """The bucket of a value: the smallest i with value <= 2^(i / BUCKETS_PER_OCTAVE), within [MIN_BUCKET, MAX_BUCKET]."""
    if math.isnan(value) or value <= upper_bound(MIN_BUCKET):
        return MIN_BUCKET
    if value >= upper_bound(MAX_BUCKET):
        return MAX_BUCKET
    # the epsilon keeps exact bounds (1, 2, 4, ...) in their own bucket despite float rounding in log2
    return math.ceil(math.log2(value) * BUCKETS_PER_OCTAVE - 1e-9)


def upper_bound(i: int) -> float:
    return 2 ** (i / BUCKETS_PER_OCTAVE)


def minute(epoch: float) -> int:
    return int(epoch // 60)


def minute_key(m: int) -> str:
    return f"{KEY_PREFIX}{m}"


def api_name(method: str, route: str) -> str:
    return f"{API} {method} {route}"


def age_name(feed_id: str, kind: str) -> str:
    return f"{AGE} {feed_id} {kind}"


class Recorder:
    """One per process. `observe`/`label` only touch memory; `flush` (or `run`) adds the counts to Redis."""

    def __init__(self, clock=time.time):
        self.clock = clock  # wall clock for the minute keys (never shifted: CLOCK_OFFSET is for the data, not this)
        self._counts: Counter[str] = Counter()

    def observe(self, name: str, value: float) -> None:
        self._counts[f"{name}|{bucket(value)}"] += 1

    def label(self, name: str, label: str) -> None:
        """Count an event of `name` under `label` (not all digits: those fields are buckets), e.g. `status:2xx`."""
        self._counts[f"{name}|{label}"] += 1

    async def flush(self, redis: Redis) -> None:
        """Add what was counted since the last flush to this minute's hash. On failure the counts are kept for the next
        flush (and the error raised)."""
        if not self._counts:
            return
        counts, self._counts = self._counts, Counter()
        key = minute_key(minute(self.clock()))
        pipe = redis.pipeline(transaction=False)
        for f, n in counts.items():
            pipe.hincrby(key, f, n)
        pipe.expire(key, RETENTION_S)
        try:
            await pipe.execute()
        except BaseException:
            self._counts.update(counts)
            raise

    async def run(self, redis: Redis, interval: float = FLUSH_S) -> None:
        """Flush every `interval` until cancelled (then once more). Never raises otherwise: a failed flush is logged and
        its counts go with the next one."""
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.flush(redis)
                except Exception as exc:  # noqa: BLE001
                    log.warning("metrics_flush_failed", error=str(exc))
        finally:
            with contextlib.suppress(Exception):
                await asyncio.shield(self.flush(redis))


@dataclass
class Summary:
    count: int = 0
    buckets: Counter[int] = field(default_factory=Counter)
    labels: Counter[str] = field(default_factory=Counter)

    def quantile(self, q: float) -> float | None:
        """Nearest rank: the upper bound of the bucket holding the ceil(q * count)-th value."""
        if self.count == 0:
            return None
        rank, seen = max(1, math.ceil(q * self.count)), 0
        for i in sorted(self.buckets):
            seen += self.buckets[i]
            if seen >= rank:
                return upper_bound(i)
        return upper_bound(max(self.buckets))

    @property
    def max(self) -> float | None:
        return upper_bound(max(self.buckets)) if self.buckets else None


def parse_field(f: str) -> tuple[str, int | None, str | None]:
    """(name, bucket, None) or (name, None, label) of a hash field."""
    name, _, suffix = f.rpartition("|")
    if suffix.lstrip("-").isdigit():
        return name, int(suffix), None
    return name, None, suffix


def summarize(hashes: list[dict[str, str]]) -> dict[str, Summary]:
    out: dict[str, Summary] = {}
    for h in hashes:
        for f, v in h.items():
            name, i, label = parse_field(f.decode() if isinstance(f, bytes) else f)
            s = out.setdefault(name, Summary())
            n = int(v)
            if i is not None:
                s.buckets[i] += n
                s.count += n
            else:
                s.labels[label] += n
    return out


async def read(redis: Redis, window_s: int, now: float | None = None) -> dict[str, Summary]:
    """Every name's histogram over the last `window_s` seconds (whole minutes: the current one, partly filled, and the
    ones before it), from all processes."""
    now = time.time() if now is None else now
    last = minute(now)
    first = minute(now - min(window_s, MAX_WINDOW_S)) + 1
    pipe = redis.pipeline(transaction=False)
    for m in range(first, last + 1):
        pipe.hgetall(minute_key(m))
    return summarize(await pipe.execute())
