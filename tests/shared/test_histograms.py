"""Latency / data-age histograms (gtfs_backend/metrics.py) and the benchmark clock offset."""

import math
import time

import pytest
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError

from gtfs_backend import metrics
from gtfs_backend.settings import Settings


@pytest.mark.parametrize("value", [0.0013, 0.5, 1, 1.0000001, 2, 3, 4.1, 99.9, 1000, 12345.6])
def test_a_value_is_at_most_one_bucket_width_below_its_bound(value):
    bound = metrics.upper_bound(metrics.bucket(value))
    assert value <= bound < value * 2 ** (1 / metrics.BUCKETS_PER_OCTAVE) + 1e-12


def test_exact_bounds_stay_in_their_bucket_and_extremes_are_clamped():
    assert [metrics.bucket(v) for v in (1, 2, 4)] == [0, 8, 16]
    assert metrics.bucket(0) == metrics.bucket(-5) == metrics.bucket(math.nan) == metrics.MIN_BUCKET
    assert metrics.bucket(math.inf) == metrics.bucket(1e12) == metrics.MAX_BUCKET


def test_quantiles_are_nearest_rank_bucket_bounds():
    s = metrics.Summary()
    for v in [1] * 98 + [16, 64]:
        s.buckets[metrics.bucket(v)] += 1
        s.count += 1
    assert s.quantile(0.5) == 1
    assert s.quantile(0.99) == 16  # the 99th of 100 values
    assert s.quantile(1.0) == s.max == 64
    assert metrics.Summary().quantile(0.5) is None


def test_labels_are_never_taken_for_buckets():
    hashes = [{"api GET /x|8": "3", "api GET /x|status:304": "2", "api GET /x|-80": "1"}, {"api GET /x|8": "1"}]
    s = metrics.summarize(hashes)["api GET /x"]
    assert s.count == 5 and s.buckets == {8: 4, -80: 1}
    assert s.labels == {"status:304": 2}


@pytest.fixture
async def redis(redis_url):
    r = aioredis.Redis.from_url(redis_url)
    yield r
    await r.aclose()


async def test_processes_add_up_in_redis(redis):
    now = 1_790_000_030.0
    a, b = metrics.Recorder(clock=lambda: now), metrics.Recorder(clock=lambda: now)
    for v in (2, 2, 2):
        a.observe("api GET /x", v)
    b.observe("api GET /x", 64)
    b.label("api GET /x", "status:2xx")
    await a.flush(redis)
    await b.flush(redis)
    await a.flush(redis)  # nothing new: no write
    s = (await metrics.read(redis, 300, now=now))["api GET /x"]
    assert s.count == 4 and s.quantile(0.5) == 2 and s.max == 64 and s.labels == {"status:2xx": 1}
    assert 0 < await redis.ttl(metrics.minute_key(metrics.minute(now))) <= metrics.RETENTION_S
    # minutes before the window don't count
    assert await metrics.read(redis, 60, now=now + 600) == {}


async def test_a_failed_flush_keeps_its_counts(redis):
    rec = metrics.Recorder()
    rec.observe("age kmk_a trip_updates", 3)
    broken = aioredis.Redis.from_url("redis://localhost:1/0", socket_connect_timeout=0.2)  # nothing listens there
    with pytest.raises(RedisConnectionError):
        await rec.flush(broken)
    await broken.aclose()
    await rec.flush(redis)
    assert (await metrics.read(redis, 60))["age kmk_a trip_updates"].count == 1


def test_clock_offset_shifts_the_clock_only_when_set():
    assert Settings(_env_file=None).clock() is time.time
    shifted = Settings(_env_file=None, clock_offset=-86400).clock()
    assert abs(shifted() - (time.time() - 86400)) < 1
