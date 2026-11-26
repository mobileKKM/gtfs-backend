"""p50/p90/p99 per endpoint and realtime data age per feed, from the histograms every process adds to Redis
(gtfs_backend/metrics.py)."""

from __future__ import annotations

from gtfs_backend import metrics
from gtfs_backend.api.context import Context
from gtfs_backend.api.schemas.metrics import DataAgeStats, EndpointStats, Metrics


def _quantiles(s: metrics.Summary, digits: int) -> tuple[float, ...]:
    """p50, p90, p99, max of a histogram with values."""
    return tuple(round(v, digits) for v in (s.quantile(0.5), s.quantile(0.9), s.quantile(0.99), s.max))


def build(summaries: dict[str, metrics.Summary], window_s: int) -> Metrics:
    endpoints, ages = [], []
    for name, s in summaries.items():
        if s.count == 0:
            continue
        kind, _, rest = name.partition(" ")
        if kind == metrics.API:
            p50, p90, p99, top = _quantiles(s, 2)
            status = {label.removeprefix("status:"): n for label, n in sorted(s.labels.items()) if label.startswith("status:")}
            endpoints.append(
                EndpointStats(route=rest, count=s.count, p50_ms=p50, p90_ms=p90, p99_ms=p99, max_ms=top, status=status)
            )
        elif kind == metrics.AGE:
            feed_id, _, file_kind = rest.partition(" ")
            p50, p90, p99, top = _quantiles(s, 1)
            ages.append(DataAgeStats(feed_id=feed_id, kind=file_kind, count=s.count, p50_s=p50, p90_s=p90, p99_s=p99, max_s=top))
    endpoints.sort(key=lambda e: (-e.p99_ms, e.route))
    ages.sort(key=lambda a: (a.feed_id, a.kind))
    return Metrics(
        window_s=window_s,
        bucket_error=round(metrics.upper_bound(1) - 1, 3),
        endpoints=endpoints,
        data_age=ages,
    )


async def summary(ctx: Context, window_s: int) -> Metrics:
    # real time, not ctx.clock: the histograms' minutes are wall-clock minutes (CLOCK_OFFSET shifts only the data)
    return build(await metrics.read(ctx.redis, window_s), window_s)
