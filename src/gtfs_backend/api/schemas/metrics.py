"""Response models: request latency and realtime data age (GET /metrics)."""

from __future__ import annotations

from pydantic import BaseModel


class EndpointStats(BaseModel):
    route: str  # method and route template, e.g. `GET /api/v1/stops/{stop_key}/departures`
    count: int
    p50_ms: float
    p90_ms: float
    p99_ms: float
    max_ms: float
    status: dict[str, int]  # `2xx`, `304`, `4xx`, `5xx`, ... -> requests


class DataAgeStats(BaseModel):
    """Now minus the feed header timestamp of the newest data, sampled at every poll of one realtime file."""

    feed_id: str
    kind: str  # vehicle_positions, trip_updates, service_alerts, combined
    count: int  # polls
    p50_s: float
    p90_s: float
    p99_s: float
    max_s: float


class Metrics(BaseModel):
    """Over the last `window_s` seconds (whole minutes), from every API process and the worker. Values are reported as
    the upper bound of their histogram bucket: at most `bucket_error` (relative) above the real one."""

    window_s: int
    bucket_error: float
    endpoints: list[EndpointStats]  # slowest p99 first
    data_age: list[DataAgeStats]  # by feed and kind
