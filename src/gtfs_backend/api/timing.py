"""How long requests take, per route template, in this process; reported every few minutes as `api_timing` log lines.

Only route templates (`/api/v1/stops/{stop_key}/departures`) are recorded, never URLs, query strings or clients.
Every response carries `Server-Timing: app;dur=<ms>`, plus `cache;desc=hit|miss` where the shared response cache
(api/cache.py) was asked, so a client or a load test sees the server's share of the latency.
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from dataclasses import dataclass

from starlette.types import ASGIApp, Message, Receive, Scope, Send

CACHE_STATE = "response_cache"  # request.state key api/cache.py sets to "hit" or "miss"
MAX_SAMPLES = 20_000  # per route and report interval; later requests only count


@dataclass(frozen=True)
class RouteStats:
    route: str
    n: int
    p50_ms: float
    p95_ms: float
    max_ms: float
    hit_rate: float | None  # share of cache hits, where the cache was asked


def _quantile(sorted_ms: list[float], q: float) -> float:
    return round(sorted_ms[min(len(sorted_ms) - 1, int(q * len(sorted_ms)))], 1)


class Timings:
    def __init__(self) -> None:
        self._reset()

    def _reset(self) -> None:
        self._ms: dict[str, list[float]] = defaultdict(list)
        self._count: Counter[str] = Counter()
        self._hits: Counter[str] = Counter()
        self._asked: Counter[str] = Counter()

    def record(self, route: str, ms: float, cache: str | None) -> None:
        self._count[route] += 1
        if len(samples := self._ms[route]) < MAX_SAMPLES:
            samples.append(ms)
        if cache is not None:
            self._asked[route] += 1
            self._hits[route] += cache == "hit"

    def take(self) -> list[RouteStats]:
        """Stats since the last call, busiest route first."""
        out = []
        for route, n in self._count.most_common():
            ms = sorted(self._ms[route])
            asked = self._asked[route]
            out.append(
                RouteStats(
                    route,
                    n,
                    _quantile(ms, 0.5),
                    _quantile(ms, 0.95),
                    round(ms[-1], 1),
                    round(self._hits[route] / asked, 2) if asked else None,
                )
            )
        self._reset()
        return out


class TimingMiddleware:
    """Pure ASGI (no response buffering): outermost, so its time includes compression."""

    def __init__(self, app: ASGIApp, timings: Timings) -> None:
        self.app = app
        self.timings = timings

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()

        def cache_state() -> str | None:
            state = scope.get("state")
            return state.get(CACHE_STATE) if isinstance(state, dict) else None

        async def send_with_timing(message: Message) -> None:
            if message["type"] == "http.response.start":
                ms = (time.perf_counter() - started) * 1000
                value = f"app;dur={ms:.1f}"
                if (cache := cache_state()) is not None:
                    value += f", cache;desc={cache}"
                message.setdefault("headers", [])
                message["headers"] = [*message["headers"], (b"server-timing", value.encode())]
                route = scope.get("route")
                self.timings.record(getattr(route, "path", "<unmatched>"), ms, cache)
            await send(message)

        await self.app(scope, receive, send_with_timing)
