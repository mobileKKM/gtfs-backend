"""Request latency per endpoint, counted into gtfs_backend.metrics (p50/p99 at GET /api/v1/metrics).

Requests are named by their route template (`GET /api/v1/stops/{stop_key}/departures`), never by their URL: no stop,
box, search or client is recorded.
"""

from __future__ import annotations

import time

from gtfs_backend import metrics

UNMATCHED = "(no route)"  # 404s for paths no route matches


def status_label(status: int) -> str:
    """`status:2xx`, `status:4xx`, ...; 304 on its own (`status:304`): how often polling clients get no body."""
    return "status:304" if status == 304 else f"status:{status // 100}xx"


def route_template(scope) -> str:
    route = scope.get("route")
    return getattr(route, "path_format", None) or getattr(route, "path", None) or UNMATCHED


class RequestMetricsMiddleware:
    """Pure ASGI. Add it last, so it is the outermost middleware: it times each HTTP request from its arrival until the
    app has sent the whole response (gzip included)."""

    def __init__(self, app, recorder: metrics.Recorder):
        self.app = app
        self.recorder = recorder

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        start = time.perf_counter()
        status = 500  # an error escaping the app becomes Starlette's 500 outside this middleware

        async def send_with_status(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_with_status)
        finally:
            name = metrics.api_name(scope["method"], route_template(scope))
            self.recorder.observe(name, (time.perf_counter() - start) * 1000)
            self.recorder.label(name, status_label(status))
