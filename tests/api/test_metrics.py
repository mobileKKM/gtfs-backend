"""Request metrics middleware and the /metrics body (no DB)."""

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from gtfs_backend import metrics
from gtfs_backend.api.metrics import RequestMetricsMiddleware, status_label
from gtfs_backend.api.services.metrics import build


def make_app(recorder: metrics.Recorder) -> FastAPI:
    app = FastAPI()

    @app.get("/api/v1/stops/{stop_key}/departures")
    async def board(stop_key: str):
        return {"stop": stop_key}

    @app.get("/api/v1/trips/{trip_key:path}/live")
    async def live(trip_key: str):
        return {}

    @app.get("/api/v1/broken")
    async def broken():
        raise RuntimeError("bug")

    app.add_middleware(RequestMetricsMiddleware, recorder=recorder)
    return app


async def test_requests_are_named_by_route_template_with_their_status():
    rec = metrics.Recorder()
    transport = ASGITransport(make_app(rec), raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        await c.get("/api/v1/stops/rondo-mogilskie-qphdu/departures")
        await c.get("/api/v1/trips/kmk_t:20261025:block_1/trip_9/live")
        await c.get("/api/v1/broken")
        await c.get("/missing")
    s = metrics.summarize([{f: str(n) for f, n in rec._counts.items()}])
    assert set(s) == {
        "api GET /api/v1/stops/{stop_key}/departures",
        "api GET /api/v1/trips/{trip_key}/live",
        "api GET /api/v1/broken",
        "api GET (no route)",
    }
    assert s["api GET /api/v1/broken"].labels == {"status:5xx": 1}
    assert s["api GET (no route)"].labels == {"status:4xx": 1}
    assert all(x.count == 1 for x in s.values())


def test_status_labels():
    assert [status_label(c) for c in (200, 204, 304, 301, 404, 503)] == [
        "status:2xx",
        "status:2xx",
        "status:304",
        "status:3xx",
        "status:4xx",
        "status:5xx",
    ]


def test_body_orders_endpoints_slowest_first_and_ages_by_feed():
    def summary(values, **labels):
        s = metrics.Summary()
        for v in values:
            s.buckets[metrics.bucket(v)] += 1
            s.count += 1
        s.labels.update(labels)
        return s

    body = build(
        {
            "api GET /a": summary([1, 2], **{"status:2xx": 2}),
            "api GET /b": summary([50], **{"status:304": 1}),
            "age kmk_t trip_updates": summary([4, 6]),
            "age kmk_a vehicle_positions": summary([3]),
            "api GET /never": metrics.Summary(),
        },
        300,
    )
    assert [e.route for e in body.endpoints] == ["GET /b", "GET /a"]
    assert body.endpoints[0].status == {"304": 1} and body.endpoints[0].p99_ms >= 50
    assert [(a.feed_id, a.kind, a.count) for a in body.data_age] == [
        ("kmk_a", "vehicle_positions", 1),
        ("kmk_t", "trip_updates", 2),
    ]
    assert body.data_age[1].p50_s >= 4
