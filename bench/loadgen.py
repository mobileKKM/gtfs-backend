"""Load generator: CLIENTS app clients polling the REST API the way the app does (the same mix as bench/k6/rest_mix.js,
in Python so it runs where the API runs, without Docker's network in between), or (--mode ws) getting the same views
pushed over the WebSocket.

    uv run python bench/loadgen.py --clients 2000 --ramp 60 --duration 600 --out bench/out/results/x/load.json
    uv run python bench/loadgen.py --mode ws --clock-offset -12345.6 ...

Each client has its own connection (as an app has) and opens a view, polling it every POLL_S seconds with
If-None-Match, then the next one:
    map 50 %         /stops?bbox once, then /vehicles?bbox (a 2-8 km box somewhere in Krakow)
    board 30 %       /stops/{key}/departures
    trip 15 %        /trips/{key} once, then /trips/{key}/live
    vehicle 5 %      /vehicles/{key}/trip
and searches (/stops/search) before 10 % of the views. Clients start spread over the ramp. Reported per request name
(steady state only, after the ramp): count, p50/p90/p99/max latency (ms, until the whole body arrived), 304 share,
errors; and the generator's own event loop lag, which would inflate the latencies if it got high.

`data_age_s`, per view: every second, each client's (now - the timestamp of the newest realtime data it holds), what
its screen would show: polling adds up to POLL_S to the data's own age, push should not. With a replay, `--clock-offset`
(the CLOCK_OFFSET the API runs with) makes "now" the replayed time. In ws mode the requests are subscriptions: the
latency until the snapshot of each topic arrived.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import random
import resource
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import httpx
import orjson

CITY = (19.8, 49.97, 20.15, 50.13)  # minLon, minLat, maxLon, maxLat
QUERIES = ["rondo", "dworzec", "plac", "nowy", "bronowice", "kurdwanow", "czerwone maki", "teatr", "wawel", "krowodrza"]
NAMES = ["vehicles", "stops_map", "board", "trip", "trip_live", "vehicle_trip", "search", "ws_tile", "ws_stop", "ws_trip", "ws"]


VIEW_OF = {"vehicles": "map", "board": "board", "trip_live": "trip", "vehicle_trip": "vehicle"}


class Stats:
    def __init__(self):
        self.ages: dict[str, list[float]] = defaultdict(list)  # view -> data age samples (s)
        self.messages = 0
        self.message_bytes = 0
        self.latencies: dict[str, list[float]] = defaultdict(list)
        self.not_modified: dict[str, int] = defaultdict(int)
        self.errors: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.recording = False  # only after the ramp
        self.lag: list[float] = []

    def add(self, name: str, ms: float, status: int | str) -> None:
        if not self.recording:
            return
        if isinstance(status, int) and status in (200, 304, 404):  # 404: a trip or vehicle that left the feed
            self.latencies[name].append(ms)
            if status == 304:
                self.not_modified[name] += 1
        else:
            self.errors[name][str(status)] += 1

    def summary(self) -> dict:
        def q(values: list[float], p: float) -> float:
            return round(values[min(len(values) - 1, max(0, math.ceil(p * len(values)) - 1))], 2)

        out = {}
        for name in NAMES:
            v = sorted(self.latencies.get(name, []))
            if not v and not self.errors.get(name):
                continue
            out[name] = {
                "count": len(v),
                "p50_ms": q(v, 0.5) if v else None,
                "p90_ms": q(v, 0.9) if v else None,
                "p99_ms": q(v, 0.99) if v else None,
                "max_ms": round(v[-1], 2) if v else None,
                "not_modified_share": round(self.not_modified.get(name, 0) / len(v), 3) if v else None,
                "errors": dict(self.errors.get(name, {})),
            }
        lag = sorted(self.lag)
        ages = {}
        for view, values in sorted(self.ages.items()):
            v = sorted(values)
            ages[view] = {"samples": len(v), "p50_s": q(v, 0.5), "p90_s": q(v, 0.9), "p99_s": q(v, 0.99)}
        return {
            "requests": out,
            "data_age_s": ages,
            "messages": self.messages,
            "message_bytes": self.message_bytes,
            "loop_lag_ms": {"p50": q(lag, 0.5), "p99": q(lag, 0.99), "max": round(lag[-1], 2)} if lag else None,
        }


def box(rng: random.Random) -> str:
    w, h = rng.uniform(2, 8) / 71.5, rng.uniform(2, 8) / 111.0  # km -> degrees at 50 N
    lon, lat = rng.uniform(CITY[0], CITY[2] - w), rng.uniform(CITY[1], CITY[3] - h)
    return ",".join(f"{x:.5f}" for x in (lon, lat, lon + w, lat + h))


class Client:
    def __init__(self, base: str, data: dict, stats: Stats, rng: random.Random, poll_s: float):
        self.base, self.data, self.stats, self.rng, self.poll_s = base, data, stats, rng, poll_s
        self.etags: dict[str, str] = {}
        self.showing: str | None = None  # the view it shows now (map, board, trip, vehicle) and its newest data's time
        self.held: int | None = None
        self.held_by_url: dict[str, int | None] = {}
        # one connection, kept while the server keeps it (as an app's)
        self.http = httpx.AsyncClient(limits=httpx.Limits(max_connections=1), timeout=30)

    async def get(self, path: str, name: str) -> None:
        url = self.base + path
        headers = {"If-None-Match": self.etags[url]} if url in self.etags else {}
        start = time.perf_counter()
        try:
            r = await self.http.get(url, headers=headers)
            status: int | str = r.status_code
            if r.status_code == 200 and "etag" in r.headers:
                self.etags[url] = r.headers["etag"]
                if name in VIEW_OF:
                    self.held_by_url[url] = orjson.loads(r.content).get("timestamp")
        except httpx.HTTPError as exc:
            status = type(exc).__name__
        self.stats.add(name, (time.perf_counter() - start) * 1000, status)
        if name in VIEW_OF and isinstance(status, int) and status in (200, 304):
            self.showing, self.held = VIEW_OF[name], self.held_by_url.get(url)

    def view(self):
        r, d = self.rng.random(), self.data
        if r < 0.5:
            bbox = box(self.rng)
            return f"/stops?bbox={bbox}", "stops_map", f"/vehicles?bbox={bbox}", "vehicles"
        if r < 0.8:
            return None, None, f"/stops/{quote(self.rng.choice(d['stops']), safe='')}/departures", "board"
        if r < 0.95:
            key = quote(self.rng.choice(d["trips"]), safe="")
            return f"/trips/{key}", "trip", f"/trips/{key}/live", "trip_live"
        return None, None, f"/vehicles/{quote(self.rng.choice(d['vehicles']), safe='')}/trip", "vehicle_trip"

    async def run(self, until: float) -> None:
        try:
            while time.monotonic() < until:
                if self.rng.random() < 0.1:
                    await self.get(f"/stops/search?q={quote(self.rng.choice(QUERIES))}", "search")
                once, once_name, poll, poll_name = self.view()
                if once:
                    await self.get(once, once_name)
                await asyncio.sleep(self.rng.random() * self.poll_s)  # clients don't poll in step
                for _ in range(3 + self.rng.randrange(10)):
                    if time.monotonic() >= until:
                        return
                    await self.get(poll, poll_name)
                    await asyncio.sleep(self.poll_s)
        finally:
            await self.http.aclose()


class WsClient:
    """The same views, pushed: one WebSocket, subscribed to the view's topics (a map box: the z14 tiles it covers)."""

    def __init__(self, base: str, data: dict, stats: Stats, rng: random.Random, poll_s: float):
        self.ws_url = base.replace("http://", "ws://").replace("https://", "wss://") + "/ws"
        self.data, self.stats, self.rng, self.poll_s = data, stats, rng, poll_s
        self.showing: str | None = None
        self.held: int | None = None
        self._topics: list[str] = []
        self._asked: dict[str, float] = {}  # topic -> when subscribed (snapshot latency)
        self._held_by_topic: dict[str, int] = {}

    def topics(self) -> tuple[str, list[str]]:
        r, d = self.rng.random(), self.data
        if r < 0.5:
            lon0, lat0, lon1, lat1 = (float(v) for v in box(self.rng).split(","))
            (x0, y0), (x1, y1) = tile_of(lat1, lon0, 14), tile_of(lat0, lon1, 14)
            return "map", [f"tile:14/{x}/{y}" for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)]
        if r < 0.8:
            return "board", [f"stop:{self.rng.choice(d['stops'])}"]
        return "trip", [f"trip:{self.rng.choice(d['trips'])}"]  # a trip, or a followed vehicle's trip

    async def run(self, until: float) -> None:
        from websockets.asyncio.client import connect

        try:
            async with connect(self.ws_url, max_size=None) as ws:
                reader = asyncio.create_task(self._read(ws))
                try:
                    while time.monotonic() < until:
                        view, topics = self.topics()
                        if self._topics:
                            await ws.send(orjson.dumps({"op": "unsubscribe", "topics": self._topics}).decode())
                        self.showing, self.held, self._topics, self._held_by_topic = view, None, topics, {}
                        now = time.perf_counter()
                        self._asked = {t: now for t in topics}
                        await ws.send(orjson.dumps({"op": "subscribe", "topics": topics}).decode())
                        await asyncio.sleep(min((3 + self.rng.randrange(10)) * self.poll_s, max(0.0, until - time.monotonic())))
                finally:
                    reader.cancel()
        except Exception as exc:  # noqa: BLE001
            self.stats.add("ws", 0, type(exc).__name__)

    async def _read(self, ws) -> None:
        async for raw in ws:
            msg = orjson.loads(raw)
            if self.stats.recording:
                self.stats.messages += 1
                self.stats.message_bytes += len(raw)
            topic = msg.get("topic")
            if topic not in self._topics or msg.get("type") == "error":
                continue
            if (asked := self._asked.pop(topic, None)) is not None:
                self.stats.add(f"ws_{topic.split(':', 1)[0]}", (time.perf_counter() - asked) * 1000, 200)
            ts = msg["data"].get("timestamp")
            if ts is not None:
                self._held_by_topic[topic] = max(ts, self._held_by_topic.get(topic, 0))
            self.held = max(self._held_by_topic.values()) if self._held_by_topic else None


def tile_of(lat: float, lon: float, z: int) -> tuple[int, int]:
    n = 2**z
    x = int((lon + 180) / 360 * n)
    y = int((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n)
    return x, y


async def sample_ages(stats: Stats, clients: list, until: float, offset: float) -> None:
    """Every second: how old the newest data each client holds is (replayed time with the offset)."""
    while time.monotonic() < until:
        await asyncio.sleep(1)
        if not stats.recording:
            continue
        now = time.time() + offset
        for c in clients:
            if c.showing is not None and c.held is not None:
                stats.ages[c.showing].append(now - c.held)


async def measure_lag(stats: Stats, until: float) -> None:
    while time.monotonic() < until:
        t = time.perf_counter()
        await asyncio.sleep(0.1)
        if stats.recording:
            stats.lag.append((time.perf_counter() - t - 0.1) * 1000)


async def setup(base: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as c:
        bbox = ",".join(map(str, CITY))
        stops = (await c.get(f"{base}/stops?bbox={bbox}&kind=group")).json()["stops"]
        vehicles = (await c.get(f"{base}/vehicles?bbox={bbox}")).json()["vehicles"]
    if not stops or not vehicles:
        raise SystemExit(f"no data to test with ({len(stops)} stops, {len(vehicles)} vehicles): is the worker running?")
    return {
        "stops": [s["key"] for s in stops],
        "trips": [v["trip_key"] for v in vehicles],
        "vehicles": [v["key"] for v in vehicles],
    }


async def main(args) -> dict:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    want = args.clients + 256
    if soft < want:  # a socket per client
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(want, hard), hard))
    data = await setup(args.base)
    stats, rng = Stats(), random.Random(args.seed)
    start = time.monotonic()
    until = start + args.ramp + args.duration
    tasks = [asyncio.create_task(measure_lag(stats, until))]
    clients: list = []
    tasks.append(asyncio.create_task(sample_ages(stats, clients, until, args.clock_offset)))
    kind = WsClient if args.mode == "ws" else Client
    for i in range(args.clients):
        client = kind(args.base, data, stats, random.Random(rng.random()), args.poll)
        clients.append(client)

        async def later(c=client, delay=args.ramp * i / args.clients):
            await asyncio.sleep(delay)
            await c.run(until)

        tasks.append(asyncio.create_task(later()))
    await asyncio.sleep(args.ramp)
    stats.recording = True
    await asyncio.gather(*tasks)
    out = {"mode": args.mode, "clients": args.clients, "ramp_s": args.ramp, "duration_s": args.duration, "poll_s": args.poll}
    out |= stats.summary()
    return out


def report(result: dict) -> str:
    lines = [f"{'request':14}{'count':>8}{'p50':>8}{'p90':>8}{'p99':>8}{'max':>8}{'304':>6}  errors"]
    for name, r in result["requests"].items():
        cells = "".join(f"{r[k]:>8.1f}" if r[k] is not None else f"{'-':>8}" for k in ("p50_ms", "p90_ms", "p99_ms", "max_ms"))
        share = f"{100 * r['not_modified_share']:>5.0f}%" if r["not_modified_share"] is not None else f"{'-':>6}"
        lines.append(f"{name:14}{r['count']:>8}{cells}{share}  {r['errors'] or ''}")
    for view, a in result["data_age_s"].items():
        lines.append(
            f"data age {view:8} p50 {a['p50_s']:6.1f} s  p90 {a['p90_s']:6.1f} s  p99 {a['p99_s']:6.1f} s"
            f"  ({a['samples']} samples)"
        )
    if result["messages"]:
        lines.append(f"pushed {result['messages']} messages, {result['message_bytes'] / 1e6:.1f} MB")
    lines.append(f"(ms; event loop lag {result['loop_lag_ms']})")
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="bench/loadgen.py", description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", default="http://127.0.0.1:8000/api/v1")
    parser.add_argument("--clients", type=int, default=2000)
    parser.add_argument("--ramp", type=float, default=60, help="seconds over which clients start")
    parser.add_argument("--duration", type=float, default=600, help="seconds measured after the ramp")
    parser.add_argument("--poll", type=float, default=10)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--mode", choices=("rest", "ws"), default="rest")
    parser.add_argument("--clock-offset", type=float, default=0, help="CLOCK_OFFSET of the API: data ages in replayed time")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    try:
        from uvloop import new_event_loop
    except ImportError:
        new_event_loop = None
    result = asyncio.run(main(args), loop_factory=new_event_loop)
    print(report(result))
    if args.out:
        args.out.write_text(json.dumps(result, indent=1))
