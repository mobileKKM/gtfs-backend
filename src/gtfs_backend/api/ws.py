"""WebSocket push (GET /api/v1/ws): one connection per client, topics instead of polling.

Client -> server (JSON text):   {"op": "subscribe" | "unsubscribe", "topics": ["tile:14/9093/5555", "stop:{key}", ...]}
Server -> client (JSON text):   {"topic", "type": "snapshot" | "update", "data"} | {"type": "error", "topic", "error"}

    tile:{z}/{x}/{y}    vehicles in a map tile (slippy map numbering, z 10-16). snapshot: as GET /vehicles for the
                        tile's box; update: {"timestamp", "upsert": [Vehicle], "remove": [vehicle key]}
    stop:{stop key}     the departure board (as GET /stops/{key}/departures, the next 20): snapshot, then the whole
                        board again whenever it changed
    trip:{trip key}     the trip's live state (as GET /trips/{key}/live): likewise

The hub follows the process's live state (api/data/live.py): every change message marks the topics it touches
dirty (a moved vehicle its old and new tile and its trip's board and trip; a changed trip the boards of its stops
and itself; alerts every board and trip), and every SWEEP_S all of them (departures leave, data gets old). A dirty
topic is computed again at most every MIN_INTERVAL_S of its kind, serialized once, and the same text goes to every
subscriber. A connection keeps only the latest message per topic: a slow client gets the newest state, not a queue
(for a tile whose update it hasn't received yet, the current snapshot instead of a second one).
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import math
import time
from dataclasses import dataclass, field

import msgspec
import orjson
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data.geo import Bbox
from gtfs_backend.api.errors import ServiceError
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.logs import log

TILE_ZOOMS = range(10, 17)
MAX_TOPICS = 50  # per connection
MAX_MESSAGE = 16 * 1024  # bytes a client sends at once
MIN_INTERVAL_S = {"tile": 1.0, "stop": 2.0, "trip": 1.0}
TICK_S = 0.25
SWEEP_S = 15.0
BOARD_LIMIT = 20

router = APIRouter(prefix=PREFIX, tags=["live"])


# ------------------------------------------------------------------------------------------------- topics


def tile_of(lat: float, lon: float, z: int) -> tuple[int, int]:
    n = 2**z
    x = int((lon + 180) / 360 * n)
    lat_r = math.radians(max(-85.05112878, min(85.05112878, lat)))
    y = int((1 - math.asinh(math.tan(lat_r)) / math.pi) / 2 * n)
    return min(max(x, 0), n - 1), min(max(y, 0), n - 1)


def tile_bbox(z: int, x: int, y: int) -> Bbox:
    n = 2**z

    def lat(yy: int) -> float:
        return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * yy / n))))

    return Bbox(min_lon=x / n * 360 - 180, min_lat=lat(y + 1), max_lon=(x + 1) / n * 360 - 180, max_lat=lat(y))


@dataclass(frozen=True)
class Topic:
    name: str
    kind: str  # tile, stop, trip
    key: str  # the stop or trip key; for tiles "z/x/y"
    zxy: tuple[int, int, int] | None = None


class TopicError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def parse_topic(ctx: Context, name: str) -> Topic:
    kind, sep, rest = name.partition(":")
    if not sep or not rest:
        raise TopicError("invalid_input", "a topic is tile:z/x/y, stop:{key} or trip:{key}")
    if kind == "tile":
        try:
            z, x, y = (int(p) for p in rest.split("/"))
        except ValueError:
            raise TopicError("invalid_input", "tile:z/x/y with whole numbers") from None
        if z not in TILE_ZOOMS or not (0 <= x < 2**z and 0 <= y < 2**z):
            raise TopicError("invalid_input", f"tile zoom {TILE_ZOOMS.start}-{TILE_ZOOMS.stop - 1}, x and y within it")
        return Topic(f"tile:{z}/{x}/{y}", "tile", f"{z}/{x}/{y}", (z, x, y))
    if kind == "stop":
        stop = ctx.catalog.stops.find(rest)
        if stop is None:
            raise TopicError("not_found", "stop not found")
        return Topic(name, "stop", rest)
    if kind == "trip":
        try:
            keys.parse_trip_key(rest)
        except keys.InvalidKey as exc:
            raise TopicError("invalid_input", str(exc)) from None
        return Topic(name, "trip", rest)
    raise TopicError("invalid_input", "a topic is tile:z/x/y, stop:{key} or trip:{key}")


# ------------------------------------------------------------------------------------------------- connections


class Connection:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.topics: set[str] = set()
        self._pending: dict[str, str] = {}  # topic -> the latest message for it not sent yet
        self._wake = asyncio.Event()
        self.closed = False

    def push(self, topic: str, text: str) -> None:
        self._pending[topic] = text
        self._wake.set()

    def has_pending(self, topic: str) -> bool:
        return topic in self._pending

    async def send_loop(self) -> None:
        while not self.closed:
            await self._wake.wait()
            self._wake.clear()
            while self._pending:
                topic, text = next(iter(self._pending.items()))
                del self._pending[topic]
                await self.ws.send_text(text)


def envelope(topic: str, kind: str, data: bytes) -> str:
    return (b'{"topic":' + orjson.dumps(topic) + b',"type":"' + kind.encode() + b'","data":' + data + b"}").decode()


def error_message(topic: str | None, code: str, message: str) -> str:
    return msgspec.json.encode({"type": "error", "topic": topic, "error": {"code": code, "message": message}}).decode()


@dataclass
class TopicState:
    topic: Topic
    subscribers: set[Connection] = field(default_factory=set)
    snapshot: str | None = None  # the current state as a snapshot message, for new subscribers
    data: bytes | None = None  # stops and trips: the last body sent
    pieces: dict[str, bytes] = field(default_factory=dict)  # tiles: vehicle key -> its JSON as last sent
    computed_at: float = 0.0


# ------------------------------------------------------------------------------------------------- hub


class Hub:
    def __init__(self, ctx: Context):
        self.ctx = ctx
        self.topics: dict[str, TopicState] = {}
        self.dirty: set[str] = set()
        self._changes: list[dict] = []
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self.connections: set[Connection] = set()
        # what the change messages said last, to know what they move away from
        self._vehicle_at: dict[str, tuple[float, float, str | None]] = {}  # vehicle key -> (lat, lon, trip key)
        self._trip_pids: dict[str, frozenset[str]] = {}  # trip key -> its stops
        self._stop_topics: dict[str, set[str]] = {}  # public_stop_id -> stop topics covering it
        self._zooms: dict[int, int] = {}  # tile zoom -> subscribed tiles

    # --------------------------------------------------------------------------------------- subscriptions

    async def subscribe(self, conn: Connection, name: str) -> None:
        try:
            topic = parse_topic(self.ctx, name)
        except TopicError as exc:
            conn.push(f"error:{name}", error_message(name, exc.code, str(exc)))
            return
        if topic.name in conn.topics:
            return
        if len(conn.topics) >= MAX_TOPICS:
            conn.push(f"error:{name}", error_message(name, "invalid_input", f"at most {MAX_TOPICS} topics"))
            return
        state = self.topics.get(topic.name)
        if state is None:
            state = self.topics[topic.name] = TopicState(topic)
            self._index(topic, add=True)
        state.subscribers.add(conn)
        conn.topics.add(topic.name)
        if state.snapshot is None:
            await self._compute(state)
        if state.snapshot is not None and topic.name in self.topics:
            conn.push(topic.name, state.snapshot)

    def unsubscribe(self, conn: Connection, name: str) -> None:
        conn.topics.discard(name)
        state = self.topics.get(name)
        if state is None:
            return
        state.subscribers.discard(conn)
        if not state.subscribers:
            del self.topics[name]
            self.dirty.discard(name)
            self._index(state.topic, add=False)

    def drop(self, conn: Connection) -> None:
        for name in list(conn.topics):
            self.unsubscribe(conn, name)
        conn.closed = True
        self.connections.discard(conn)

    def _index(self, topic: Topic, add: bool) -> None:
        if topic.kind == "tile":
            z = topic.zxy[0]
            self._zooms[z] = self._zooms.get(z, 0) + (1 if add else -1)
            if not self._zooms[z]:
                del self._zooms[z]
        elif topic.kind == "stop":
            stop = self.ctx.catalog.stops.find(topic.key)
            for pid in self.ctx.catalog.stops.public_stop_ids(stop) if stop else []:
                names = self._stop_topics.setdefault(pid, set())
                (names.add if add else names.discard)(topic.name)
                if not names:
                    del self._stop_topics[pid]

    # --------------------------------------------------------------------------------------- changes

    def on_change(self, message: dict) -> None:
        """LiveState listener: called after a change message (or a feed's load) was applied."""
        self._changes.append(message)
        self._wake.set()

    def _mark(self, message: dict) -> None:
        live = self.ctx.live_state
        if message.get("loaded") or live is None:  # a whole feed was loaded again: everything may have changed
            self._rebuild()
            self.dirty.update(self.topics)
            return
        if (vehicles := message.get("vehicles")) is not None:
            for key in [*vehicles.get("set", {}), *vehicles.get("del", ())]:
                old = self._vehicle_at.pop(key, None)
                v = live.vehicle(key)
                new = (v["lat"], v["lon"], v["trip"]["key"]) if v else None
                if new is not None:
                    self._vehicle_at[key] = new
                for pos in (old, new):
                    if pos is None:
                        continue
                    for z in self._zooms:
                        x, y = tile_of(pos[0], pos[1], z)
                        self.dirty.add(f"tile:{z}/{x}/{y}")
                    self._trip_touched(pos[2])
        if (trips := message.get("trips")) is not None:
            for key in [*trips.get("set", {}), *trips.get("del", ())]:
                self._trip_touched(key, reindex=True)
        if message.get("alerts") is not None:
            self.dirty.update(n for n, s in self.topics.items() if s.topic.kind in ("stop", "trip"))
        self.dirty &= self.topics.keys()

    def _trip_touched(self, trip_key: str | None, reindex: bool = False) -> None:
        if trip_key is None:
            return
        old = self._trip_pids.get(trip_key, frozenset())
        if reindex:
            trip = self.ctx.live_state.trips.get(trip_key)
            new = frozenset(e[1] for e in trip[6] if e[1]) if trip else frozenset()
            if new:
                self._trip_pids[trip_key] = new
            else:
                self._trip_pids.pop(trip_key, None)
            pids = old | new
        else:
            pids = old
        for pid in pids:
            self.dirty.update(self._stop_topics.get(pid, ()))
        self.dirty.add(f"trip:{trip_key}")

    def _rebuild(self) -> None:
        live = self.ctx.live_state
        self._vehicle_at = {k: (v["lat"], v["lon"], v["trip"]["key"]) for k, (_, v) in live.vehicles.items()}
        self._trip_pids = {k: frozenset(e[1] for e in t[6] if e[1]) for k, t in live.trips.items()}

    # --------------------------------------------------------------------------------------- computing

    async def _compute(self, state: TopicState) -> None:
        """The topic's current state; pushes an update to its subscribers if it changed."""
        topic = state.topic
        state.computed_at = time.monotonic()
        try:
            if topic.kind == "tile":
                await self._compute_tile(state)
            else:
                await self._compute_body(state)
        except ServiceError as exc:  # the stop or trip doesn't exist (any more)
            text = error_message(topic.name, exc.code, str(exc))
            for conn in list(state.subscribers):
                conn.push(f"error:{topic.name}", text)
                self.unsubscribe(conn, topic.name)
        except Exception as exc:  # noqa: BLE001 - a store down: the next change tries again
            log.warning("ws_topic_failed", kind=topic.kind, error=f"{type(exc).__name__}: {exc}")

    async def _compute_tile(self, state: TopicState) -> None:
        from gtfs_backend.api.services import vehicles

        bbox = tile_bbox(*state.topic.zxy)
        pieces = await vehicles._in_bbox(self.ctx, bbox, set(), set())
        current = {p.model.key: p.json for p in pieces}
        newest = max((p.timestamp for p in pieces if p.timestamp is not None), default=None)
        body = (
            b'{"timestamp":'
            + (b"null" if newest is None else str(newest).encode())
            + b',"vehicles":['
            + b",".join(current.values())
            + b"]}"
        )
        first = state.snapshot is None
        state.snapshot = envelope(state.topic.name, "snapshot", body)
        if first:
            state.pieces = current
            return
        upsert = [msgspec.Raw(j) for k, j in current.items() if state.pieces.get(k) != j]
        remove = sorted(state.pieces.keys() - current.keys())
        state.pieces = current
        if not upsert and not remove:
            return
        update = envelope(
            state.topic.name, "update", msgspec.json.encode({"timestamp": newest, "upsert": upsert, "remove": remove})
        )
        for conn in state.subscribers:
            # a client that hasn't got the last update yet gets the whole tile instead: diffs never pile up
            conn.push(state.topic.name, state.snapshot if conn.has_pending(state.topic.name) else update)

    async def _compute_body(self, state: TopicState) -> None:
        from gtfs_backend.api.services import departures, trips

        if state.topic.kind == "stop":
            model = await departures.board(self.ctx, state.topic.key, None, set(), set(), BOARD_LIMIT)
        else:
            model = await trips.live_by_key(self.ctx, state.topic.key)
        body = model.model_dump_json().encode()
        if body == state.data:
            return
        first = state.data is None
        state.data = body
        state.snapshot = envelope(state.topic.name, "snapshot", body)
        if not first:
            text = envelope(state.topic.name, "update", body)
            for conn in state.subscribers:
                conn.push(state.topic.name, text)

    # --------------------------------------------------------------------------------------- running

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), context=contextvars.Context())

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        for conn in list(self.connections):
            with contextlib.suppress(Exception):
                await conn.ws.close(code=1001)

    async def _run(self) -> None:
        swept = time.monotonic()
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), TICK_S)
            self._wake.clear()
            try:
                changes, self._changes = self._changes, []
                for message in changes:
                    self._mark(message)
                now = time.monotonic()
                if now - swept > SWEEP_S:
                    self.dirty.update(self.topics)
                    swept = now
                for name in list(self.dirty):
                    state = self.topics.get(name)
                    if state is None:
                        self.dirty.discard(name)
                        continue
                    if now - state.computed_at < MIN_INTERVAL_S[state.topic.kind]:
                        continue  # again soon: it stays dirty
                    self.dirty.discard(name)
                    await self._compute(state)
                    await asyncio.sleep(0)  # requests get their turn between topics
            except Exception as exc:  # noqa: BLE001
                log.warning("ws_hub_failed", error=f"{type(exc).__name__}: {exc}")


@router.websocket("/ws")
async def live_socket(websocket: WebSocket):
    """Push instead of polling: subscribe to topics, get a snapshot of each, then its changes (see api/ws.py)."""
    hub: Hub = websocket.app.state.hub
    await websocket.accept()
    conn = Connection(websocket)
    hub.connections.add(conn)
    sender = asyncio.create_task(conn.send_loop())
    try:
        while True:
            text = await websocket.receive_text()
            if len(text) > MAX_MESSAGE:
                conn.push("error:", error_message(None, "invalid_input", f"messages are at most {MAX_MESSAGE} bytes"))
                continue
            try:
                msg = orjson.loads(text)
                op, names = msg["op"], msg["topics"]
                if op not in ("subscribe", "unsubscribe") or not isinstance(names, list):
                    raise ValueError
            except (ValueError, KeyError, TypeError):
                conn.push(
                    "error:", error_message(None, "invalid_input", 'send {"op": "subscribe"|"unsubscribe", "topics": [...]}')
                )
                continue
            for name in names:
                if not isinstance(name, str):
                    continue
                if op == "subscribe":
                    await hub.subscribe(conn, name)
                else:
                    hub.unsubscribe(conn, name)
    except WebSocketDisconnect:
        pass
    finally:
        hub.drop(conn)
        sender.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await sender
