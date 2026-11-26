"""WebSocket push (GET /api/v1/ws): one connection per client, topics instead of polling.

Client -> server (JSON text):   {"op": "subscribe" | "unsubscribe", "topics": ["vehicles", "stop:{key}", ...]}
Server -> client (JSON text):   {"topic", "type": "snapshot" | "update", "data"} | {"type": "error", "topic", "error"}

    vehicles            every vehicle of the city, for the map (~1,000 at the peak, ~20 kB compressed), which shows
                        the ones in view itself. snapshot: {"timestamp", "vehicles": [Vehicle without `timestamp`]};
                        update: {"timestamp", "upsert": [those that changed or are new], "remove": [vehicle key]}:
                        apply it to the snapshot. A vehicle standing still sends nothing. `timestamp`: the newest
                        position's.
    stop:{stop key}     the departure board, as GET /stops/{key}/departures (the next 20): snapshot, then the whole
                        board again as an update whenever it changed
    trip:{trip key}     the trip's live state, as GET /trips/{key}/live: likewise

The hub follows the process's live state (api/data/live.py): what each change touched (live.Changed) marks topics
dirty (vehicles the vehicles topic; trips themselves; stops their boards; alerts every board and trip), and every
SWEEP_S all of them are (departures leave, data gets old: vehicles not shown any more are removed). A dirty topic is
computed again at most every MIN_INTERVAL_S of its kind, serialized once, and the same text goes to every subscriber.
A connection keeps only the latest message per topic: a slow client gets the newest state, not a queue (for
vehicles, a new snapshot instead of an update it would miss the one before of).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field

import msgspec
import orjson
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from gtfs_backend import keys
from gtfs_backend.api.context import Context
from gtfs_backend.api.data.live import Changed
from gtfs_backend.api.errors import ServiceError
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.logs import log

VEHICLES = "vehicles"
MAX_TOPICS = 50  # per connection
MAX_MESSAGE = 16 * 1024  # bytes a client sends at once
MIN_INTERVAL_S = {VEHICLES: 1.0, "stop": 2.0, "trip": 1.0}
TICK_S = 0.25
SWEEP_S = 15.0
BOARD_LIMIT = 20

router = APIRouter(prefix=PREFIX, tags=["live"])


# ------------------------------------------------------------------------------------------------- topics


@dataclass(frozen=True)
class Topic:
    name: str
    kind: str  # vehicles, stop, trip
    key: str = ""  # the stop or trip key


class TopicError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


TOPICS_HELP = "a topic is vehicles, stop:{key} or trip:{key}"


def parse_topic(ctx: Context, name: str) -> Topic:
    if name == VEHICLES:
        return Topic(VEHICLES, VEHICLES)
    kind, sep, rest = name.partition(":")
    if not sep or not rest:
        raise TopicError("invalid_input", TOPICS_HELP)
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
    raise TopicError("invalid_input", TOPICS_HELP)


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
    data: bytes | None = None  # the current state, as a snapshot sends it
    sent: dict[str, bytes] = field(default_factory=dict)  # vehicles: vehicle key -> its JSON as last sent
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
        self._stop_topics: dict[str, set[str]] = {}  # public_stop_id -> stop topics covering it

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
        if state.data is None:
            await self._compute(state)  # its first state goes to every subscriber as a snapshot
        else:
            conn.push(topic.name, envelope(topic.name, "snapshot", state.data))

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
        if topic.kind == "stop":
            stop = self.ctx.catalog.stops.find(topic.key)
            for pid in self.ctx.catalog.stops.public_stop_ids(stop) if stop else []:
                names = self._stop_topics.setdefault(pid, set())
                (names.add if add else names.discard)(topic.name)
                if not names:
                    del self._stop_topics[pid]

    # --------------------------------------------------------------------------------------- changes

    def on_change(self, changed: Changed) -> None:
        """LiveState listener: called after a change message (or a feed's load) was applied."""
        self._changes.append(changed)
        self._wake.set()

    def _mark(self, changed: Changed) -> None:
        if changed.everything:
            self.dirty.update(self.topics)
            return
        if changed.vehicles:
            self.dirty.add(VEHICLES)
        self.dirty.update(f"trip:{key}" for key in changed.trips)
        for pid in changed.stops:
            self.dirty.update(self._stop_topics.get(pid, ()))
        if changed.alerts:
            self.dirty.update(n for n, s in self.topics.items() if s.topic.kind in ("stop", "trip"))
        self.dirty &= self.topics.keys()

    # --------------------------------------------------------------------------------------- computing

    async def _compute(self, state: TopicState) -> None:
        """The topic's current state; what changed goes to its subscribers (all of it, the first time: also those
        that subscribed while it failed)."""
        topic = state.topic
        state.computed_at = time.monotonic()
        try:
            await self.ctx.catalog.ensure_fresh()  # as REST requests do (deps.py): a new import, a new day
            if topic.kind == VEHICLES:
                await self._compute_vehicles(state)
            else:
                await self._compute_body(state)
        except ServiceError as exc:  # the stop or trip doesn't exist (any more)
            text = error_message(topic.name, exc.code, str(exc))
            for conn in list(state.subscribers):
                conn.push(f"error:{topic.name}", text)
                self.unsubscribe(conn, topic.name)
        except Exception as exc:  # noqa: BLE001 - the next change tries again
            log.warning("ws_topic_failed", kind=topic.kind, error=f"{type(exc).__name__}: {exc}")

    async def _compute_body(self, state: TopicState) -> None:
        """A board or trip: the whole body, as an update whenever it changed."""
        from gtfs_backend.api.services import departures, trips

        if state.topic.kind == "stop":
            model = await departures.board(self.ctx, state.topic.key, None, set(), set(), BOARD_LIMIT)
        else:
            model = await trips.live_by_key(self.ctx, state.topic.key)
        body = model.model_dump_json().encode()
        if body == state.data:
            return
        kind = "snapshot" if state.data is None else "update"
        state.data = body
        text = envelope(state.topic.name, kind, body)
        for conn in state.subscribers:
            conn.push(state.topic.name, text)

    async def _compute_vehicles(self, state: TopicState) -> None:
        """The city's vehicles: an update with those that changed since the last one, and those that went."""
        from gtfs_backend.api.services import vehicles

        newest, current = await vehicles.city(self.ctx)
        upsert = [msgspec.Raw(j) for key, j in current.items() if state.sent.get(key) != j]
        remove = sorted(state.sent.keys() - current.keys())
        first = state.data is None
        state.sent = current
        state.data = msgspec.json.encode({"timestamp": newest, "vehicles": [msgspec.Raw(j) for j in current.values()]})
        if first:
            snapshot = envelope(VEHICLES, "snapshot", state.data)
            for conn in state.subscribers:
                conn.push(VEHICLES, snapshot)
            return
        if not (upsert or remove):
            return
        update = envelope(VEHICLES, "update", msgspec.json.encode({"timestamp": newest, "upsert": upsert, "remove": remove}))
        snapshot = None
        for conn in state.subscribers:
            if conn.has_pending(VEHICLES):  # it hasn't got the last one yet: this update alone would leave a gap
                snapshot = snapshot or envelope(VEHICLES, "snapshot", state.data)
                conn.push(VEHICLES, snapshot)
            else:
                conn.push(VEHICLES, update)

    # --------------------------------------------------------------------------------------- running

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

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
