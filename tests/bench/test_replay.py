"""bench/replay.py: serving a recording like the operators do, recording from it, and the replay feeds file."""

import importlib.util
import json
import shutil
import threading
import time
from http.server import ThreadingHTTPServer

import httpx
import pytest
import yaml
from conftest import FIXTURES, ROOT

from gtfs_backend.feeds import load_feeds

spec = importlib.util.spec_from_file_location("replay", ROOT / "bench" / "replay.py")
replay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(replay)

T0 = 1_790_000_000.0
TU = (FIXTURES / "dst_2026-10-25" / "TripUpdates_T.pb").read_bytes()
VP = (FIXTURES / "dst_2026-10-25" / "VehiclePositions_T.pb").read_bytes()


def make_recording(folder):
    """kmk_t: trip updates changing at T0 and T0 + 100, vehicles from T0 + 50 on."""
    (folder / "kmk_t" / "trip_updates").mkdir(parents=True)
    (folder / "kmk_t" / "vehicle_positions").mkdir(parents=True)
    shutil.copy(FIXTURES / "static" / "GTFS_KRK_T.zip", folder / "kmk_t" / "static.zip")
    entries = {"trip_updates": [], "vehicle_positions": []}
    later_tu = TU + b"\x00"  # any other body: the server never parses what it serves
    for kind, t, body in (("trip_updates", T0 + 100, later_tu), ("trip_updates", T0, TU), ("vehicle_positions", T0 + 50, VP)):
        name = f"{t:.3f}.pb"
        (folder / "kmk_t" / kind / name).write_bytes(body)
        entries[kind].append({"t": t, "file": f"kmk_t/{kind}/{name}", "sha256": replay._sha(body), "header_ts": 0})
    manifest = {
        "started": T0,
        "ended": T0 + 200,
        "feeds": {
            "kmk_t": {
                "static": "kmk_t/static.zip",
                "static_sha256": replay._sha((folder / "kmk_t" / "static.zip").read_bytes()),
                "realtime": entries,
            }
        },
    }
    (folder / replay.MANIFEST).write_text(json.dumps(manifest))


@pytest.fixture
def server(tmp_path):
    """The recording served as if it were T0 + 20 now."""
    make_recording(tmp_path)
    offset = T0 + 20 - time.time()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), replay.make_handler(replay.Recording(tmp_path), offset))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield tmp_path, f"http://127.0.0.1:{httpd.server_address[1]}", offset
    httpd.shutdown()
    httpd.server_close()


def test_serves_the_version_current_at_the_replayed_moment(server):
    _, url, offset = server
    r = httpx.get(f"{url}/kmk_t/trip_updates.pb")
    assert r.status_code == 200 and r.content == TU and r.headers["etag"]
    assert httpx.get(f"{url}/kmk_t/trip_updates.pb", headers={"If-None-Match": r.headers["etag"]}).status_code == 304
    assert httpx.get(f"{url}/kmk_t/vehicle_positions.pb").status_code == 404  # not yet recorded at T0 + 20
    assert httpx.get(f"{url}/kmk_t/static.zip").content[:2] == b"PK"
    assert httpx.get(f"{url}/kmk_x/trip_updates.pb").status_code == 404
    assert abs(float(httpx.get(f"{url}/clock_offset").text) - offset) < 0.01


def test_recording_lookup_follows_time(tmp_path):
    make_recording(tmp_path)
    rec = replay.Recording(tmp_path)
    assert rec.at("kmk_t", "trip_updates", T0 - 1) is None
    assert rec.at("kmk_t", "trip_updates", T0)["t"] == T0
    assert rec.at("kmk_t", "trip_updates", T0 + 99.9)["t"] == T0
    assert rec.at("kmk_t", "trip_updates", T0 + 500)["t"] == T0 + 100  # after the end: the last version


async def test_feeds_file_points_at_the_server_and_a_new_recording_can_be_made_from_it(server, tmp_path_factory):
    folder, url, _ = server
    out = tmp_path_factory.mktemp("feeds") / "feeds.replay.yml"
    out.write_text(yaml.safe_dump(replay.replay_feeds(folder, ROOT / "feeds.yml", url), sort_keys=False))
    feeds = load_feeds(out)
    tram = feeds.get("kmk_t")
    assert tram.static_url == f"{url}/kmk_t/static.zip" and tram.day_start == "midnight"  # the rest of feeds.yml stays
    assert dict(tram.realtime.items()).keys() == {"trip_updates", "vehicle_positions"}
    assert [f.feed_id for f in feeds.enabled] == ["kmk_t"]  # feeds not recorded are disabled

    rec2 = tmp_path_factory.mktemp("rec2")
    await replay.record(rec2, out, minutes=0.02, only=set())
    manifest = json.loads((rec2 / replay.MANIFEST).read_text())
    files = manifest["feeds"]["kmk_t"]["realtime"]
    assert [e["sha256"] for e in files["trip_updates"]] == [replay._sha(TU)]
    assert files["vehicle_positions"] == []  # 404 the whole time
    assert manifest["feeds"]["kmk_t"]["static_sha256"] == replay._sha((folder / "kmk_t" / "static.zip").read_bytes())
    assert (rec2 / files["trip_updates"][0]["file"]).read_bytes() == TU
