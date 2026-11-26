"""One load run, the same way every time: replay a recording, run the worker and the API on it, k6 against the API,
and save the results in a folder (docs/benchmark.md).

    uv run python bench/load.py import bench/out/rec-peak                      # its timetables into the bench DB, once
    uv run python bench/load.py run bench/out/rec-peak --out bench/out/results/phase1 [--code ../wt-phase1]

`run`: replay server -> worker + API (CLOCK_OFFSET, API_WORKERS) -> wait for /health `fresh` -> warm-up -> load
(bench/loadgen.py of this checkout: the same load whatever code runs) -> /metrics, /health, the API's CPU and memory ->
everything stopped. `--code` runs another checkout's worker and API (a git worktree of an older
commit: before/after numbers). Uses its own database and Redis db (BENCH_DATABASE_URL, BENCH_REDIS_URL; the Redis db
is flushed first), so dev data stays as it is.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATABASE_URL = os.environ.get("BENCH_DATABASE_URL", "postgresql://gtfs:gtfs@localhost:5432/gtfs_replay")
REDIS_URL = os.environ.get("BENCH_REDIS_URL", "redis://localhost:6379/14")
REPLAY_PORT, API_PORT = 8765, 8000
REPLAY = f"http://127.0.0.1:{REPLAY_PORT}"
API = f"http://127.0.0.1:{API_PORT}/api/v1"


def env(feeds: Path, offset: float | None = None, **extra) -> dict:
    e = dict(os.environ)
    e.update(
        DATABASE_URL=DATABASE_URL,
        REDIS_URL=REDIS_URL,
        FEEDS_CONFIG=str(feeds),
        ARCHIVE_DIR=str(ROOT / "bench" / "out" / "archive"),
        SNAPSHOT_DIR=str(ROOT / "bench" / "out" / "snapshot"),
        OSM_REFRESH_INTERVAL="0",  # the OSM poles file that is there; never a download
        OSM_STOPS_FILE=str(ROOT / "data" / "osm" / "stops.json"),
        LOG_LEVEL="WARNING",
        **{k: str(v) for k, v in extra.items()},
    )
    if offset is not None:
        e["CLOCK_OFFSET"] = f"{offset:.3f}"
    return e


def get(url: str, timeout: float = 5) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310 - our own local services
        return r.read()


def wait_for(url: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            get(url, timeout=1)
            return
        except OSError:
            time.sleep(0.5)
    raise SystemExit(f"{url} didn't come up in {seconds:g} s")


def _more_files() -> None:
    """Each API process holds hundreds of client sockets (macOS allows 256 open files by default)."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(16384, hard), hard))


def start(cmd: list[str], cwd: Path, environment: dict, log: Path) -> subprocess.Popen:
    fh = log.open("w")
    return subprocess.Popen(
        cmd, cwd=cwd, env=environment, stdout=fh, stderr=subprocess.STDOUT, start_new_session=True, preexec_fn=_more_files
    )


def stop(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        if p.poll() is None:
            os.killpg(p.pid, signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)


def replay_feeds(recording: Path, out: Path) -> Path:
    feeds = out / "feeds.replay.yml"
    subprocess.run(
        [sys.executable, str(ROOT / "bench" / "replay.py"), "feeds", str(recording), "--url", REPLAY, "-o", str(feeds)],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    return feeds


def serve(recording: Path, out: Path) -> tuple[subprocess.Popen, float]:
    p = start(
        [sys.executable, str(ROOT / "bench" / "replay.py"), "serve", str(recording), "--port", str(REPLAY_PORT)],
        ROOT,
        dict(os.environ),
        out / "replay.log",
    )
    wait_for(f"{REPLAY}/clock_offset", 15)
    return p, float(get(f"{REPLAY}/clock_offset"))


def descendants(pid: int) -> list[int]:
    out, todo = [], [pid]
    while todo:
        p = todo.pop()
        children = subprocess.run(["pgrep", "-P", str(p)], capture_output=True, text=True).stdout.split()
        out += [int(c) for c in children]
        todo += [int(c) for c in children]
    return out


def sample(pids: list[int]) -> tuple[float, int]:
    """(summed %CPU, summed RSS in kB) of these processes now."""
    if not pids:
        return 0.0, 0
    lines = subprocess.run(["ps", "-o", "%cpu=,rss=", "-p", ",".join(map(str, pids))], capture_output=True, text=True).stdout
    cpu = rss = 0.0
    for line in lines.splitlines():
        c, r = line.split()
        cpu, rss = cpu + float(c), rss + int(r)
    return cpu, int(rss)


def cmd_import(args) -> None:
    out = ROOT / "bench" / "out" / "import"
    out.mkdir(parents=True, exist_ok=True)
    feeds = replay_feeds(args.recording, out)
    server, _ = serve(args.recording, out)
    try:
        subprocess.run(["uv", "run", "gtfs-worker", "import-static", "--all"], cwd=args.code, env=env(feeds), check=True)
    finally:
        stop([server])


def cmd_run(args) -> None:
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)
    feeds = replay_feeds(args.recording, out)
    subprocess.run(
        ["uv", "run", "python", "-c", f"import redis; redis.Redis.from_url({REDIS_URL!r}).flushdb()"], cwd=ROOT, check=True
    )
    server, offset = serve(args.recording, out)
    procs = [server]
    try:
        procs.append(start(["uv", "run", "gtfs-worker", "run"], args.code, env(feeds, offset), out / "worker.log"))
        api = start(
            ["uv", "run", "gtfs-api", "--host", "127.0.0.1", "--port", str(API_PORT), "--workers", str(args.workers)],
            args.code,
            env(feeds, offset, API_WORKERS=args.workers),
            out / "api.log",
        )
        procs.append(api)
        wait_for(f"{API}/health", 120)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            health = json.loads(get(f"{API}/health"))
            if health.get("fresh"):
                break
            time.sleep(2)
        else:
            print("not fresh:", [(f["feed_id"], f["problems"]) for f in health["feeds"] if f["problems"]], file=sys.stderr)
        print(f"fresh; warming up {args.warmup:g} s")
        time.sleep(args.warmup)
        load = subprocess.Popen(
            [
                sys.executable, str(ROOT / "bench" / "loadgen.py"), "--base", API,
                "--clients", str(args.clients), "--ramp", str(args.ramp), "--duration", str(args.duration),
                "--mode", args.mode, "--clock-offset", f"{offset:.3f}",
                "--out", str(out / "load.json"),
            ],
            stdout=(out / "load.log").open("w"),
            stderr=subprocess.STDOUT,
            preexec_fn=_more_files,
        )  # fmt: skip
        started = time.monotonic()
        samples = []
        while load.poll() is None:
            time.sleep(5)
            if time.monotonic() - started > args.ramp:  # steady state only
                samples.append(sample(descendants(api.pid)))
        run_s = time.monotonic() - started
        (out / "metrics.json").write_bytes(get(f"{API}/metrics?window={min(3600, int(run_s) + 60)}"))
        (out / "health.json").write_bytes(get(f"{API}/health"))
        resources = {
            "api_workers": args.workers,
            "mode": args.mode,
            "api_cpu_percent_mean": round(sum(c for c, _ in samples) / max(len(samples), 1), 1),
            "api_cpu_percent_max": max((c for c, _ in samples), default=0),
            "api_rss_mb_max": round(max((r for _, r in samples), default=0) / 1024, 1),
            "load_exit": load.returncode,
            "recording": str(args.recording),
            "recording_manifest_sha256": __import__("hashlib")
            .sha256((args.recording / "manifest.json").read_bytes())
            .hexdigest(),
            "code": str(args.code),
            "commit": subprocess.run(["git", "rev-parse", "HEAD"], cwd=args.code, capture_output=True, text=True).stdout.strip(),
        }
        (out / "resources.json").write_text(json.dumps(resources, indent=1))
        print((out / "load.log").read_text()[-3000:])
        print(json.dumps(resources, indent=1))
    finally:
        stop(procs[::-1])


def main() -> None:
    parser = argparse.ArgumentParser(prog="bench/load.py", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("import", help="import a recording's timetables into the bench database")
    p.add_argument("recording", type=Path)
    p.add_argument("--code", type=Path, default=ROOT)
    p = sub.add_parser("run", help="one load run")
    p.add_argument("recording", type=Path)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--code", type=Path, default=ROOT, help="checkout whose worker and API run (default: this one)")
    p.add_argument("--clients", type=int, default=2000)
    p.add_argument("--duration", type=float, default=600, help="seconds measured, after the ramp")
    p.add_argument("--ramp", type=float, default=60, help="seconds over which clients start")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--warmup", type=float, default=30)
    p.add_argument("--mode", choices=("rest", "ws"), default="rest", help="clients poll REST, or get it pushed (WebSocket)")
    args = parser.parse_args()
    args.recording = args.recording.resolve()
    args.code = args.code.resolve()
    {"import": cmd_import, "run": cmd_run}[args.command](args)


if __name__ == "__main__":
    main()
