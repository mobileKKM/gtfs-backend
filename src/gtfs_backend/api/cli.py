"""gtfs-api command line: serves the HTTP API (/api/v1, docs at /api/docs).

gtfs-api [--host 0.0.0.0] [--port 8000] [--workers 4]

With more than one worker, uvicorn starts that many processes on the same port and restarts any that die. Each
process builds its own app (create_app), so it has its own connection pools and catalog.

uvicorn's access log is off: it would record client IPs and full URLs (viewport boxes, searches, stops looked
at). Requests aren't logged at all.
"""

from __future__ import annotations

import argparse

import uvicorn

from gtfs_backend.api.settings import get_api_settings
from gtfs_backend.logs import setup_logging


def main() -> None:
    settings = get_api_settings()
    parser = argparse.ArgumentParser(prog="gtfs-api", description="mobileKKM GTFS API")
    parser.add_argument("--host", default=settings.api_host)
    parser.add_argument("--port", type=int, default=settings.api_port)
    parser.add_argument("--workers", type=int, default=settings.api_workers, help="processes (API_WORKERS)")
    args = parser.parse_args()
    setup_logging(settings.log_level)
    uvicorn.run(
        "gtfs_backend.api.app:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        workers=args.workers,
        access_log=False,
        proxy_headers=False,  # client addresses are never used, so don't resolve them from X-Forwarded-For
        server_header=False,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
