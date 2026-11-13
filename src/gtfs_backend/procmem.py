"""This process's memory, as the kernel counts it (Linux only; None elsewhere). Logged by the worker, the import process
and the API so memory over time is visible without a profiler."""

from __future__ import annotations

STATUS = "/proc/self/status"


def _status_mb(field: str) -> int | None:
    try:
        with open(STATUS) as fh:
            for line in fh:
                if line.startswith(field + ":"):
                    return int(line.split()[1]) // 1024  # kB
    except OSError:
        return None
    return None


def rss_mb() -> int | None:
    """Resident memory now."""
    return _status_mb("VmRSS")


def peak_mb() -> int | None:
    """The most resident memory this process has had."""
    return _status_mb("VmHWM")
