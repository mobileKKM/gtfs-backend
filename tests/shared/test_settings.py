"""Env settings: env vars override the defaults, bad values are rejected."""

from datetime import time

import pytest
from pydantic import ValidationError

from gtfs_backend.api.settings import ApiSettings
from gtfs_backend.worker.settings import WorkerSettings


def worker(**env) -> WorkerSettings:
    return WorkerSettings(_env_file=None, **env)


def api(**env) -> ApiSettings:
    return ApiSettings(_env_file=None, **env)


def test_env_vars_override(monkeypatch):
    monkeypatch.setenv("VEHICLE_TTL", "30")
    monkeypatch.setenv("RETIRE_AT", "04:15")
    monkeypatch.setenv("CACHE_TTL_LIVE", "0")
    monkeypatch.setenv("REALTIME_POLL_INTERVAL", "20")
    assert worker().vehicle_ttl == 30 and worker().retire_at == time(4, 15)
    assert worker().realtime_poll_interval == 20
    assert api().cache_ttl_live == 0


def test_removed_settings_are_ignored(monkeypatch):
    # an old .env may still set them; they no longer do anything
    monkeypatch.setenv("HTTP_USER_AGENT", "x/1")
    monkeypatch.setenv("SEARCH_NAMESPACES", "ztp")
    assert not hasattr(worker(), "http_user_agent") and not hasattr(api(), "search_namespaces")


@pytest.mark.parametrize(
    "make, env",
    [
        (worker, {"http_retries": -1}),
        (worker, {"vehicle_ttl": 0}),
        (worker, {"retire_at": "25:00"}),
        (worker, {"db_pool_max_size": 0}),
        (worker, {"realtime_max_age": 0}),
        (api, {"api_port": 70000}),
        (api, {"cache_ttl_static": -1}),
    ],
)
def test_invalid_values_are_rejected(make, env):
    with pytest.raises(ValidationError):
        make(**env)
