"""Env settings: env vars override the defaults, bad values are rejected."""

import pytest
from pydantic import ValidationError

from gtfs_backend.api.settings import ApiSettings
from gtfs_backend.settings import Settings


def shared(**env) -> Settings:
    """The settings the API shares with the worker (the worker, in Rust, reads its own: worker/src/config.rs)."""
    return Settings(_env_file=None, **env)


def api(**env) -> ApiSettings:
    return ApiSettings(_env_file=None, **env)


def test_env_vars_override(monkeypatch):
    monkeypatch.setenv("REALTIME_MAX_AGE", "30")
    monkeypatch.setenv("CACHE_TTL_STATIC", "0")
    assert shared().realtime_max_age == 30 and api().realtime_max_age == 30
    assert api().cache_ttl_static == 0


def test_removed_settings_are_ignored(monkeypatch):
    # an old .env may still set them; they no longer do anything
    monkeypatch.setenv("HTTP_USER_AGENT", "x/1")
    monkeypatch.setenv("SEARCH_NAMESPACES", "ztp")
    monkeypatch.setenv("CACHE_TTL_LIVE", "3")  # the response cache is gone
    assert not hasattr(api(), "http_user_agent") and not hasattr(api(), "search_namespaces")
    assert not hasattr(api(), "cache_ttl_live")


@pytest.mark.parametrize(
    "make, env",
    [
        (shared, {"db_pool_max_size": 0}),
        (shared, {"realtime_max_age": 0}),
        (api, {"api_port": 70000}),
        (api, {"cache_ttl_static": -1}),
    ],
)
def test_invalid_values_are_rejected(make, env):
    with pytest.raises(ValidationError):
        make(**env)
