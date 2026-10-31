"""Download retries: ZTP sometimes serves a file while it is being replaced (2026-10-25)."""

import httpx
import pytest
from conftest import FIXTURES

from gtfs_backend.worker import http as http_mod
from gtfs_backend.worker.http import fetch_bytes, fetch_to_file
from gtfs_backend.worker.realtime.poller import parse_feed
from gtfs_backend.worker.static.archive import validate_zip

GOOD = (FIXTURES / "dst_2026-10-25" / "VehiclePositions_T.pb").read_bytes()


RETRY = {"retries": 3, "retry_delay": 0}


def client_serving(*responses):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return responses[min(len(calls) - 1, len(responses) - 1)](request)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), calls


def truncated(request):
    # announces the full length but sends half the bytes
    return httpx.Response(200, headers={"Content-Length": str(len(GOOD))}, content=GOOD[: len(GOOD) // 2])


def corrupt(request):
    return httpx.Response(200, content=GOOD[:-7] + b"\xff\xff\xff\xff\xff\xff\xff")


def good(request):
    return httpx.Response(200, headers={"ETag": '"v2"'}, content=GOOD)


async def test_truncated_body_is_retried():
    client, calls = client_serving(truncated, good)
    async with client:
        res = await fetch_bytes(client, "https://x.test/vp.pb", parse_feed, **RETRY)
    assert len(calls) == 2
    assert len(res.value.entity) > 100
    assert res.etag == '"v2"'


async def test_corrupt_protobuf_is_retried_then_fails():
    client, calls = client_serving(corrupt)
    async with client:
        with pytest.raises(Exception):  # noqa: B017 - protobuf DecodeError or our header check
            await fetch_bytes(client, "https://x.test/vp.pb", parse_feed, **RETRY)
    assert len(calls) == 3


async def test_not_modified_sends_etag():
    client, calls = client_serving(lambda r: httpx.Response(304))
    async with client:
        res = await fetch_bytes(client, "https://x.test/vp.pb", parse_feed, etag='"v1"', **RETRY)
    assert res.not_modified
    assert calls[0].headers["If-None-Match"] == '"v1"'


async def test_bad_zip_is_retried(tmp_path):
    zip_bytes = (FIXTURES / "static" / "GTFS_KRK_M.zip").read_bytes()
    client, calls = client_serving(
        lambda r: httpx.Response(200, content=zip_bytes[:1000]), lambda r: httpx.Response(200, content=zip_bytes)
    )
    async with client:
        res = await fetch_to_file(client, "https://x.test/M.zip", tmp_path / "m.zip", validate_zip, **RETRY)
    assert len(calls) == 2
    assert res.value.read_bytes() == zip_bytes


async def test_empty_file_is_not_an_empty_feed():
    # an empty body parses as a FeedMessage without header; storing it would wipe the map
    client, calls = client_serving(lambda r: httpx.Response(200, content=b""), good)
    async with client:
        res = await fetch_bytes(client, "https://x.test/vp.pb", parse_feed, **RETRY)
    assert len(calls) == 2
    assert len(res.value.entity) > 100


@pytest.mark.parametrize("status", [404, 429, 503])
async def test_missing_file_and_server_errors_are_retried(status):
    # ZTP briefly has no file while replacing it
    client, calls = client_serving(lambda r: httpx.Response(status), good)
    async with client:
        res = await fetch_bytes(client, "https://x.test/vp.pb", parse_feed, **RETRY)
    assert len(calls) == 2 and len(res.value.entity) > 100


async def test_other_client_errors_are_not_retried_and_retries_end():
    client, calls = client_serving(lambda r: httpx.Response(403))
    async with client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_bytes(client, "https://x.test/vp.pb", parse_feed, **RETRY)
    assert len(calls) == 1
    client, calls = client_serving(lambda r: httpx.Response(500))
    async with client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_bytes(client, "https://x.test/vp.pb", parse_feed, **RETRY)
    assert len(calls) == 3


async def test_static_download_retries_a_missing_file(tmp_path):
    zip_bytes = (FIXTURES / "static" / "GTFS_KRK_M.zip").read_bytes()
    client, calls = client_serving(lambda r: httpx.Response(404), lambda r: httpx.Response(200, content=zip_bytes))
    async with client:
        res = await fetch_to_file(client, "https://x.test/M.zip", tmp_path / "m.zip", validate_zip, **RETRY)
    assert len(calls) == 2 and res.value.read_bytes() == zip_bytes


def test_user_agent_is_sent():
    assert http_mod.make_client(30, 10).headers["User-Agent"] == http_mod.USER_AGENT
