"""Errors services raise, and the one error body every failure has: {"error": {"code", "message"}}."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import OperationalError
from psycopg_pool import PoolTimeout
from pydantic import BaseModel
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from starlette.exceptions import HTTPException

from gtfs_backend.keys import InvalidKey


class ServiceError(Exception):
    status_code = 400
    code = "bad_request"


class NotFound(ServiceError):
    status_code = 404
    code = "not_found"

    def __init__(self, what: str):
        super().__init__(f"{what} not found")


class InvalidInput(ServiceError):
    status_code = 422
    code = "invalid_input"


class ErrorDetail(BaseModel):
    code: str  # not_found, invalid_input, method_not_allowed, ...
    message: str


class ErrorBody(BaseModel):
    error: ErrorDetail


# documented on every route (see app.py)
ERROR_RESPONSES = {404: {"model": ErrorBody}, 422: {"model": ErrorBody}, 503: {"model": ErrorBody}}

_HTTP_CODES = {404: "not_found", 405: "method_not_allowed"}


def _error(status: int, code: str, message: str, headers: dict | None = None) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message}}, status_code=status, headers=headers)


def _location(loc: tuple) -> str:
    return ".".join(str(p) for p in loc if p not in ("query", "path", "body"))


def install(app: FastAPI) -> None:
    @app.exception_handler(ServiceError)
    async def service_error(request: Request, exc: ServiceError) -> JSONResponse:
        return _error(exc.status_code, exc.code, str(exc))

    @app.exception_handler(InvalidKey)
    async def invalid_key(request: Request, exc: InvalidKey) -> JSONResponse:
        return _error(422, InvalidInput.code, str(exc))

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        message = "; ".join(f"{_location(e['loc'])}: {e['msg']}" for e in exc.errors())
        return _error(422, InvalidInput.code, message)

    # every connection of this process is busy (or Redis is down): tell the client to retry instead of hanging
    @app.exception_handler(PoolTimeout)
    async def db_busy(request: Request, exc: PoolTimeout) -> JSONResponse:
        return _error(503, "unavailable", "database busy, try again", {"Retry-After": "1"})

    # the database went away mid-request (restart, network): the pool reconnects, the client retries
    @app.exception_handler(OperationalError)
    async def db_down(request: Request, exc: OperationalError) -> JSONResponse:
        return _error(503, "unavailable", "database unavailable, try again", {"Retry-After": "1"})

    @app.exception_handler(RedisConnectionError)
    @app.exception_handler(RedisTimeoutError)
    async def redis_down(request: Request, exc: Exception) -> JSONResponse:
        return _error(503, "unavailable", "live data unavailable, try again", {"Retry-After": "1"})

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException) -> JSONResponse:
        return _error(exc.status_code, _HTTP_CODES.get(exc.status_code, "error"), str(exc.detail), exc.headers)

    # anything else is a bug: the usual error body without details (uvicorn logs the traceback)
    @app.exception_handler(Exception)
    async def internal_error(request: Request, exc: Exception) -> JSONResponse:
        return _error(500, "internal", "internal error")
