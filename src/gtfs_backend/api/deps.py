"""Dependencies shared by the routers: the service context and parsed query parameters."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal

from fastapi import Depends, Query, Request

from gtfs_backend.api.context import Context
from gtfs_backend.api.data.geo import BBOX_FORMAT, Bbox
from gtfs_backend.api.errors import InvalidInput
from gtfs_backend.keys import InvalidKey, parse_service_date


def bare_context(request: Request) -> Context:
    """The context without refreshing the catalog (works while the database is down)."""
    return request.app.state.ctx


async def context(request: Request) -> Context:
    ctx = bare_context(request)
    await ctx.catalog.ensure_fresh()
    return ctx


def parse_date(value: str) -> date:
    try:
        return parse_service_date(value)
    except InvalidKey as exc:
        raise InvalidInput(str(exc)) from None


def split_csv(values: list[str] | None) -> set[str]:
    """`?modes=tram,bus` and `?modes=tram&modes=bus` alike."""
    return {v.strip() for x in values or [] for v in x.split(",") if v.strip()}


def _bbox(bbox: Annotated[str, Query(description=BBOX_FORMAT)]) -> Bbox:
    return Bbox.parse(bbox)


def _query_date(date_: Annotated[str | None, Query(alias="date", description="YYYYMMDD, default today")] = None):
    return parse_date(date_) if date_ else None


def _modes(modes: Annotated[list[str] | None, Query(description="e.g. tram,bus")] = None) -> set[str]:
    return split_csv(modes)


StopKind = Literal["stop", "group"]


def _stop_kind(
    kind: Annotated[StopKind, Query(description="`stop` (default) or `group`: one stop group per marker")] = "stop",
) -> StopKind:
    return kind


def _feeds(feeds: Annotated[list[str] | None, Query(description="feed ids, e.g. ztp,kmk_t")] = None) -> set[str]:
    return split_csv(feeds)


def _routes(routes: Annotated[list[str] | None, Query(description="route names, e.g. 52,4")] = None) -> set[str]:
    return split_csv(routes)


Ctx = Annotated[Context, Depends(context)]
BareCtx = Annotated[Context, Depends(bare_context)]
BboxQuery = Annotated[Bbox, Depends(_bbox)]
DateQuery = Annotated[date | None, Depends(_query_date)]
ModesQuery = Annotated[set[str], Depends(_modes)]
FeedsQuery = Annotated[set[str], Depends(_feeds)]
RoutesQuery = Annotated[set[str], Depends(_routes)]
StopKindQuery = Annotated[StopKind, Depends(_stop_kind)]
