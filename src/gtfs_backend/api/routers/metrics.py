from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from gtfs_backend import metrics as histograms
from gtfs_backend.api.cache import NO_STORE, json_response
from gtfs_backend.api.deps import BareCtx
from gtfs_backend.api.routers import PREFIX
from gtfs_backend.api.schemas.metrics import Metrics
from gtfs_backend.api.services import metrics

router = APIRouter(prefix=PREFIX, tags=["health"])


@router.get("/metrics", response_model=Metrics)
async def get_metrics(
    request: Request,
    ctx: BareCtx,
    window: Annotated[int, Query(ge=60, le=histograms.MAX_WINDOW_S, description="seconds")] = 300,
):
    """Latency per endpoint (p50/p90/p99, by route template, all API processes) and the age of each feed's realtime
    data (now minus its feed header timestamp, sampled at every poll), over the last `window` seconds."""
    return json_response(request, await metrics.summary(ctx, window), NO_STORE)
