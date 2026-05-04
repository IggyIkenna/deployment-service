"""deployment-service — FastAPI app.

Exposes /health + /readiness via UTL make_health_router and the orchestration /
state / ml-experiments routers consumed by deployment-api.
"""

from __future__ import annotations

from datetime import date

from fastapi import FastAPI
from unified_trading_library import make_health_router

from deployment_service.api.routes import ml_experiments, orchestration, state

_last_processed_date: date | None = None


def set_last_processed_date(d: date) -> None:
    global _last_processed_date
    _last_processed_date = d


def _data_freshness() -> dict[str, object]:
    if _last_processed_date is None:
        return {"last_processed_date": None, "stale": True}
    return {"last_processed_date": _last_processed_date.isoformat(), "stale": False}


def create_app() -> FastAPI:
    app = FastAPI(
        title="deployment-service",
        version="0.1.1",
        docs_url="/docs",
        redoc_url="/redoc",
    )
    health_router = make_health_router(
        service_name="deployment-service",
        version="0.1.1",
        data_freshness=_data_freshness,
    )
    app.include_router(health_router)
    app.include_router(state.router)
    app.include_router(orchestration.router)
    app.include_router(ml_experiments.router)
    return app


app = create_app()
