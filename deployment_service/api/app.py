"""
FastAPI application for the deployment-service HTTP API.

Run with:
    python -m deployment_service --serve --port 9000

Or directly:
    uvicorn deployment_service.api.app:app --host 0.0.0.0 --port 9000
"""

from fastapi import APIRouter, Depends, FastAPI

from deployment_service.api.routes.ml_experiments import router as ml_experiments_router
from deployment_service.api.routes.orchestration import router as orchestration_router
from deployment_service.api.routes.state import router
from deployment_service.auth_s2s import verify_service_token

app = FastAPI(
    title="Deployment Service API",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
)

# All deployment API routes require S2S authentication
_authenticated_router = APIRouter(dependencies=[Depends(verify_service_token)])
_authenticated_router.include_router(router)
_authenticated_router.include_router(orchestration_router)
_authenticated_router.include_router(ml_experiments_router)
app.include_router(_authenticated_router)
