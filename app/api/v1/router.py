from fastapi import APIRouter
from app.api.v1.endpoints.scraper_endpoint import scraper_endpoint
from app.api.v1.endpoints.queue_endpoint import queue_endpoint
from app.api.v1.endpoints.data_endpoint import data_endpoint
from app.api.v1.endpoints.url_endpoint import url_endpoint
from app.api.v1.endpoints.health_endpoint import health_endpoint

# Create the main API router
api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(
    scraper_endpoint.router,
    prefix="/scraper",
    tags=["scraper"]
)

api_router.include_router(
    queue_endpoint.router,
    prefix="/queues",
    tags=["queues"]
)

api_router.include_router(
    data_endpoint.router,
    prefix="/newsbreak/data",
    tags=["data"]
)

api_router.include_router(
    url_endpoint.router,
    prefix="/newsbreak/urls",
    tags=["urls"]
)

api_router.include_router(
    health_endpoint.router,
    prefix="/health",
    tags=["health"]
)
