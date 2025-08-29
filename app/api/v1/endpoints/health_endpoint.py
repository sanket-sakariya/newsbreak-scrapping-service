from fastapi import APIRouter, Depends
from app.schema.base import ResponseSchema
from app.api.deps import get_data_pool, get_url_pool, get_rabbitmq_channel, get_workers
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class HealthEndpoint:
    def __init__(self):
        self.router = APIRouter()
        self.setup_routes()
    
    def setup_routes(self):
        """Setup the router routes"""
        self.router.add_api_route(
            "/",
            self.health_check,
            methods=["GET"],
            response_model=ResponseSchema,
            summary="Health check endpoint"
        )
    
    async def health_check(self):
        """Health check endpoint"""
        try:
            # Check database connections
            data_pool = get_data_pool()
            url_pool = get_url_pool()
            
            async with data_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            async with url_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            # Check RabbitMQ connection
            rabbitmq_channel = get_rabbitmq_channel()
            await rabbitmq_channel.declare_queue("health_check", durable=True)
            
            # Get worker status
            workers = get_workers()
            worker_status = {
                "scraper_workers": 0,  # This will be managed by scraper endpoint
                "data_worker_running": workers['data_worker'].is_running,
                "url_worker_running": workers['url_worker'].is_running,
                "url_feeder_running": workers['url_feeder'].is_running
            }
            
            return ResponseSchema(
                success=True,
                data={
                    "status": "healthy",
                    "worker_status": worker_status,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return ResponseSchema(
                success=False,
                data={
                    "status": "unhealthy",
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

# Create endpoint instance
health_endpoint = HealthEndpoint()
