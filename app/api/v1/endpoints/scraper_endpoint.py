from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from app.schema.scraper_schema import StartScrapingRequest, StopScrapingRequest, AddUrlsRequest, ScrapingStatusSchema
from app.schema.base import ResponseSchema
from app.core.workers import ScraperWorker
from app.api.deps import get_rabbitmq_channel, get_workers
from app.core.config import settings
import asyncio
import uuid
import logging
import aio_pika
from app.api.deps import get_data_pool, get_url_pool

logger = logging.getLogger(__name__)

class ScraperEndpoint:
    def __init__(self):
        self.router = APIRouter()
        self.scraper_workers = {}
        self.setup_routes()
    
    def setup_routes(self):
        """Setup the router routes"""
        self.router.add_api_route(
            "/start",
            self.start_scraping,
            methods=["POST"],
            response_model=ResponseSchema,
            summary="Start the scraping process"
        )
        
        self.router.add_api_route(
            "/stop",
            self.stop_scraping,
            methods=["POST"],
            response_model=ResponseSchema,
            summary="Stop the scraping process"
        )
        
        self.router.add_api_route(
            "/status",
            self.get_scraping_status,
            methods=["GET"],
            response_model=ResponseSchema,
            summary="Get scraping process status and statistics"
        )
    
    async def start_scraping(self, request: StartScrapingRequest, background_tasks: BackgroundTasks):
        """Start the scraping process"""
        try:
            job_id = str(uuid.uuid4())
            rabbitmq_channel = get_rabbitmq_channel()
            
            # Add initial URLs to scraper queue
            for url in request.initial_urls:
                await rabbitmq_channel.default_exchange.publish(
                    aio_pika.Message(str(url).encode()),
                    routing_key=settings.scraper_queue
                )
            
            # Start workers
            workers_started = 0
            for i in range(request.worker_count):
                worker_id = f"{job_id}_worker_{i}"
                worker = ScraperWorker(worker_id, request.batch_size)
                task = asyncio.create_task(worker.start())
                self.scraper_workers[worker_id] = task
                workers_started += 1
            
            logger.info(f"Started {workers_started} scraper workers for job {job_id}")
            
            return ResponseSchema(
                success=True,
                data={
                    "job_id": job_id,
                    "status": "started",
                    "worker_count": workers_started,
                    "initial_url_count": len(request.initial_urls)
                }
            )
            
        except Exception as e:
            logger.error(f"Error starting scraper: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def stop_scraping(self, request: StopScrapingRequest = None):
        """Stop the scraping process"""
        try:
            stopped_workers = 0
            
            if request and request.job_id:
                # Stop specific job workers
                workers_to_stop = [k for k in self.scraper_workers.keys() if k.startswith(request.job_id)]
            else:
                # Stop all workers
                workers_to_stop = list(self.scraper_workers.keys())
            
            for worker_id in workers_to_stop:
                task = self.scraper_workers.pop(worker_id, None)
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                stopped_workers += 1
            
            return ResponseSchema(
                success=True,
                data={
                    "message": "Scraping process stopped successfully",
                    "stopped_workers": stopped_workers
                }
            )
            
        except Exception as e:
            logger.error(f"Error stopping scraper: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_scraping_status(self):
        """Get scraping process status and statistics"""
        try:
            rabbitmq_channel = get_rabbitmq_channel()
            
            # Declare queues and get their info
            scraper_queue = await rabbitmq_channel.declare_queue(
                settings.scraper_queue, 
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": settings.dlx_queue
                }
            )
            urls_queue = await rabbitmq_channel.declare_queue(settings.newsbreak_urls_queue, durable=True)
            data_queue = await rabbitmq_channel.declare_queue(settings.newsbreak_data_queue, durable=True)
            dlx_queue = await rabbitmq_channel.declare_queue(settings.dlx_queue, durable=True)
            
            # Get database statistics
            data_pool = get_data_pool()
            url_pool = get_url_pool()
            
            async with data_pool.acquire() as conn:
                data_extracted = await conn.fetchval("SELECT COUNT(*) FROM newsbreak_data")
            
            async with url_pool.acquire() as conn:
                urls_processed = await conn.fetchval("SELECT COUNT(*) FROM urls")
                urls_scraped = await conn.fetchval("SELECT COUNT(*) FROM urls WHERE last_scraped IS NOT NULL")
            
            errors_count = dlx_queue.declaration_result.message_count
            status = "running" if self.scraper_workers else "stopped"
            
            return ResponseSchema(
                success=True,
                data={
                    "status": status,
                    "statistics": {
                        "urls_collected": urls_processed or 0,
                        "urls_scraped": urls_scraped or 0,
                        "data_extracted": data_extracted or 0,
                        "errors_count": errors_count,
                        "active_workers": len(self.scraper_workers),
                        "queue_sizes": {
                            "scraper_queue": scraper_queue.declaration_result.message_count,
                            "newsbreak_urls_queue": urls_queue.declaration_result.message_count,
                            "newsbreak_data_queue": data_queue.declaration_result.message_count,
                            "dlx_queue": dlx_queue.declaration_result.message_count
                        }
                    }
                }
            )
            
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# Create endpoint instance
scraper_endpoint = ScraperEndpoint()
