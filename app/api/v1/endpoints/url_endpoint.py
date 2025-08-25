from fastapi import APIRouter, HTTPException, Depends, Query
from app.schema.scraper_schema import AddUrlsRequest, NewsbreakUrlSchema
from app.schema.base import ResponseSchema
from app.api.deps import get_url_pool, get_rabbitmq_channel
from app.core.config import settings
from typing import List
import aio_pika
import logging
import time

logger = logging.getLogger(__name__)

class UrlEndpoint:
    def __init__(self):
        self.router = APIRouter()
        self.setup_routes()
    
    def setup_routes(self):
        """Setup the router routes"""
        self.router.add_api_route(
            "/",
            self.get_newsbreak_urls,
            methods=["GET"],
            response_model=ResponseSchema,
            summary="Retrieve collected NewsBreak URLs with pagination"
        )
        
        self.router.add_api_route(
            "/",
            self.add_urls_to_queue,
            methods=["POST"],
            response_model=ResponseSchema,
            summary="Add URLs to the scraping queue"
        )
        
        self.router.add_api_route(
            "/deduplicate",
            self.deduplicate_urls,
            methods=["POST"],
            response_model=ResponseSchema,
            summary="Run deduplication process on collected URLs"
        )
    
    async def get_newsbreak_urls(self, page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
        """Retrieve collected NewsBreak URLs with pagination"""
        try:
            offset = (page - 1) * limit
            url_pool = get_url_pool()
            
            async with url_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT url, created_at FROM urls
                    ORDER BY created_at DESC
                    LIMIT $1 OFFSET $2
                """, limit, offset)
            
            data = [{
                "url": row["url"],
                "created_at": row["created_at"],
                
            } for row in rows]
            
            return ResponseSchema(
                success=True,
                data={"urls": data, "total_urls": len(data), "page": page, "limit": limit}
            )
            
        except Exception as e:
            logger.error(f"Error getting NewsBreak URLs: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def add_urls_to_queue(self, request: AddUrlsRequest):
        """Add URLs to the scraping queue"""
        try:
            urls_added = 0
            duplicates_skipped = 0
            url_pool = get_url_pool()
            rabbitmq_channel = get_rabbitmq_channel()
            
            # Check for duplicates and add to queue
            for url in request.urls:
                url_str = str(url)
                
                # Check if URL already exists
                async with url_pool.acquire() as conn:
                    exists = await conn.fetchval("SELECT 1 FROM urls WHERE url = $1", url_str)
                    
                    if exists:
                        duplicates_skipped += 1
                    else:
                        # Insert URL into database
                        await conn.execute(
                            "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING", 
                            url_str
                        )
                        
                        # Add to scraper queue
                        await rabbitmq_channel.default_exchange.publish(
                            aio_pika.Message(url_str.encode()),
                            routing_key=settings.scraper_queue
                        )
                        urls_added += 1
            
            # Get current queue size
            scraper_queue = await rabbitmq_channel.declare_queue(
                settings.scraper_queue, 
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": settings.dlx_queue
                }
            )
            
            return ResponseSchema(
                success=True,
                data={
                    "urls_added": urls_added,
                    "duplicates_skipped": duplicates_skipped,
                    "queue_size": scraper_queue.declaration_result.message_count
                }
            )
            
        except Exception as e:
            logger.error(f"Error adding URLs to queue: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def deduplicate_urls(self):
        """Run deduplication process on collected URLs"""
        try:
            start_time = time.time()
            url_pool = get_url_pool()
            
            async with url_pool.acquire() as conn:
                # Count duplicates before deduplication
                initial_count = await conn.fetchval("SELECT COUNT(*) FROM urls")
                
                # Remove duplicates (this should not happen with UNIQUE constraint, but just in case)
                await conn.execute("""
                    DELETE FROM urls a USING (
                        SELECT MIN(ctid) as ctid, url
                        FROM urls
                        GROUP BY url HAVING COUNT(*) > 1
                    ) b
                    WHERE a.url = b.url AND a.ctid <> b.ctid
                """)
                
                # Count after deduplication
                final_count = await conn.fetchval("SELECT COUNT(*) FROM urls")
                
                duplicates_removed = initial_count - final_count
                processing_time = time.time() - start_time
            
            return ResponseSchema(
                success=True,
                data={
                    "duplicates_removed": duplicates_removed,
                    "unique_urls_remaining": final_count,
                    "processing_time": round(processing_time, 2)
                }
            )
            
        except Exception as e:
            logger.error(f"Error deduplicating URLs: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# Create endpoint instance
url_endpoint = UrlEndpoint()
