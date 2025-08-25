import aio_pika
import logging
from app.core.config import settings
from app.core.database import db_manager

logger = logging.getLogger(__name__)


class UrlWorker:
    """Worker to process URL queue and insert into database"""
    
    def __init__(self):
        self.is_running = False
        self.processed_count = 0
    
    async def start(self):
        """Start the URL worker"""
        self.is_running = True
        logger.info("URL worker started")
        await self.consume_url_queue()
    
    async def stop(self):
        """Stop the URL worker"""
        self.is_running = False
        logger.info(f"URL worker stopped. Processed {self.processed_count} URLs")
    
    async def consume_url_queue(self):
        """Consume URLs from newsbreak_urls_queue"""
        try:
            urls_queue = await db_manager.get_rabbitmq_channel().declare_queue(
                settings.newsbreak_urls_queue, 
                durable=True
            )
            
            async with urls_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self.is_running:
                        break
                    
                    try:
                        url = message.body.decode()
                        await self.insert_url(url)
                        await message.ack()
                        self.processed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing URL message: {e}")
                        await message.nack(requeue=False)
                        
        except Exception as e:
            logger.error(f"URL worker error: {e}")
    
    async def insert_url(self, url: str):
        """Insert URL into database (with deduplication) and publish to scraper_queue"""
        try:
            async with db_manager.get_url_pool().acquire() as conn:
                await conn.execute(
                    "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING", 
                    url
                )
            # Always publish to scraper queue; dedup handled by ScraperWorker
            try:
                await db_manager.get_rabbitmq_channel().default_exchange.publish(
                    aio_pika.Message(url.encode()),
                    routing_key=settings.scraper_queue
                )
            except Exception as e:
                logger.error(f"Error publishing URL to scraper_queue: {e}")
        except Exception as e:
            logger.error(f"Error inserting URL: {e}")


