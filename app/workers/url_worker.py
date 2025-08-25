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
        self._channel = None
    
    async def start(self):
        """Start the URL worker"""
        self.is_running = True
        logger.info("URL worker started")
        # Create dedicated channel with higher prefetch for URL consumption
        try:
            connection = db_manager.get_rabbitmq_connection()
            if connection:
                self._channel = await connection.channel()
                await self._channel.set_qos(prefetch_count=200)
        except Exception as e:
            logger.error(f"Failed to create dedicated channel for UrlWorker: {e}")
        await self.consume_url_queue()
    
    async def stop(self):
        """Stop the URL worker"""
        self.is_running = False
        logger.info(f"URL worker stopped. Processed {self.processed_count} URLs")
        if self._channel and not self._channel.is_closed:
            try:
                await self._channel.close()
            except Exception:
                pass
    
    async def consume_url_queue(self):
        """Consume URLs from newsbreak_urls_queue"""
        try:
            channel = self._channel or db_manager.get_rabbitmq_channel()
            urls_queue = await channel.declare_queue(
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
        """Insert URL into DB; publish to scraper_queue only if newly inserted."""
        try:
            inserted = False
            async with db_manager.get_url_pool().acquire() as conn:
                # RETURNING returns a row only when an insert actually happened
                inserted = await conn.fetchval(
                    "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING RETURNING url",
                    url
                ) is not None

            if inserted:
                try:
                    await db_manager.get_rabbitmq_channel().default_exchange.publish(
                        aio_pika.Message(url.encode()),
                        routing_key=settings.scraper_queue
                    )
                except Exception as e:
                    logger.error(f"Error publishing URL to scraper_queue: {e}")
        except Exception as e:
            logger.error(f"Error inserting URL: {e}")


