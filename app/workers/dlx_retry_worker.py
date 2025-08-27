import asyncio
import aio_pika
import json
import logging
from datetime import datetime, timedelta
from typing import Optional
from app.core.config import settings
from app.core.database import db_manager

logger = logging.getLogger(__name__)


class DLXRetryWorker:
    def __init__(self, retry_delay: int = 300):  # 5 minutes default delay
        self.is_running = False
        self.retry_delay = retry_delay  # Delay between retries in seconds
        self.max_retries = 3  # Maximum retry attempts
        self.processed_count = 0

    async def start(self):
        self.is_running = True
        logger.info("DLX Retry worker started")
        await self.consume_dlx_queue()

    async def stop(self):
        self.is_running = False
        logger.info(f"DLX Retry worker stopped. Processed {self.processed_count} retries")

    async def consume_dlx_queue(self):
        try:
            dlx_queue = await db_manager.get_rabbitmq_channel().declare_queue(
                settings.dlx_queue,
                durable=True
            )
            
            async with dlx_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self.is_running:
                        break

                    try:
                        await self.process_dlx_message(message)
                        await message.ack()
                        self.processed_count += 1
                        
                        if self.processed_count % 10 == 0:
                            logger.info(f"DLX Retry worker processed {self.processed_count} messages")
                            
                    except Exception as e:
                        logger.error(f"Error processing DLX message: {e}")
                        await message.nack(requeue=False)
                        
        except Exception as e:
            logger.error(f"DLX Retry worker error: {e}")

    async def process_dlx_message(self, message: aio_pika.IncomingMessage):
        try:
            # Parse the DLX message
            message_data = json.loads(message.body.decode())
            url = message_data.get('url')
            error = message_data.get('error')
            retry_count = message_data.get('retry_count', 0)
            original_timestamp = message_data.get('timestamp')
            
            logger.info(f"Processing DLX message for URL: {url}, retry count: {retry_count}")
            
            # Check if we should retry
            if retry_count >= self.max_retries:
                logger.warning(f"URL {url} exceeded max retries ({self.max_retries}), discarding")
                return
            
            # Wait for retry delay before re-queueing
            await asyncio.sleep(self.retry_delay)
            
            # Increment retry count
            new_retry_count = retry_count + 1
            
            # Create enhanced message data for tracking
            enhanced_data = {
                'url': url,
                'retry_count': new_retry_count,
                'original_error': error,
                'original_timestamp': original_timestamp,
                'retry_timestamp': datetime.utcnow().isoformat()
            }
            
            # Send URL back to scraper queue with retry tracking
            await db_manager.get_rabbitmq_channel().default_exchange.publish(
                aio_pika.Message(
                    json.dumps(enhanced_data).encode(), 
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=settings.scraper_queue
            )
            
            logger.info(f"Re-queued URL {url} to scraper (attempt {new_retry_count}/{self.max_retries})")
            
        except Exception as e:
            logger.error(f"Error processing DLX message: {e}")
            raise
