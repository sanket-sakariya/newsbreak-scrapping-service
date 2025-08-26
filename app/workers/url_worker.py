import aio_pika
import logging
import asyncio
from typing import List
from app.core.config import settings
from app.core.database import db_manager

logger = logging.getLogger(__name__)


class UrlWorker:
    """Worker to process URL queue and insert into database"""
    
    def __init__(self):
        self.is_running = False
        self.processed_count = 0
        self._channel = None
        self._buffer: List[str] = []
        self._buffer_msgs: List[aio_pika.IncomingMessage] = []
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._batch_size = getattr(settings, 'default_batch_size', 100)
        self._flush_interval = getattr(settings, 'batch_flush_seconds', 5)
    
    async def start(self):
        """Start the URL worker"""
        self.is_running = True
        logger.info("URL worker started")
        # Create dedicated channel with higher prefetch for URL consumption
        try:
            connection = db_manager.get_rabbitmq_connection()
            if connection:
                self._channel = await connection.channel()
                await self._channel.set_qos(prefetch_count=max(200, self._batch_size))
        except Exception as e:
            logger.error(f"Failed to create dedicated channel for UrlWorker: {e}")
        # Start periodic flush
        self._flush_task = asyncio.create_task(self._periodic_flush())
        await self.consume_url_queue()
    
    async def stop(self):
        """Stop the URL worker"""
        self.is_running = False
        logger.info(f"URL worker stopped. Processed {self.processed_count} URLs")
        # Flush remaining buffered URLs
        try:
            if self._flush_task:
                self._flush_task.cancel()
                try:
                    await self._flush_task
                except asyncio.CancelledError:
                    pass
            async with self._lock:
                if self._buffer:
                    urls, msgs = self._buffer[:], self._buffer_msgs[:]
                    self._buffer.clear(); self._buffer_msgs.clear()
                else:
                    urls, msgs = [], []
            if urls:
                await self._insert_and_publish_batch(urls, msgs)
        except Exception as e:
            logger.error(f"Error flushing URL batch on stop: {e}")
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
                        # Buffer for batch insert
                        async with self._lock:
                            self._buffer.append(url)
                            self._buffer_msgs.append(message)
                            should_flush = len(self._buffer) >= self._batch_size
                        if should_flush:
                            # Snapshot outside lock
                            async with self._lock:
                                urls = self._buffer[:]
                                msgs = self._buffer_msgs[:]
                                self._buffer.clear(); self._buffer_msgs.clear()
                            await self._insert_and_publish_batch(urls, msgs)
                        
                    except Exception as e:
                        logger.error(f"Error processing URL message: {e}")
                        await message.nack(requeue=False)
                        
        except Exception as e:
            logger.error(f"URL worker error: {e}")
    
    async def insert_url(self, url: str):
        """(Kept for compatibility) Insert single URL; prefer batch path."""
        try:
            async with db_manager.get_url_pool().acquire() as conn:
                inserted = await conn.fetchval(
                    "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING RETURNING url",
                    url
                ) is not None
            if inserted:
                await db_manager.get_rabbitmq_channel().default_exchange.publish(
                    aio_pika.Message(url.encode()),
                    routing_key=settings.scraper_queue
                )
        except Exception as e:
            logger.error(f"Error inserting URL: {e}")

    async def _insert_and_publish_batch(self, urls: List[str], messages: List[aio_pika.IncomingMessage]):
        """Batch insert URLs; publish only newly inserted; ack all messages on success."""
        if not urls:
            return
        try:
            async with db_manager.get_url_pool().acquire() as conn:
                # Deduplicate in-memory first to reduce DB work
                unique_urls = list(dict.fromkeys(urls))
                # Insert all with a single statement; RETURNING gives only newly inserted
                rows = await conn.fetch(
                    """
                    INSERT INTO urls (url)
                    SELECT unnest($1::text[])
                    ON CONFLICT (url) DO NOTHING
                    RETURNING url
                    """,
                    unique_urls
                )
                inserted_urls = [r['url'] for r in rows]

            # Publish only newly inserted URLs
            if inserted_urls:
                channel = self._channel or db_manager.get_rabbitmq_channel()
                for u in inserted_urls:
                    try:
                        await channel.default_exchange.publish(
                            aio_pika.Message(u.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                            routing_key=settings.scraper_queue
                        )
                    except Exception as e:
                        logger.error(f"Error publishing URL to scraper_queue: {e}")

            # Ack all consumed messages after DB success (even duplicates/no-ops)
            for msg in messages:
                await msg.ack()
            self.processed_count += len(messages)
        except Exception as e:
            logger.error(f"Error in URL batch insert/publish: {e}")
            # Nack all on failure to avoid stuck messages
            for msg in messages:
                try:
                    await msg.nack(requeue=False)
                except Exception:
                    pass

    async def _periodic_flush(self):
        try:
            while self.is_running:
                await asyncio.sleep(self._flush_interval)
                async with self._lock:
                    if not self._buffer:
                        continue
                    urls = self._buffer[:]
                    msgs = self._buffer_msgs[:]
                    self._buffer.clear(); self._buffer_msgs.clear()
                try:
                    await self._insert_and_publish_batch(urls, msgs)
                except Exception as e:
                    logger.error(f"Periodic URL flush failed: {e}")
        except asyncio.CancelledError:
            return


