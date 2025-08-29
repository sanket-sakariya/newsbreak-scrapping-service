import aio_pika
import logging
import asyncio
import json
from typing import List, Tuple, Optional
from app.core.config import settings
from app.core.database import db_manager

logger = logging.getLogger(__name__)


class UrlWorker:
    """Worker to process URL queue and insert into database"""
    
    def __init__(self):
        self.is_running = False
        self.processed_count = 0
        self._channel = None
        self._connection = None
        self._buffer: List[str] = []
        self._buffer_msgs: List[aio_pika.IncomingMessage] = []
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._batch_size = getattr(settings, 'default_batch_size', 200)  # Increased from 100 to 200
        self._flush_interval = getattr(settings, 'batch_flush_seconds', 3)  # Reduced from 5 to 3 seconds
    
    async def start(self):
        """Start the URL worker"""
        self.is_running = True
        logger.info("URL worker started")
        # Create dedicated channel with higher prefetch for URL consumption
        try:
            self._connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=max(200, self._batch_size))
            logger.info("URL worker created dedicated RabbitMQ channel")
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
                    url_data, msgs = self._buffer[:], self._buffer_msgs[:]
                    self._buffer.clear(); self._buffer_msgs.clear()
                else:
                    url_data, msgs = [], []
            if url_data:
                await self._insert_and_publish_batch(url_data, msgs)
        except Exception as e:
            logger.error(f"Error flushing URL batch on stop: {e}")
        if self._channel and not self._channel.is_closed:
            try:
                await self._channel.close()
            except Exception:
                pass
        if self._connection and not self._connection.is_closed:
            try:
                await self._connection.close()
            except Exception:
                pass

    async def _ensure_channel(self) -> aio_pika.Channel:
        """Ensure we have a working RabbitMQ channel"""
        try:
            # If we already have a working channel, return it
            if self._channel and not self._channel.is_closed:
                return self._channel
            
            # If we have a connection but no channel, create a new channel
            if self._connection and not self._connection.is_closed:
                try:
                    self._channel = await self._connection.channel()
                    await self._channel.set_qos(prefetch_count=max(200, self._batch_size))
                    logger.info("URL worker created new channel on existing connection")
                    return self._channel
                except Exception as e:
                    logger.warning(f"Failed to create channel on existing connection: {e}")
                    # Fall through to create new connection
            
            # Close existing connection if it exists
            if self._connection and not self._connection.is_closed:
                try:
                    await self._connection.close()
                except Exception:
                    pass
            
            # Create new connection and channel
            self._connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=max(200, self._batch_size))
            logger.info("URL worker created new RabbitMQ connection and channel")
            return self._channel
        except Exception as e:
            logger.error(f"URL worker failed to create channel: {e}")
            raise
    
    async def consume_url_queue(self):
        """Consume URLs from newsbreak_urls_queue"""
        while self.is_running:
            try:
                channel = await self._ensure_channel()
                urls_queue = await channel.declare_queue(
                    settings.newsbreak_urls_queue, 
                    durable=True
                )
                
                async with urls_queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        if not self.is_running:
                            break
                        
                        try:
                            message_body = message.body.decode()
                            
                            # Try to parse as JSON (for 4xx error messages) or plain URL
                            try:
                                message_data = json.loads(message_body)
                                url = message_data.get('url', message_body)
                                status_code = message_data.get('status_code')
                            except json.JSONDecodeError:
                                # Plain URL message
                                url = message_body
                                status_code = None
                            
                            # Buffer for batch insert
                            async with self._lock:
                                self._buffer.append((url, status_code))  # Store URL with status code
                                self._buffer_msgs.append(message)
                                should_flush = len(self._buffer) >= self._batch_size
                            if should_flush:
                                # Snapshot outside lock
                                async with self._lock:
                                    url_data = self._buffer[:]
                                    msgs = self._buffer_msgs[:]
                                    self._buffer.clear(); self._buffer_msgs.clear()
                                await self._insert_and_publish_batch(url_data, msgs)
                            
                        except Exception as e:
                            logger.error(f"Error processing URL message: {e}")
                            await message.nack(requeue=False)
                            
            except aio_pika.exceptions.ChannelClosed as e:
                logger.warning(f"URL worker channel closed, reconnecting: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting
                continue
            except aio_pika.exceptions.ConnectionClosed as e:
                logger.warning(f"URL worker connection closed, reconnecting: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting
                continue
            except Exception as e:
                logger.error(f"URL worker error: {e}")
                await asyncio.sleep(10)  # Wait before retrying
    
    async def insert_url(self, url: str):
        """(Kept for compatibility) Insert single URL; prefer batch path."""
        try:
            async with db_manager.get_url_pool().acquire() as conn:
                inserted = await conn.fetchval(
                    "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING RETURNING url",
                    url
                ) is not None
            if inserted:
                channel = await self._ensure_channel()
                await channel.default_exchange.publish(
                    aio_pika.Message(url.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                    routing_key=settings.scraper_queue
                )
        except Exception as e:
            logger.error(f"Error inserting URL: {e}")

    async def _insert_and_publish_batch(self, url_data: List[Tuple[str, Optional[int]]], messages: List[aio_pika.IncomingMessage]):
        """Batch insert URLs; publish only newly inserted (non-4xx); ack all messages on success."""
        if not url_data:
            return
        try:
            # Extract URLs for deduplication and insertion
            urls = [item[0] for item in url_data]
            # Deduplicate in-memory first to reduce DB work
            unique_urls = list(dict.fromkeys(urls))
            
            # Batch insert with single database transaction
            async with db_manager.get_url_pool().acquire() as conn:
                async with conn.transaction():
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

            # Publish only newly inserted URLs that don't have 4xx status codes
            if inserted_urls:
                channel = await self._ensure_channel()
                
                # Create a mapping of URLs to their status codes
                url_status_map = {item[0]: item[1] for item in url_data}
                
                # Batch publish messages for better performance
                publish_tasks = []
                for url in inserted_urls:
                    status_code = url_status_map.get(url)
                    
                    # Skip URLs with 4xx status codes
                    if status_code and 400 <= status_code < 500:
                        logger.debug(f"Skipping 4xx URL from scraper_queue: {url} (status: {status_code})")
                        continue
                    
                    # Create publish task
                    publish_tasks.append(
                        channel.default_exchange.publish(
                            aio_pika.Message(url.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                            routing_key=settings.scraper_queue
                        )
                    )
                
                # Execute all publish tasks concurrently
                if publish_tasks:
                    await asyncio.gather(*publish_tasks, return_exceptions=True)
                    logger.debug(f"Published {len(publish_tasks)} URLs to scraper queue")

            # Ack all consumed messages after DB success (even duplicates/no-ops)
            for msg in messages:
                await msg.ack()
            self.processed_count += len(messages)
            
            # Log progress every 1000 processed URLs
            if self.processed_count % 1000 == 0:
                logger.info(f"URL worker processed {self.processed_count} URLs total")
                
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
                    url_data = self._buffer[:]
                    msgs = self._buffer_msgs[:]
                    self._buffer.clear(); self._buffer_msgs.clear()
                try:
                    await self._insert_and_publish_batch(url_data, msgs)
                except Exception as e:
                    logger.error(f"Periodic URL flush failed: {e}")
        except asyncio.CancelledError:
            return


