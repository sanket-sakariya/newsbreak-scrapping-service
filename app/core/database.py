import asyncpg
import aio_pika
from typing import Optional
import logging
from .config import settings

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.data_pool: Optional[asyncpg.Pool] = None
        self.url_pool: Optional[asyncpg.Pool] = None
        self.rabbitmq_connection: Optional[aio_pika.Connection] = None
        self.rabbitmq_channel: Optional[aio_pika.Channel] = None
    
    async def initialize_connections(self):
        """Initialize database and RabbitMQ connections"""
        try:
            # Initialize PostgreSQL connection pools
            self.data_pool = await asyncpg.create_pool(
                settings.database_url_1, 
                min_size=5, 
                max_size=20
            )
            logger.info("Database connection pool initialized for data")
            
            self.url_pool = await asyncpg.create_pool(
                settings.database_url_2, 
                min_size=5, 
                max_size=20
            )
            logger.info("Database connection pool initialized for URLs")
            
            # Initialize RabbitMQ connection
            self.rabbitmq_connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            await self.rabbitmq_channel.set_qos(prefetch_count=10)
            logger.info("RabbitMQ connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize connections: {e}")
            raise
    
    async def setup_queues(self):
        """Setup RabbitMQ queues with DLX configuration"""
        try:
            # Declare DLX queue first (without dead letter configuration to avoid loops)
            dlx_queue = await self.rabbitmq_channel.declare_queue(
                settings.dlx_queue, 
                durable=True
            )
            
            # Declare main queues with DLX configuration
            scraper_queue = await self.rabbitmq_channel.declare_queue(
                settings.scraper_queue, 
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": settings.dlx_queue
                }
            )
            
            # Other queues
            urls_queue = await self.rabbitmq_channel.declare_queue(
                settings.newsbreak_urls_queue, 
                durable=True
            )
            data_queue = await self.rabbitmq_channel.declare_queue(
                settings.newsbreak_data_queue, 
                durable=True
            )
            
            logger.info("All queues setup successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup queues: {e}")
            raise
    
    async def cleanup_connections(self):
        """Cleanup connections on shutdown"""
        if self.data_pool:
            await self.data_pool.close()
        if self.url_pool:
            await self.url_pool.close()
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
    
    def get_data_pool(self) -> Optional[asyncpg.Pool]:
        return self.data_pool
    
    def get_url_pool(self) -> Optional[asyncpg.Pool]:
        return self.url_pool
    
    def get_rabbitmq_channel(self) -> Optional[aio_pika.Channel]:
        return self.rabbitmq_channel

# Global database manager instance
db_manager = DatabaseManager()
