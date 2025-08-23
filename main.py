# main.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, HttpUrl
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import asyncio
import aiohttp
import asyncpg
import aio_pika
import json
import uuid
import re
from contextlib import asynccontextmanager
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin, urlparse
import os
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic Models
class StartScrapingRequest(BaseModel):
    initial_urls: List[HttpUrl]
    batch_size: int = 100
    worker_count: int = 5

class StopScrapingRequest(BaseModel):
    job_id: Optional[str] = None
    force: bool = False

class AddUrlsRequest(BaseModel):
    urls: List[HttpUrl]

class NewsbreakData(BaseModel):
    id: str
    likeCount: int
    commentCount: int
    source_id: int
    city_id: int
    title: str
    origin_url: str
    share_count: int
    first_text_category_id: int
    second_text_category_id: int
    third_text_category_id: int
    first_text_category_value: float
    second_text_category_value: float
    third_text_category_value: float
    nf_entities_id: int
    nf_entities_value: float
    nf_tags_id: int
    workspace_id: str
    created_by: str
    is_active: bool
    status: str
    created_at: datetime
    updated_at: datetime

class NewsbreakUrl(BaseModel):
    url: str

# Global variables for managing workers and connections
scraper_workers: Dict[str, asyncio.Task] = {}
data_pool: Optional[asyncpg.Pool] = None
url_pool: Optional[asyncpg.Pool] = None
rabbitmq_connection: Optional[aio_pika.Connection] = None
rabbitmq_channel: Optional[aio_pika.Channel] = None

# Database and Queue Configuration
DATABASE_URL_1 = os.getenv("DATABASE_URL_1", "postgresql://sanket:root@localhost:5432/newsbreak_scraper_data")
DATABASE_URL_2 = os.getenv("DATABASE_URL_2", "postgresql://sanket:root@localhost:5432/newsbreak_scraper_urls")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

# Queue Names
SCRAPER_QUEUE = "scraper_queue"
NEWSBREAK_URLS_QUEUE = "newsbreak_urls_queue"
NEWSBREAK_DATA_QUEUE = "newsbreak_data_queue"
DLX_QUEUE = "dlx_queue"

async def initialize_connections():
    """Initialize database and RabbitMQ connections"""
    global data_pool, url_pool, rabbitmq_connection, rabbitmq_channel
    
    try:
        # Initialize PostgreSQL connection pools
        data_pool = await asyncpg.create_pool(DATABASE_URL_1, min_size=5, max_size=20)
        logger.info("Database connection pool initialized for data")
        
        url_pool = await asyncpg.create_pool(DATABASE_URL_2, min_size=5, max_size=20)
        logger.info("Database connection pool initialized for URLs")
        
        # Initialize RabbitMQ connection
        rabbitmq_connection = await aio_pika.connect_robust(RABBITMQ_URL)
        rabbitmq_channel = await rabbitmq_connection.channel()
        await rabbitmq_channel.set_qos(prefetch_count=10)
        logger.info("RabbitMQ connection initialized")
        
    except Exception as e:
        logger.error(f"Failed to initialize connections: {e}")
        raise

async def setup_queues():
    """Setup RabbitMQ queues with DLX configuration"""
    try:
        # Declare DLX queue first (without dead letter configuration to avoid loops)
        dlx_queue = await rabbitmq_channel.declare_queue(
            DLX_QUEUE, 
            durable=True
        )
        
        # Declare main queues with DLX configuration
        scraper_queue = await rabbitmq_channel.declare_queue(
            SCRAPER_QUEUE, 
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": DLX_QUEUE
            }
        )
        
        # Other queues
        urls_queue = await rabbitmq_channel.declare_queue(NEWSBREAK_URLS_QUEUE, durable=True)
        data_queue = await rabbitmq_channel.declare_queue(NEWSBREAK_DATA_QUEUE, durable=True)
        
        logger.info("All queues setup successfully")
        
    except Exception as e:
        logger.error(f"Failed to setup queues: {e}")
        raise

async def setup_database():
    """Setup database tables"""
    try:
        async with data_pool.acquire() as conn:
            # Create all tables
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS source_data (
                    source_id SERIAL PRIMARY KEY,
                    source_name VARCHAR(255) UNIQUE NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS city_data (
                    city_id SERIAL PRIMARY KEY,
                    city_name VARCHAR(255) UNIQUE NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS text_category_data (
                    text_category_id SERIAL PRIMARY KEY,
                    category_name VARCHAR(255) UNIQUE NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS nf_entities_data (
                    nf_entities_id SERIAL PRIMARY KEY,
                    entity_name VARCHAR(255) UNIQUE NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS nf_tags_data (
                    nf_tags_id SERIAL PRIMARY KEY,
                    tag_name VARCHAR(255) UNIQUE NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS newsbreak_data (
                    id VARCHAR(255) PRIMARY KEY,
                    likeCount INTEGER,
                    commentCount INTEGER,
                    source_id INTEGER REFERENCES source_data(source_id),
                    city_id INTEGER REFERENCES city_data(city_id),
                    title TEXT,
                    origin_url TEXT,
                    share_count INTEGER,
                    first_text_category_id INTEGER REFERENCES text_category_data(text_category_id),
                    second_text_category_id INTEGER REFERENCES text_category_data(text_category_id),
                    third_text_category_id INTEGER REFERENCES text_category_data(text_category_id),
                    first_text_category_value FLOAT,
                    second_text_category_value FLOAT,
                    third_text_category_value FLOAT,
                    nf_entities_id INTEGER REFERENCES nf_entities_data(nf_entities_id),
                    nf_entities_value FLOAT,
                    nf_tags_id INTEGER REFERENCES nf_tags_data(nf_tags_id),
                    workspace_id UUID,
                    created_by UUID,
                    is_active BOOLEAN DEFAULT TRUE,
                    status VARCHAR(50) DEFAULT 'creating',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_newsbreak_data_created_at ON newsbreak_data(created_at)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_newsbreak_data_status ON newsbreak_data(status)")

        async with url_pool.acquire() as conn:
            # Create URLs table with the missing last_scraped column
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    url TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_scraped TIMESTAMP NULL,
                    scrape_count INTEGER DEFAULT 0
                )
            """)
            
            # Add the missing last_scraped column if table exists but column doesn't
            try:
                await conn.execute("""
                    ALTER TABLE urls 
                    ADD COLUMN IF NOT EXISTS last_scraped TIMESTAMP NULL
                """)
            except Exception as e:
                logger.debug(f"Column last_scraped might already exist: {e}")
            
            # Add the missing scrape_count column if table exists but column doesn't
            try:
                await conn.execute("""
                    ALTER TABLE urls 
                    ADD COLUMN IF NOT EXISTS scrape_count INTEGER DEFAULT 0
                """)
            except Exception as e:
                logger.debug(f"Column scrape_count might already exist: {e}")
            
            # Add index for performance
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_last_scraped ON urls(last_scraped)")
            
        logger.info("Database tables setup successfully")
        
    except Exception as e:
        logger.error(f"Failed to setup database: {e}")
        raise

async def cleanup_connections():
    """Cleanup connections on shutdown"""
    global data_pool, url_pool, rabbitmq_connection
    
    # Stop all workers
    for job_id, task in scraper_workers.items():
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    
    # Close connections
    if data_pool:
        await data_pool.close()
    if url_pool:
        await url_pool.close()
    if rabbitmq_connection:
        await rabbitmq_connection.close()

class ScraperWorker:
    """Main scraper worker class"""
    
    def __init__(self, worker_id: str, batch_size: int = 100):
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_running = False
        self.processed_count = 0
    
    async def start(self):
        """Start the worker"""
        self.is_running = True
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        
        logger.info(f"Worker {self.worker_id} started")
        
        # Start consuming from scraper queue
        await self.consume_scraper_queue()
    
    async def stop(self):
        """Stop the worker"""
        self.is_running = False
        if self.session:
            await self.session.close()
        logger.info(f"Worker {self.worker_id} stopped. Processed {self.processed_count} URLs")
    
    async def consume_scraper_queue(self):
        """Consume URLs from scraper queue and process them"""
        try:
            scraper_queue = await rabbitmq_channel.declare_queue(
                SCRAPER_QUEUE, 
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": DLX_QUEUE
                }
            )
            
            async with scraper_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self.is_running:
                        break
                    
                    try:
                        url = message.body.decode()
                        logger.info(f"Worker {self.worker_id} processing URL: {url}")
                        
                        # Process single URL
                        result = await self.scrape_url(url)
                        if result:
                            extracted_urls, extracted_data = result
                            await self.handle_extracted_data(extracted_urls, extracted_data)
                            
                            # Update URL processing timestamp
                            await self.update_url_processed(url)
                            
                        self.processed_count += 1
                        await message.ack()
                        
                        # Small delay to avoid overwhelming the server
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Worker {self.worker_id} error processing message: {e}")
                        await message.nack(requeue=False)
                        await self.send_to_dlx(url, str(e))
                    
        except Exception as e:
            logger.error(f"Worker {self.worker_id} consume error: {e}")
    
    async def scrape_url(self, url: str):
        """Scrape a single URL and extract data and URLs"""
        try:
            logger.debug(f"Scraping URL: {url}")
            
            async with self.session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract URLs
                extracted_urls = self.extract_urls(soup, url)
                
                # Extract data from __NEXT_DATA__
                extracted_data = self.extract_next_data(soup)
                
                logger.debug(f"Extracted {len(extracted_urls)} URLs and {'data' if extracted_data else 'no data'} from {url}")
                
                return extracted_urls, extracted_data
                
        except Exception as e:
            logger.error(f"Failed to scrape {url}: {e}")
            raise
    
    def extract_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract NewsBreak URLs from BeautifulSoup object"""
        try:
            urls = set()
            
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                
                if href:
                    # Handle relative URLs
                    if href.startswith('/'):
                        full_url = urljoin(base_url, href)
                    else:
                        full_url = href
                    
                    # Only collect NewsBreak URLs
                    if 'newsbreak.com' in full_url:
                        parsed = urlparse(full_url)
                        # Clean URL (remove fragments and unnecessary params)
                        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                        if clean_url != base_url:  # Don't add the same URL
                            urls.add(clean_url)
            
            return list(urls)
        except Exception as e:
            logger.error(f"Error extracting URLs: {e}")
            return []
    
    def extract_next_data(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract data from __NEXT_DATA__ script tag"""
        try:
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            if not script_tag:
                return None
            
            json_data = json.loads(script_tag.string)
            page_props = json_data.get('props', {}).get('pageProps', {})
            
            if not page_props.get('id'):
                return None
            
            # Extract the required data structure
            text_category_obj = page_props.get('text_category', {}) or {}
            if isinstance(text_category_obj, dict):
                try:
                    logger.debug(
                        f"__NEXT_DATA__ text_category keys: {list(text_category_obj.keys())}"
                    )
                except Exception:
                    pass
            else:
                logger.debug("__NEXT_DATA__ text_category is not a dict")

            extracted_data = {
                'id': page_props.get('id'),
                'likeCount': page_props.get('likeCount', 0),
                'commentCount': page_props.get('commentCount', 0),
                'source': page_props.get('source'),
                'cityName': page_props.get('cityName'),
                'title': page_props.get('title'),
                'origin_url': page_props.get('origin_url'),
                'share_count': page_props.get('share_count', 0),
                'text_category': text_category_obj,
                'nf_entities': page_props.get('nf_entities', {}),
                'nf_tags': page_props.get('nf_tags', [])
            }
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extracting __NEXT_DATA__: {e}")
            return None
    
    async def handle_extracted_data(self, urls: List[str], data: Optional[Dict]):
        """Send extracted URLs and data to their respective queues"""
        try:
            # Send URLs to newsbreak_urls_queue
            if urls:
                for url in urls:
                    await rabbitmq_channel.default_exchange.publish(
                        aio_pika.Message(url.encode()),
                        routing_key=NEWSBREAK_URLS_QUEUE
                    )
                logger.debug(f"Sent {len(urls)} URLs to URL queue")
            
            # Send data to newsbreak_data_queue
            if data:
                await rabbitmq_channel.default_exchange.publish(
                    aio_pika.Message(json.dumps(data).encode()),
                    routing_key=NEWSBREAK_DATA_QUEUE
                )
                logger.debug(f"Sent data for ID {data.get('id')} to data queue")
                
        except Exception as e:
            logger.error(f"Error handling extracted data: {e}")
    
    async def update_url_processed(self, url: str):
        """Update URL processing timestamp"""
        try:
            async with url_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE urls 
                    SET last_scraped = CURRENT_TIMESTAMP, scrape_count = scrape_count + 1 
                    WHERE url = $1
                """, url)
        except Exception as e:
            logger.error(f"Error updating URL processed timestamp: {e}")
    
    async def send_to_dlx(self, url: str, error: str):
        """Send failed URL to DLX queue"""
        try:
            message_data = {
                'url': url, 
                'error': error, 
                'timestamp': datetime.utcnow().isoformat(),
                'worker_id': self.worker_id
            }
            await rabbitmq_channel.default_exchange.publish(
                aio_pika.Message(json.dumps(message_data).encode()),
                routing_key=DLX_QUEUE
            )
        except Exception as e:
            logger.error(f"Error sending to DLX: {e}")

class DataWorker:
    """Worker to process data queue and insert into database"""
    
    def __init__(self):
        self.is_running = False
        self.processed_count = 0
    
    async def start(self):
        """Start the data worker"""
        self.is_running = True
        logger.info("Data worker started")
        await self.consume_data_queue()
    
    async def stop(self):
        """Stop the data worker"""
        self.is_running = False
        logger.info(f"Data worker stopped. Processed {self.processed_count} records")
    
    async def consume_data_queue(self):
        """Consume data from newsbreak_data_queue"""
        try:
            data_queue = await rabbitmq_channel.declare_queue(NEWSBREAK_DATA_QUEUE, durable=True)
            
            async with data_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self.is_running:
                        break
                    
                    try:
                        data = json.loads(message.body.decode())
                        await self.insert_data(data)
                        await message.ack()
                        self.processed_count += 1
                        logger.debug(f"Data worker processed record ID: {data.get('id')}")
                        
                    except Exception as e:
                        logger.error(f"Error processing data message: {e}")
                        await message.nack(requeue=False)
                        
        except Exception as e:
            logger.error(f"Data worker error: {e}")
    
    async def insert_data(self, data: Dict):
        """Insert extracted data into PostgreSQL database"""
        try:
            async with data_pool.acquire() as conn:
                async with conn.transaction():
                    # Insert or get foreign key references
                    source_id = await self.get_or_create_source(conn, data.get('source'))
                    city_id = await self.get_or_create_city(conn, data.get('cityName'))
                    
                    # Handle text categories
                    text_category = data.get('text_category', {}) or {}
                    # Log raw structure to verify what we received from JSON
                    try:
                        logger.debug(
                            f"Inserting data ID={data.get('id')} text_category keys={list(text_category.keys())}"
                        )
                    except Exception:
                        pass

                    first_cat_raw = text_category.get('first_cat', {})
                    second_cat_raw = text_category.get('second_cat', {})
                    third_cat_raw = text_category.get('third_cat', {})

                    logger.debug(
                        f"Parsed categories: first={first_cat_raw}, second={second_cat_raw}, third={third_cat_raw}"
                    )

                    first_cat_id, first_cat_value = await self.process_category(conn, first_cat_raw)
                    second_cat_id, second_cat_value = await self.process_category(conn, second_cat_raw)
                    third_cat_id, third_cat_value = await self.process_category(conn, third_cat_raw)

                    
                    # Handle entities and tags
                    entities_id, entities_value = await self.process_entities(conn, data.get('nf_entities', {}))
                    tags_id = await self.process_tags(conn, data.get('nf_tags', []))
                    
                    # Insert main data
                    await conn.execute("""
                        INSERT INTO newsbreak_data (
                            id, likeCount, commentCount, source_id, city_id, title, origin_url, share_count,
                            first_text_category_id, second_text_category_id, third_text_category_id,
                            first_text_category_value, second_text_category_value, third_text_category_value,
                            nf_entities_id, nf_entities_value, nf_tags_id, workspace_id, created_by, status
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                        ON CONFLICT (id) DO UPDATE SET
                            likeCount = EXCLUDED.likeCount,
                            commentCount = EXCLUDED.commentCount,
                            share_count = EXCLUDED.share_count,
                            updated_at = CURRENT_TIMESTAMP
                    """, 
                        data.get('id'), data.get('likeCount', 0), data.get('commentCount', 0),
                        source_id, city_id, data.get('title'), data.get('origin_url'), data.get('share_count', 0),
                        first_cat_id, second_cat_id, third_cat_id,
                        first_cat_value, second_cat_value, third_cat_value,
                        entities_id, entities_value, tags_id,
                        str(uuid.uuid4()), str(uuid.uuid4()), 'created'
                    )
                    
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            raise
    
    async def get_or_create_source(self, conn, source_name: str) -> Optional[int]:
        if not source_name:
            return None
        
        result = await conn.fetchval("SELECT source_id FROM source_data WHERE source_name = $1", source_name)
        if result:
            return result
        
        return await conn.fetchval("INSERT INTO source_data (source_name) VALUES ($1) RETURNING source_id", source_name)
    
    async def get_or_create_city(self, conn, city_name: str) -> Optional[int]:
        if not city_name:
            return None
        
        result = await conn.fetchval("SELECT city_id FROM city_data WHERE city_name = $1", city_name)
        if result:
            return result
        
        return await conn.fetchval("INSERT INTO city_data (city_name) VALUES ($1) RETURNING city_id", city_name)
    
    async def process_category(self, conn, category: Dict) -> Tuple[Optional[int], Optional[float]]:
        """Ensure category exists and return its ID + float value"""
        if not category:
            return None, None

        # Accept either {"name": "Sports", "value": 0.87}
        # or {"Sports": 0.87}
        name: Optional[str] = None
        value: Optional[float] = None

        if isinstance(category, dict):
            if "name" in category or "value" in category:
                name = category.get("name")
                value_raw = category.get("value")
                try:
                    value = float(value_raw) if value_raw is not None else None
                except (TypeError, ValueError):
                    value = None
            else:
                # Take the first key:value pair where value can be cast to float
                for k, v in category.items():
                    try:
                        value = float(v) if v is not None else None
                        name = k
                        break
                    except (TypeError, ValueError):
                        continue

        if not name:
            return None, value

        # Insert or fetch category id in text_category_data(category_name)
        row = await conn.fetchrow(
            """
            INSERT INTO text_category_data (category_name)
            VALUES ($1)
            ON CONFLICT (category_name) DO UPDATE SET category_name = EXCLUDED.category_name
            RETURNING text_category_id
            """,
            name,
        )
        return row["text_category_id"], value
    
    async def process_entities(self, conn, entities_data: Dict) -> tuple:
        if not entities_data:
            return None, None
        
        entity_name = list(entities_data.keys())[0] if entities_data else None
        entity_value = list(entities_data.values())[0] if entities_data else None
        
        if not entity_name:
            return None, None
        
        result = await conn.fetchval("SELECT nf_entities_id FROM nf_entities_data WHERE entity_name = $1", entity_name)
        if not result:
            result = await conn.fetchval("INSERT INTO nf_entities_data (entity_name) VALUES ($1) RETURNING nf_entities_id", entity_name)
        
        return result, float(entity_value) if entity_value else None
    
    async def process_tags(self, conn, tags_data: List) -> Optional[int]:
        if not tags_data:
            return None
        
        tag_name = tags_data[0] if tags_data else None
        if not tag_name:
            return None
        
        result = await conn.fetchval("SELECT nf_tags_id FROM nf_tags_data WHERE tag_name = $1", tag_name)
        if not result:
            result = await conn.fetchval("INSERT INTO nf_tags_data (tag_name) VALUES ($1) RETURNING nf_tags_id", tag_name)
        
        return result

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
            urls_queue = await rabbitmq_channel.declare_queue(NEWSBREAK_URLS_QUEUE, durable=True)
            
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
        """Insert URL into database (with deduplication)"""
        try:
            async with url_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING", 
                    url
                )
        except Exception as e:
            logger.error(f"Error inserting URL: {e}")

class UrlFeeder:
    """Background service to feed URLs from database to scraper queue"""
    
    def __init__(self):
        self.is_running = False
        self.batch_size = 50
        self.feed_interval = 30  # seconds
        self.homepage_url = "https://www.newsbreak.com"
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_bootstrap = None
    
    async def start(self):
        """Start the URL feeder"""
        self.is_running = True
        
        # Create HTTP session for homepage fetching
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        
        logger.info("URL feeder started")
        
        while self.is_running:
            try:
                await self.feed_urls()
                await asyncio.sleep(self.feed_interval)
            except Exception as e:
                logger.error(f"URL feeder error: {e}")
                await asyncio.sleep(10)  # Wait before retrying
    
    async def stop(self):
        """Stop the URL feeder"""
        self.is_running = False
        if self.session:
            await self.session.close()
        logger.info("URL feeder stopped")
    
    async def feed_urls(self):
        """Feed URLs from database to scraper queue, fetch homepage if no URLs available"""
        try:
            # Check if scraper queue needs more URLs
            scraper_queue = await rabbitmq_channel.declare_queue(
                SCRAPER_QUEUE, 
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": DLX_QUEUE
                }
            )
            
            current_queue_size = scraper_queue.declaration_result.message_count
            
            # Only feed if queue is getting low
            if current_queue_size < 50:
                async with url_pool.acquire() as conn:
                    # Get URLs that haven't been scraped recently
                    urls = await conn.fetch("""
                        SELECT url FROM urls 
                        WHERE (last_scraped IS NULL OR last_scraped < NOW() - INTERVAL '1 hour')
                        ORDER BY 
                            CASE WHEN last_scraped IS NULL THEN 0 ELSE 1 END,
                            last_scraped ASC,
                            scrape_count ASC
                        LIMIT $1
                    """, self.batch_size)
                
                # If no URLs available, bootstrap from homepage
                if not urls:
                    logger.info("No unprocessed URLs found, bootstrapping from homepage")
                    await self.bootstrap_from_homepage()
                    return
                
                # Add URLs to scraper queue
                urls_added = 0
                for url_row in urls:
                    try:
                        await rabbitmq_channel.default_exchange.publish(
                            aio_pika.Message(url_row["url"].encode()),
                            routing_key=SCRAPER_QUEUE
                        )
                        urls_added += 1
                    except Exception as e:
                        logger.error(f"Error publishing URL to queue: {e}")
                
                if urls_added > 0:
                    logger.info(f"Fed {urls_added} URLs to scraper queue")
                    
        except Exception as e:
            logger.error(f"Error feeding URLs: {e}")
    
    async def bootstrap_from_homepage(self):
        """Fetch NewsBreak homepage and extract initial URLs to bootstrap the scraping process"""
        try:
            # Don't bootstrap too frequently
            now = datetime.utcnow()
            if self.last_bootstrap and (now - self.last_bootstrap).seconds < 300:  # 5 minutes
                return
            
            self.last_bootstrap = now
            logger.info(f"Bootstrapping from homepage: {self.homepage_url}")
            
            async with self.session.get(self.homepage_url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch homepage, status: {response.status}")
                    return
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract NewsBreak URLs from homepage
                extracted_urls = self.extract_newsbreak_urls(soup, self.homepage_url)
                
                if not extracted_urls:
                    logger.warning("No URLs extracted from homepage")
                    return
                
                # Insert URLs into database
                unique_urls = list(set(extracted_urls))
                urls_inserted = 0
                
                async with url_pool.acquire() as conn:
                    for url in unique_urls:
                        try:
                            await conn.execute(
                                "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING", 
                                url
                            )
                            urls_inserted += 1
                        except Exception as e:
                            logger.error(f"Error inserting URL {url}: {e}")
                
                # Add some URLs directly to scraper queue for immediate processing
                urls_to_queue = unique_urls[:20]
                queued_count = 0
                
                for url in urls_to_queue:
                    try:
                        await rabbitmq_channel.default_exchange.publish(
                            aio_pika.Message(url.encode()),
                            routing_key=SCRAPER_QUEUE
                        )
                        queued_count += 1
                    except Exception as e:
                        logger.error(f"Error queuing URL {url}: {e}")
                
                logger.info(f"Bootstrap completed: {urls_inserted} URLs inserted, {queued_count} URLs queued")
                
        except Exception as e:
            logger.error(f"Error bootstrapping from homepage: {e}")
    
    def extract_newsbreak_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract NewsBreak URLs from the homepage"""
        urls = []
        
        try:
            # Look for various link patterns on NewsBreak homepage
            link_selectors = [
                'a[href*="/articles/"]',
                'a[href*="/news/"]',
                'a[href*="/local/"]',
                'a[href*="/trending/"]',
                'a[href*="/politics/"]',
                'a[href*="/sports/"]',
                'a[href*="/business/"]',
                'a[href*="/entertainment/"]',
                'a[href*="/technology/"]',
                'a[href*="/health/"]',
                'a[href*="/science/"]',
                'a[href*="/lifestyle/"]',
                'article a[href]',
                '.article-title a',
                '.news-item a'
            ]
            
            for selector in link_selectors:
                try:
                    links = soup.select(selector)
                    for link in links:
                        href = link.get('href')
                        if href:
                            full_url = urljoin(base_url, href)
                            urls.append(full_url)
                except Exception as e:
                    logger.debug(f"Error with selector {selector}: {e}")
            
            # Also look for any links with newsbreak.com domain
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                urls.append(full_url)
            
            # Remove duplicates and filter
            unique_urls = list(set(urls))
            
            # Filter out unwanted URLs
            filtered_urls = self.filter_unwanted_urls(unique_urls)
            
            logger.info(f"Extracted {len(filtered_urls)} valid URLs from homepage")
            return filtered_urls
            
        except Exception as e:
            logger.error(f"Error extracting URLs from homepage: {e}")
            return []
    
    
    def filter_unwanted_urls(self, urls: List[str]) -> List[str]:
        """Filter out unwanted URLs"""
        filtered_urls = []
        
        unwanted_patterns = [
            '/ads/', '/redirect/', 'javascript:', 'mailto:', 'tel:',
            '#', '?utm_', '/login', '/register', '/subscribe',
            '/privacy', '/terms', '/api/', '/static/', '/css/',
            '/js/', '/images/', '/favicon'
        ]
        
        for url in urls:
            if not any(pattern in url.lower() for pattern in unwanted_patterns):
                filtered_urls.append(url)
        
        return filtered_urls

# Global workers
data_worker = DataWorker()
url_worker = UrlWorker()
url_feeder = UrlFeeder()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await initialize_connections()
    await setup_queues()
    await setup_database()
    
    # Start background workers
    asyncio.create_task(data_worker.start())
    asyncio.create_task(url_worker.start())
    asyncio.create_task(url_feeder.start())
    
    yield
    
    # Shutdown
    await data_worker.stop()
    await url_worker.stop()
    await url_feeder.stop()
    await cleanup_connections()

app = FastAPI(
    title="NewsBreak Scraper API",
    description="API for scraping NewsBreak.com with cyclic workflow using RabbitMQ queues and PostgreSQL storage",
    version="1.0.0",
    lifespan=lifespan
)

# API Endpoints
@app.post("/scraper/start")
async def start_scraping(request: StartScrapingRequest, background_tasks: BackgroundTasks):
    """Start the scraping process"""
    try:
        job_id = str(uuid.uuid4())
        
        # Add initial URLs to scraper queue
        for url in request.initial_urls:
            await rabbitmq_channel.default_exchange.publish(
                aio_pika.Message(str(url).encode()),
                routing_key=SCRAPER_QUEUE
            )
        
        # Start workers
        workers_started = 0
        for i in range(request.worker_count):
            worker_id = f"{job_id}_worker_{i}"
            worker = ScraperWorker(worker_id, request.batch_size)
            task = asyncio.create_task(worker.start())
            scraper_workers[worker_id] = task
            workers_started += 1
        
        logger.info(f"Started {workers_started} scraper workers for job {job_id}")
        
        return {
            "success": True,
            "data": {
                "job_id": job_id,
                "status": "started",
                "worker_count": workers_started,
                "initial_url_count": len(request.initial_urls)
            }
        }
        
    except Exception as e:
        logger.error(f"Error starting scraper: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scraper/stop")
async def stop_scraping(request: StopScrapingRequest = None):
    """Stop the scraping process"""
    try:
        stopped_workers = 0
        
        if request and request.job_id:
            # Stop specific job workers
            workers_to_stop = [k for k in scraper_workers.keys() if k.startswith(request.job_id)]
        else:
            # Stop all workers
            workers_to_stop = list(scraper_workers.keys())
        
        for worker_id in workers_to_stop:
            task = scraper_workers.pop(worker_id, None)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            stopped_workers += 1
        
        return {
            "success": True,
            "data": {
                "message": "Scraping process stopped successfully",
                "stopped_workers": stopped_workers
            }
        }
        
    except Exception as e:
        logger.error(f"Error stopping scraper: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scraper/status")
async def get_scraping_status():
    """Get scraping process status and statistics"""
    try:
        # Declare queues and get their info
        scraper_queue = await rabbitmq_channel.declare_queue(
            SCRAPER_QUEUE, 
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": DLX_QUEUE
            }
        )
        urls_queue = await rabbitmq_channel.declare_queue(NEWSBREAK_URLS_QUEUE, durable=True)
        data_queue = await rabbitmq_channel.declare_queue(NEWSBREAK_DATA_QUEUE, durable=True)
        dlx_queue = await rabbitmq_channel.declare_queue(DLX_QUEUE, durable=True)
        
        # Get database statistics
        async with data_pool.acquire() as conn:
            data_extracted = await conn.fetchval("SELECT COUNT(*) FROM newsbreak_data")
        
        async with url_pool.acquire() as conn:
            urls_processed = await conn.fetchval("SELECT COUNT(*) FROM urls")
            urls_scraped = await conn.fetchval("SELECT COUNT(*) FROM urls WHERE last_scraped IS NOT NULL")
        
        errors_count = dlx_queue.declaration_result.message_count
        status = "running" if scraper_workers else "stopped"
        
        return {
            "success": True,
            "data": {
                "status": status,
                "statistics": {
                    "urls_collected": urls_processed or 0,
                    "urls_scraped": urls_scraped or 0,
                    "data_extracted": data_extracted or 0,
                    "errors_count": errors_count,
                    "active_workers": len(scraper_workers),
                    "queue_sizes": {
                        "scraper_queue": scraper_queue.declaration_result.message_count,
                        "newsbreak_urls_queue": urls_queue.declaration_result.message_count,
                        "newsbreak_data_queue": data_queue.declaration_result.message_count,
                        "dlx_queue": dlx_queue.declaration_result.message_count
                    }
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/queues")
async def get_queue_statistics():
    """Get all queue statistics and information"""
    try:
        queues_info = []
        queue_names = [SCRAPER_QUEUE, NEWSBREAK_URLS_QUEUE, NEWSBREAK_DATA_QUEUE, DLX_QUEUE]
        
        for queue_name in queue_names:
            try:
                # Declare queue and get info from the returned object
                if queue_name == SCRAPER_QUEUE:
                    queue = await rabbitmq_channel.declare_queue(
                        queue_name, 
                        durable=True,
                        arguments={
                            "x-dead-letter-exchange": "",
                            "x-dead-letter-routing-key": DLX_QUEUE
                        }
                    )
                else:
                    queue = await rabbitmq_channel.declare_queue(queue_name, durable=True)
                
                # Use the declaration result to get queue info
                queue_info = queue.declaration_result
                
                queues_info.append({
                    "name": queue_name,
                    "message_count": queue_info.message_count,
                    "consumer_count": queue_info.consumer_count,
                    "status": "active" if queue_info.consumer_count > 0 else "idle",
                    "last_activity": datetime.now(timezone.utc).isoformat()
                })
                
            except Exception as e:
                logger.warning(f"Could not get info for queue {queue_name}: {e}")
                queues_info.append({
                    "name": queue_name,
                    "message_count": 0,
                    "consumer_count": 0,
                    "status": "unknown",
                    "last_activity": datetime.now(timezone.utc).isoformat(),
                    "error": str(e)
                })
        
        return {
            "success": True,
            "data": queues_info
        }
        
    except Exception as e:
        logger.error(f"Error getting queue statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/queues/{queue_name}/purge")
async def purge_queue(queue_name: str):
    """Purge all messages from a specific queue"""
    try:
        valid_queues = [SCRAPER_QUEUE, NEWSBREAK_URLS_QUEUE, NEWSBREAK_DATA_QUEUE, DLX_QUEUE]
        
        if queue_name not in valid_queues:
            raise HTTPException(status_code=400, detail="Invalid queue name")
        
        # Declare queue to get info
        if queue_name == SCRAPER_QUEUE:
            queue = await rabbitmq_channel.declare_queue(
                queue_name, 
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": DLX_QUEUE
                }
            )
        else:
            queue = await rabbitmq_channel.declare_queue(queue_name, durable=True)
        
        messages_purged = queue.declaration_result.message_count
        await queue.purge()
        
        return {
            "success": True,
            "data": {
                "queue_name": queue_name,
                "messages_purged": messages_purged
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error purging queue: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/newsbreak/data")
async def get_newsbreak_data(page: int = 1, limit: int = 100):
    """Retrieve scraped NewsBreak data with pagination"""
    try:
        offset = (page - 1) * limit
        
        async with data_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT nd.*, sd.source_name, cd.city_name,
                       tc1.category_name as first_category_name,
                       tc2.category_name as second_category_name,
                       tc3.category_name as third_category_name,
                       ne.entity_name, nt.tag_name
                FROM newsbreak_data nd
                LEFT JOIN source_data sd ON nd.source_id = sd.source_id
                LEFT JOIN city_data cd ON nd.city_id = cd.city_id
                LEFT JOIN text_category_data tc1 ON nd.first_text_category_id = tc1.text_category_id
                LEFT JOIN text_category_data tc2 ON nd.second_text_category_id = tc2.text_category_id
                LEFT JOIN text_category_data tc3 ON nd.third_text_category_id = tc3.text_category_id
                LEFT JOIN nf_entities_data ne ON nd.nf_entities_id = ne.nf_entities_id
                LEFT JOIN nf_tags_data nt ON nd.nf_tags_id = nt.nf_tags_id
                ORDER BY nd.created_at DESC
                LIMIT $1 OFFSET $2
            """, limit, offset)
        
        data = []
        for row in rows:
            data.append({
                "id": row["id"],
                "likeCount": row["likecount"],
                "commentCount": row["commentcount"],
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "city_id": row["city_id"],
                "city_name": row["city_name"],
                "title": row["title"],
                "origin_url": row["origin_url"],
                "share_count": row["share_count"],
                "first_text_category_id": row["first_text_category_id"],
                "first_category_name": row["first_category_name"],
                "second_text_category_id": row["second_text_category_id"],
                "second_category_name": row["second_category_name"],
                "third_text_category_id": row["third_text_category_id"],
                "third_category_name": row["third_category_name"],
                "first_text_category_value": row["first_text_category_value"],
                "second_text_category_value": row["second_text_category_value"],
                "third_text_category_value": row["third_text_category_value"],
                "nf_entities_id": row["nf_entities_id"],
                "entity_name": row["entity_name"],
                "nf_entities_value": row["nf_entities_value"],
                "nf_tags_id": row["nf_tags_id"],
                "tag_name": row["tag_name"],
                "workspace_id": row["workspace_id"],
                "created_by": row["created_by"],
                "is_active": row["is_active"],
                "status": row["status"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            })
        
        return {
            "success": True,
            "data": data
        }
        
    except Exception as e:
        logger.error(f"Error getting NewsBreak data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/newsbreak/data/{id}")
async def get_newsbreak_data_by_id(id: str):
    """Retrieve specific NewsBreak data record"""
    try:
        async with data_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT nd.*, sd.source_name, cd.city_name,
                       tc1.category_name as first_category_name,
                       tc2.category_name as second_category_name,
                       tc3.category_name as third_category_name,
                       ne.entity_name, nt.tag_name
                FROM newsbreak_data nd
                LEFT JOIN source_data sd ON nd.source_id = sd.source_id
                LEFT JOIN city_data cd ON nd.city_id = cd.city_id
                LEFT JOIN text_category_data tc1 ON nd.first_text_category_id = tc1.text_category_id
                LEFT JOIN text_category_data tc2 ON nd.second_text_category_id = tc2.text_category_id
                LEFT JOIN text_category_data tc3 ON nd.third_text_category_id = tc3.text_category_id
                LEFT JOIN nf_entities_data ne ON nd.nf_entities_id = ne.nf_entities_id
                LEFT JOIN nf_tags_data nt ON nd.nf_tags_id = nt.nf_tags_id
                WHERE nd.id = $1
            """, id)
        
        if not row:
            raise HTTPException(status_code=404, detail="NewsBreak data not found")
        
        data = {
            "id": row["id"],
            "likeCount": row["likecount"],
            "commentCount": row["commentcount"],
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "city_id": row["city_id"],
            "city_name": row["city_name"],
            "title": row["title"],
            "origin_url": row["origin_url"],
            "share_count": row["share_count"],
            "first_text_category_id": row["first_text_category_id"],
            "first_category_name": row["first_category_name"],
            "second_text_category_id": row["second_text_category_id"],
            "second_category_name": row["second_category_name"],
            "third_text_category_id": row["third_text_category_id"],
            "third_category_name": row["third_category_name"],
            "first_text_category_value": row["first_text_category_value"],
            "second_text_category_value": row["second_text_category_value"],
            "third_text_category_value": row["third_text_category_value"],
            "nf_entities_id": row["nf_entities_id"],
            "entity_name": row["entity_name"],
            "nf_entities_value": row["nf_entities_value"],
            "nf_tags_id": row["nf_tags_id"],
            "tag_name": row["tag_name"],
            "workspace_id": row["workspace_id"],
            "created_by": row["created_by"],
            "is_active": row["is_active"],
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"]
        }
        
        return {
            "success": True,
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting NewsBreak data by ID: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/newsbreak/urls")
async def get_newsbreak_urls(page: int = 1, limit: int = 100):
    """Retrieve collected NewsBreak URLs with pagination"""
    try:
        offset = (page - 1) * limit
        
        async with url_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT url, created_at, last_scraped, scrape_count FROM urls
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
            """, limit, offset)
        
        data = [{
            "url": row["url"],
            "created_at": row["created_at"],
            "last_scraped": row["last_scraped"],
            "scrape_count": row["scrape_count"]
        } for row in rows]
        
        return {
            "success": True,
            "data": data
        }
        
    except Exception as e:
        logger.error(f"Error getting NewsBreak URLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/newsbreak/urls")
async def add_urls_to_queue(request: AddUrlsRequest):
    """Add URLs to the scraping queue"""
    try:
        urls_added = 0
        duplicates_skipped = 0
        
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
                        routing_key=SCRAPER_QUEUE
                    )
                    urls_added += 1
        
        # Get current queue size
        scraper_queue = await rabbitmq_channel.declare_queue(
            SCRAPER_QUEUE, 
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": DLX_QUEUE
            }
        )
        
        return {
            "success": True,
            "data": {
                "urls_added": urls_added,
                "duplicates_skipped": duplicates_skipped,
                "queue_size": scraper_queue.declaration_result.message_count
            }
        }
        
    except Exception as e:
        logger.error(f"Error adding URLs to queue: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/newsbreak/urls/deduplicate")
async def deduplicate_urls():
    """Run deduplication process on collected URLs"""
    try:
        import time
        start_time = time.time()
        
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
        
        return {
            "success": True,
            "data": {
                "duplicates_removed": duplicates_removed,
                "unique_urls_remaining": final_count,
                "processing_time": round(processing_time, 2)
            }
        }
        
    except Exception as e:
        logger.error(f"Error deduplicating URLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connections
        async with data_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        
        async with url_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        
        # Check RabbitMQ connection
        await rabbitmq_channel.declare_queue("health_check", durable=False, auto_delete=True)
        
        # Get worker status
        worker_status = {
            "scraper_workers": len(scraper_workers),
            "data_worker_running": data_worker.is_running,
            "url_worker_running": url_worker.is_running,
            "url_feeder_running": url_feeder.is_running
        }
        
        return {
            "success": True,
            "status": "healthy",
            "worker_status": worker_status,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "success": False,
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)