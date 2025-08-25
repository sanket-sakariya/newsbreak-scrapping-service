import asyncio
import aiohttp
import aio_pika
import json
import logging
from datetime import datetime
from typing import List, Optional, Dict, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from .config import settings
from .database import db_manager

logger = logging.getLogger(__name__)

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
            timeout=aiohttp.ClientTimeout(total=settings.worker_timeout),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        
        logger.info(f"Worker {self.worker_id} started")
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
            scraper_queue = await db_manager.get_rabbitmq_channel().declare_queue(
                settings.scraper_queue, 
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": settings.dlx_queue
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
                            await self.update_url_processed(url)
                            
                        self.processed_count += 1
                        await message.ack()
                        await asyncio.sleep(1)  # Small delay
                        
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
            # Find the __NEXT_DATA__ script tag
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            if not script_tag:
                logger.debug("No __NEXT_DATA__ script tag found")
                return None
            
            # Parse the JSON data
            json_data = json.loads(script_tag.string)
            page_props = json_data.get('props', {}).get('pageProps', {})
            
            # Handle both direct pageProps and nested docInfo structures
            doc_info = page_props.get('docInfo', {}) or page_props
            
            # Extract article ID
            article_id = (doc_info.get('docid') or 
                         doc_info.get('id') or 
                         doc_info.get('article_id') or 
                         doc_info.get('slug'))
            
            if not article_id:
                logger.debug("No article ID found in data")
                return None
            
            # Extract the data with proper field mapping
            extracted_data = {
                "id": str(article_id),
                "title": doc_info.get('title') or doc_info.get('headline'),
                "source": doc_info.get('source') or doc_info.get('source_name'),
                "cityName": doc_info.get('cityName') or doc_info.get('city_name'),
                "likeCount": self._safe_int(doc_info.get('likeCount') or doc_info.get('like')),
                "commentCount": self._safe_int(doc_info.get('commentCount') or doc_info.get('comment_count')),
                "share_count": self._safe_int(doc_info.get('share_count') or doc_info.get('shares')),
                "origin_url": doc_info.get('origin_url') or doc_info.get('original_url'),
                "text_category": doc_info.get('text_category', {}),
                "nf_entities": doc_info.get('nf_entities', {}),
                "nf_tags": doc_info.get('nf_tags', [])
            }
            
            return extracted_data
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from __NEXT_DATA__: {e}")
            return None
        except Exception as e:
            logger.error(f"Error extracting __NEXT_DATA__: {e}")
            return None
    
    def _safe_int(self, value) -> int:
        """Safely convert value to int"""
        try:
            return int(value) if value is not None else 0
        except (TypeError, ValueError):
            return 0
    
    async def handle_extracted_data(self, urls: List[str], data: Optional[Dict]):
        """Send extracted URLs and data to their respective queues"""
        try:
            # Send URLs to newsbreak_urls_queue
            if urls:
                for url in urls:
                    await db_manager.get_rabbitmq_channel().default_exchange.publish(
                        aio_pika.Message(url.encode()),
                        routing_key=settings.newsbreak_urls_queue
                    )
                logger.debug(f"Sent {len(urls)} URLs to URL queue")
            
            # Send data to newsbreak_data_queue
            if data:
                await db_manager.get_rabbitmq_channel().default_exchange.publish(
                    aio_pika.Message(json.dumps(data).encode()),
                    routing_key=settings.newsbreak_data_queue
                )
                logger.debug(f"Sent data for ID {data.get('id')} to data queue")
                
        except Exception as e:
            logger.error(f"Error handling extracted data: {e}")
    
    async def update_url_processed(self, url: str):
        """Update URL processing timestamp"""
        try:
            async with db_manager.get_url_pool().acquire() as conn:
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
            await db_manager.get_rabbitmq_channel().default_exchange.publish(
                aio_pika.Message(json.dumps(message_data).encode()),
                routing_key=settings.dlx_queue
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
            data_queue = await db_manager.get_rabbitmq_channel().declare_queue(
                settings.newsbreak_data_queue, 
                durable=True
            )
            
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
            async with db_manager.get_data_pool().acquire() as conn:
                async with conn.transaction():
                    logger.info(f"Processing data for ID: {data.get('id')}")
                    
                    # Insert or get foreign key references
                    source_id = await self.get_or_create_source(conn, data.get('source'))
                    city_id = await self.get_or_create_city(conn, data.get('cityName'))
                    
                    # Handle text categories properly
                    text_category = data.get('text_category', {})
                    logger.info(f"Raw text_category data: {text_category}")
                    
                    # Process each category level
                    first_cat_id, first_cat_value = await self.process_category(
                        conn, text_category.get('first_cat', {})
                    )
                    second_cat_id, second_cat_value = await self.process_category(
                        conn, text_category.get('second_cat', {})
                    )
                    third_cat_id, third_cat_value = await self.process_category(
                        conn, text_category.get('third_cat', {})
                    )
                    
                    logger.info(f"Processed categories:")
                    logger.info(f"  First: ID={first_cat_id}, value={first_cat_value}")
                    logger.info(f"  Second: ID={second_cat_id}, value={second_cat_value}")
                    logger.info(f"  Third: ID={third_cat_id}, value={third_cat_value}")
                    
                    # Handle entities and tags
                    entities_id, entities_value = await self.process_entities(conn, data.get('nf_entities', {}))
                    tags_id = await self.process_tags(conn, data.get('nf_tags', []))
                    
                    # Insert main data
                    await conn.execute("""
                        INSERT INTO newsbreak_data (
                            id, likeCount, commentCount, source_id, city_id, title, origin_url, share_count,
                            first_text_category_id, second_text_category_id, third_text_category_id,
                            first_text_category_value, second_text_category_value, third_text_category_value,
                            nf_entities_id, nf_entities_value, nf_tags_id, status
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                        ON CONFLICT (id) DO UPDATE SET
                            likeCount = EXCLUDED.likeCount,
                            commentCount = EXCLUDED.commentCount,
                            share_count = EXCLUDED.share_count,
                            first_text_category_id = EXCLUDED.first_text_category_id,
                            second_text_category_id = EXCLUDED.second_text_category_id,
                            third_text_category_id = EXCLUDED.third_text_category_id,
                            first_text_category_value = EXCLUDED.first_text_category_value,
                            second_text_category_value = EXCLUDED.second_text_category_value,
                            third_text_category_value = EXCLUDED.third_text_category_value,
                            nf_entities_id = EXCLUDED.nf_entities_id,
                            nf_entities_value = EXCLUDED.nf_entities_value,
                            nf_tags_id = EXCLUDED.nf_tags_id,
                            updated_at = CURRENT_TIMESTAMP
                    """, 
                        data.get('id'), data.get('likeCount', 0), data.get('commentCount', 0),
                        source_id, city_id, data.get('title'), data.get('origin_url'), data.get('share_count', 0),
                        first_cat_id, second_cat_id, third_cat_id,
                        first_cat_value, second_cat_value, third_cat_value,
                        entities_id, entities_value, tags_id,
                        'created'
                    )
                    
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            raise
    
    async def get_or_create_source(self, conn, source_name: str) -> Optional[int]:
        """Get or create source and return ID"""
        if not source_name:
            return None
        
        result = await conn.fetchval("SELECT source_id FROM source_data WHERE source_name = $1", source_name)
        if result:
            return result
        
        return await conn.fetchval("INSERT INTO source_data (source_name) VALUES ($1) RETURNING source_id", source_name)
    
    async def get_or_create_city(self, conn, city_name: str) -> Optional[int]:
        """Get or create city and return ID"""
        if not city_name:
            return None
        
        result = await conn.fetchval("SELECT city_id FROM city_data WHERE city_name = $1", city_name)
        if result:
            return result
        
        return await conn.fetchval("INSERT INTO city_data (city_name) VALUES ($1) RETURNING city_id", city_name)
    
    async def process_category(self, conn, category: Dict) -> Tuple[Optional[int], Optional[float]]:
        """Process category data and return category ID and confidence value"""
        if not category or not isinstance(category, dict):
            return None, None

        try:
            # Extract the first (and typically only) key-value pair
            # Example: {"PoliticsGovernment": 0.92442}
            category_name = list(category.keys())[0]
            category_value = list(category.values())[0]
            
            # Convert value to float
            try:
                confidence_value = float(category_value) if category_value is not None else None
            except (TypeError, ValueError):
                logger.warning(f"Could not convert category value to float: {category_value}")
                confidence_value = None
            
            # Insert or get category ID from text_category_data table
            category_id = await conn.fetchval(
                """
                INSERT INTO text_category_data (category_name)
                VALUES ($1)
                ON CONFLICT (category_name) DO UPDATE SET category_name = EXCLUDED.category_name
                RETURNING text_category_id
                """,
                category_name
            )
            
            logger.debug(f"Category '{category_name}' -> ID: {category_id}, Value: {confidence_value}")
            return category_id, confidence_value
            
        except (IndexError, KeyError) as e:
            logger.error(f"Error processing category {category}: {e}")
            return None, None
    
    async def process_entities(self, conn, entities_data: Dict) -> Tuple[Optional[int], Optional[float]]:
        """Process entities data and return entity ID and value"""
        if not entities_data or not isinstance(entities_data, dict):
            return None, None
        
        try:
            # Extract first entity (example: {"Donald_Trump": 6})
            entity_name = list(entities_data.keys())[0]
            entity_value = list(entities_data.values())[0]
            
            # Get or create entity ID
            entity_id = await conn.fetchval(
                """
                INSERT INTO nf_entities_data (entity_name)
                VALUES ($1)
                ON CONFLICT (entity_name) DO UPDATE SET entity_name = EXCLUDED.entity_name
                RETURNING nf_entities_id
                """,
                entity_name
            )
            
            # Convert value to float if possible
            try:
                entity_float_value = float(entity_value) if entity_value is not None else None
            except (TypeError, ValueError):
                entity_float_value = None
            
            return entity_id, entity_float_value
            
        except (IndexError, KeyError) as e:
            logger.error(f"Error processing entities {entities_data}: {e}")
            return None, None
    
    async def process_tags(self, conn, tags_data: List) -> Optional[int]:
        """Process tags data and return first tag ID"""
        if not tags_data or not isinstance(tags_data, list):
            return None
        
        try:
            # Get first tag (example: ["Political_Figures"])
            tag_name = tags_data[0]
            
            # Get or create tag ID
            tag_id = await conn.fetchval(
                """
                INSERT INTO nf_tags_data (tag_name)
                VALUES ($1)
                ON CONFLICT (tag_name) DO UPDATE SET tag_name = EXCLUDED.tag_name
                RETURNING nf_tags_id
                """,
                tag_name
            )
            
            return tag_id
            
        except (IndexError, KeyError) as e:
            logger.error(f"Error processing tags {tags_data}: {e}")
            return None

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
        """Insert URL into database (with deduplication)"""
        try:
            async with db_manager.get_url_pool().acquire() as conn:
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
        self.feed_interval = settings.feed_interval
        self.homepage_url = "https://www.newsbreak.com"
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_bootstrap = None
    
    async def start(self):
        """Start the URL feeder"""
        self.is_running = True
        
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=settings.worker_timeout),
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
                await asyncio.sleep(10)
    
    async def stop(self):
        """Stop the URL feeder"""
        self.is_running = False
        if self.session:
            await self.session.close()
        logger.info("URL feeder stopped")
    
    async def feed_urls(self):
        """Feed URLs from database to scraper queue, fetch homepage if no URLs available"""
        try:
            scraper_queue = await db_manager.get_rabbitmq_channel().declare_queue(
                settings.scraper_queue, 
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": settings.dlx_queue
                }
            )
            
            current_queue_size = scraper_queue.declaration_result.message_count
            
            if current_queue_size < 50:
                async with db_manager.get_url_pool().acquire() as conn:
                    urls = await conn.fetch("""
                        SELECT url FROM urls 
                        WHERE (last_scraped IS NULL OR last_scraped < NOW() - INTERVAL '1 hour')
                        ORDER BY 
                            CASE WHEN last_scraped IS NULL THEN 0 ELSE 1 END,
                            last_scraped ASC,
                            scrape_count ASC
                        LIMIT $1
                    """, self.batch_size)
                
                if not urls:
                    logger.info("No unprocessed URLs found, bootstrapping from homepage")
                    await self.bootstrap_from_homepage()
                    return
                
                urls_added = 0
                for url_row in urls:
                    try:
                        await db_manager.get_rabbitmq_channel().default_exchange.publish(
                            aio_pika.Message(url_row["url"].encode()),
                            routing_key=settings.scraper_queue
                        )
                        urls_added += 1
                    except Exception as e:
                        logger.error(f"Error publishing URL to queue: {e}")
                
                if urls_added > 0:
                    logger.info(f"Fed {urls_added} URLs to scraper queue")
                    
        except Exception as e:
            logger.error(f"Error feeding URLs: {e}")
    
    async def bootstrap_from_homepage(self):
        """Fetch NewsBreak homepage and extract initial URLs"""
        try:
            now = datetime.utcnow()
            if self.last_bootstrap and (now - self.last_bootstrap).seconds < settings.bootstrap_interval:
                return
            
            self.last_bootstrap = now
            logger.info(f"Bootstrapping from homepage: {self.homepage_url}")
            
            async with self.session.get(self.homepage_url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch homepage, status: {response.status}")
                    return
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                extracted_urls = self.extract_newsbreak_urls(soup, self.homepage_url)
                
                if not extracted_urls:
                    logger.warning("No URLs extracted from homepage")
                    return
                
                unique_urls = list(set(extracted_urls))
                urls_inserted = 0
                
                async with db_manager.get_url_pool().acquire() as conn:
                    for url in unique_urls:
                        try:
                            await conn.execute(
                                "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING", 
                                url
                            )
                            urls_inserted += 1
                        except Exception as e:
                            logger.error(f"Error inserting URL {url}: {e}")
                
                urls_to_queue = unique_urls[:20]
                queued_count = 0
                
                for url in urls_to_queue:
                    try:
                        await db_manager.get_rabbitmq_channel().default_exchange.publish(
                            aio_pika.Message(url.encode()),
                            routing_key=settings.scraper_queue
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
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                urls.append(full_url)
            
            unique_urls = list(set(urls))
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

# Global worker instances
data_worker = DataWorker()
url_worker = UrlWorker()
url_feeder = UrlFeeder()