import asyncio
import aiohttp
import aio_pika
import logging
from datetime import datetime
from typing import List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from app.core.config import settings
from app.core.database import db_manager

logger = logging.getLogger(__name__)


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
        self.is_running = False
        if self.session:
            await self.session.close()
        logger.info("URL feeder stopped")
    
    async def feed_urls(self):
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
                    urls = await conn.fetch(
                        """
                        SELECT url FROM urls 
                        ORDER BY created_at ASC
                        LIMIT $1
                        """,
                        self.batch_size
                    )
                
                if not urls:
                    logger.info("No URLs found in DB, bootstrapping from NewsBreak categories")
                    await self.bootstrap_from_categories()
                    return
                
                urls_added = 0
                for url_row in urls:
                    try:
                        await db_manager.get_rabbitmq_channel().default_exchange.publish(
                            aio_pika.Message(url_row["url"].encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
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
                            aio_pika.Message(url.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                            routing_key=settings.scraper_queue
                        )
                        queued_count += 1
                    except Exception as e:
                        logger.error(f"Error queuing URL {url}: {e}")
                
                logger.info(f"Bootstrap completed: {urls_inserted} URLs inserted, {queued_count} URLs queued")
                
        except Exception as e:
            logger.error(f"Error bootstrapping from homepage: {e}")
    
    async def bootstrap_from_categories(self):
        """Seed DB and queue with a set of NewsBreak category URLs"""
        try:
            categories = [
                "https://www.newsbreak.com/trending",
                "https://www.newsbreak.com/local",
                "https://www.newsbreak.com/politics",
                "https://www.newsbreak.com/sports",
                "https://www.newsbreak.com/business",
                "https://www.newsbreak.com/entertainment",
                "https://www.newsbreak.com/technology",
                "https://www.newsbreak.com/health",
                "https://www.newsbreak.com/science",
                "https://www.newsbreak.com/lifestyle",
            ]

            async with db_manager.get_url_pool().acquire() as conn:
                inserted = 0
                for url in categories:
                    try:
                        await conn.execute(
                            "INSERT INTO urls (url) VALUES ($1) ON CONFLICT (url) DO NOTHING",
                            url
                        )
                        inserted += 1
                    except Exception as e:
                        logger.debug(f"Category insert skipped/failed for {url}: {e}")

            queued = 0
            for url in categories:
                try:
                    await db_manager.get_rabbitmq_channel().default_exchange.publish(
                        aio_pika.Message(url.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                        routing_key=settings.scraper_queue
                    )
                    queued += 1
                except Exception as e:
                    logger.error(f"Error queuing category URL {url}: {e}")

            logger.info(f"Category bootstrap completed: inserted={inserted}, queued={queued}")

        except Exception as e:
            logger.error(f"Error bootstrapping categories: {e}")

    def extract_newsbreak_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
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
            return self.filter_unwanted_urls(unique_urls)
        except Exception as e:
            logger.error(f"Error extracting URLs from homepage: {e}")
            return []
    
    def filter_unwanted_urls(self, urls: List[str]) -> List[str]:
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


