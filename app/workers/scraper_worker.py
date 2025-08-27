import asyncio
import aiohttp
import aio_pika
import json
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from app.core.config import settings
from app.core.database import db_manager

logger = logging.getLogger(__name__)


class HTTPError(Exception):
    """Custom HTTP error with status code"""
    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


class ScraperWorker:
    def __init__(self, worker_number: int, batch_size: int = 100):
        self.worker_number = worker_number
        self.batch_size = batch_size
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_running = False
        self.processed_count = 0

    async def start(self):
        self.is_running = True
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=settings.worker_timeout),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        logger.info(f"Worker {self.worker_number} started")
        await self.consume_scraper_queue()

    async def stop(self):
        self.is_running = False
        if self.session:
            await self.session.close()
        logger.info(f"Worker {self.worker_number} stopped. Processed {self.processed_count} URLs")

    async def consume_scraper_queue(self):
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
                        # Try to parse as JSON (for retry messages) or plain URL
                        message_body = message.body.decode()
                        retry_count = 0
                        
                        try:
                            # Check if it's a retry message with JSON data
                            message_data = json.loads(message_body)
                            url = message_data.get('url', message_body)
                            retry_count = message_data.get('retry_count', 0)
                            if retry_count > 0:
                                logger.info(f"Worker {self.worker_number} processing retry URL: {url} (attempt {retry_count}/3)")
                            else:
                                logger.info(f"Worker {self.worker_number} processing URL: {url}")
                        except json.JSONDecodeError:
                            # Plain URL message (not a retry)
                            url = message_body
                            logger.info(f"Worker {self.worker_number} processing URL: {url}")

                        result = await self.scrape_url(url)
                        if result:
                            extracted_urls, extracted_data = result
                            await self.handle_extracted_data(extracted_urls, extracted_data)

                        self.processed_count += 1
                        if self.processed_count % 50 == 0:
                            logger.info(f"Worker {self.worker_number} processed {self.processed_count} URLs")
                        await message.ack()
                        await asyncio.sleep(1)

                    except HTTPError as e:
                        # Handle HTTP errors based on status code
                        if 500 <= e.status_code < 600:
                            # 5xx errors (server errors) - send to DLX for retry
                            logger.warning(f"Worker {self.worker_number} server error {e.status_code} for URL: {url}")
                            await message.nack(requeue=False)
                            await self.send_to_dlx(url, str(e), retry_count)
                        else:
                            # 4xx errors (client errors) - send to urls_queue with status code
                            logger.info(f"Worker {self.worker_number} client error {e.status_code} for URL: {url} - sending to urls_queue")
                            await self.send_4xx_to_urls_queue(url, e.status_code)
                            await message.ack()  # Acknowledge to remove from queue
                    except Exception as e:
                        # Other errors (network, parsing, etc.) - send to DLX
                        logger.error(f"Worker {self.worker_number} error processing message: {e}")
                        await message.nack(requeue=False)
                        await self.send_to_dlx(url, str(e), retry_count)

        except Exception as e:
            logger.error(f"Worker {self.worker_number} consume error: {e}")

    async def scrape_url(self, url: str):
        try:
            logger.debug(f"Scraping URL: {url}")
            async with self.session.get(url) as response:
                if response.status != 200:
                    # Create custom exception with status code for proper error handling
                    raise HTTPError(f"HTTP {response.status}", response.status)
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                extracted_urls = self.extract_urls(soup, url)
                extracted_data = None
                if self.should_extract_data(url):
                    extracted_data = self.extract_next_data(soup, html)
                logger.debug(f"Extracted {len(extracted_urls)} URLs and {'data' if extracted_data else 'no data'} from {url}")
                return extracted_urls, extracted_data
        except HTTPError:
            # Re-raise HTTPError to preserve status code
            raise
        except Exception as e:
            logger.error(f"Failed to scrape {url}: {e}")
            raise

    def should_extract_data(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            path = (parsed.path or '').lower()
            if "/trending/top/" in path:
                return False
            tokens = [t for t in path.split('/') if t]
            for token in tokens:
                digits = ''.join(ch for ch in token if ch.isdigit())
                if len(digits) >= 10:
                    return True
            return True
        except Exception:
            return True

    def extract_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        try:
            urls = set()
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href:
                    full_url = urljoin(base_url, href) if href.startswith('/') else href
                    if 'newsbreak.com' in full_url:
                        parsed = urlparse(full_url)
                        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                        if clean_url != base_url:
                            urls.add(clean_url)
            return list(urls)
        except Exception as e:
            logger.error(f"Error extracting URLs: {e}")
            return []

    def extract_next_data(self, soup: BeautifulSoup, html: str) -> Optional[Dict]:
        try:
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            if not script_tag:
                logger.debug("No __NEXT_DATA__ script tag found")
                return None
            json_data = json.loads(script_tag.string)
            page_props = json_data.get('props', {}).get('pageProps', {})
            doc_info = page_props.get('docInfo', {}) or page_props
            article_id = (doc_info.get('docid') or doc_info.get('id') or doc_info.get('article_id') or doc_info.get('slug'))
            if not article_id:
                logger.debug("No article ID found in data")
                return None
            
            # Extract additional fields
            date = doc_info.get('date') or doc_info.get('publishDate') or doc_info.get('publish_date')
            wordcount = self._safe_int(doc_info.get('wordcount') or doc_info.get('word_count'))
            
            # Extract image using the DOM selector approach
            images = self.extract_images_from_html(html)
            
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
                "nf_tags": doc_info.get('nf_tags', []),
                "date": date,
                "wordcount": wordcount,
                "images": images
            }
            return extracted_data
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from __NEXT_DATA__: {e}")
            return None
        except Exception as e:
            logger.error(f"Error extracting __NEXT_DATA__: {e}")
            return None

    def _safe_int(self, value) -> int:
        try:
            return int(value) if value is not None else 0
        except (TypeError, ValueError):
            return 0
    
    def _safe_bool(self, value) -> Optional[bool]:
        try:
            if value is None:
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        except (TypeError, ValueError):
            return None
    
    def extract_images_from_html(self, html: str) -> Optional[str]:
        """Extract image using DOM selector approach similar to browser JS"""
        try:
            # Parse HTML and find images with the specific pattern
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find all images with src containing the pattern
            img_tags = soup.find_all('img', src=lambda x: x and 'https://img.particlenews.com/' in x)
            
            if len(img_tags) >= 2:
                # Get the second image (index 1) as specified
                return img_tags[1].get('src')
            elif len(img_tags) == 1:
                # If only one image, return it
                return img_tags[0].get('src')
            else:
                return None
        except Exception as e:
            logger.error(f"Error extracting images: {e}")
            return None

    async def handle_extracted_data(self, urls: List[str], data: Optional[Dict]):
        try:
            if urls:
                unique_urls = list(set(urls))
                urls_sent = 0
                for url in unique_urls:
                    try:
                        if await self.check_deduplication(url):
                            await db_manager.get_rabbitmq_channel().default_exchange.publish(
                                aio_pika.Message(url.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                                routing_key=settings.newsbreak_urls_queue
                            )
                    except Exception as e:
                        logger.error(f"Error publishing URL to queues: {e}")
                logger.info(f"Queued {urls_sent}/{len(unique_urls)} deduped URLs to both queues")

            if data:
                await db_manager.get_rabbitmq_channel().default_exchange.publish(
                    aio_pika.Message(json.dumps(data).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                    routing_key=settings.newsbreak_data_queue
                )
                logger.debug(f"Sent data for ID {data.get('id')} to data queue")
        except Exception as e:
            logger.error(f"Error handling extracted data: {e}")

    async def check_deduplication(self, url: str) -> bool:
        try:
            async with db_manager.get_url_pool().acquire() as conn:
                exists = await conn.fetchval(
                    "SELECT 1 FROM urls WHERE url = $1",
                    url
                )
                # If not in DB, allow queueing; if present, skip to avoid duplicates
                return not bool(exists)
        except Exception as e:
            logger.error(f"Dedup check failed for URL {url}: {e}")
            # Fail-open to avoid losing URLs
            return True

    # Removed DB timestamp updates per request; scraper worker remains read-only

    async def send_4xx_to_urls_queue(self, url: str, status_code: int):
        """Send 4xx error URLs to urls_queue with status code for storage without scraping"""
        try:
            message_data = {
                'url': url,
                'status_code': status_code,
                'timestamp': datetime.utcnow().isoformat(),
                'worker_number': self.worker_number
            }
            await db_manager.get_rabbitmq_channel().default_exchange.publish(
                aio_pika.Message(json.dumps(message_data).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=settings.newsbreak_urls_queue
            )
            logger.info(f"Sent 4xx URL {url} (status: {status_code}) to urls_queue")
        except Exception as e:
            logger.error(f"Error sending 4xx URL to urls_queue: {e}")

    async def send_to_dlx(self, url: str, error: str, retry_count: int = 0):
        try:
            message_data = {
                'url': url, 
                'error': error, 
                'retry_count': retry_count,
                'timestamp': datetime.utcnow().isoformat(), 
                'worker_number': self.worker_number
            }
            await db_manager.get_rabbitmq_channel().default_exchange.publish(
                aio_pika.Message(json.dumps(message_data).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=settings.dlx_queue
            )
            logger.info(f"Sent URL {url} to DLX (retry count: {retry_count})")
        except Exception as e:
            logger.error(f"Error sending to DLX: {e}")


