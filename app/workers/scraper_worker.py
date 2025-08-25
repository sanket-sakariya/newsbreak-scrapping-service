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
                        url = message.body.decode()
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

                    except Exception as e:
                        logger.error(f"Worker {self.worker_number} error processing message: {e}")
                        await message.nack(requeue=False)
                        await self.send_to_dlx(url, str(e))

        except Exception as e:
            logger.error(f"Worker {self.worker_number} consume error: {e}")

    async def scrape_url(self, url: str):
        try:
            logger.debug(f"Scraping URL: {url}")
            async with self.session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                extracted_urls = self.extract_urls(soup, url)
                extracted_data = None
                if self.should_extract_data(url):
                    extracted_data = self.extract_next_data(soup)
                logger.debug(f"Extracted {len(extracted_urls)} URLs and {'data' if extracted_data else 'no data'} from {url}")
                return extracted_urls, extracted_data
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

    def extract_next_data(self, soup: BeautifulSoup) -> Optional[Dict]:
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
        try:
            return int(value) if value is not None else 0
        except (TypeError, ValueError):
            return 0

    async def handle_extracted_data(self, urls: List[str], data: Optional[Dict]):
        try:
            if urls:
                unique_urls = list(set(urls))
                urls_sent = 0
                for url in unique_urls:
                    try:
                        if await self.check_deduplication(url):
                            await db_manager.get_rabbitmq_channel().default_exchange.publish(
                                aio_pika.Message(url.encode()),
                                routing_key=settings.newsbreak_urls_queue
                            )
                    except Exception as e:
                        logger.error(f"Error publishing URL to queues: {e}")
                logger.info(f"Queued {urls_sent}/{len(unique_urls)} deduped URLs to both queues")

            if data:
                await db_manager.get_rabbitmq_channel().default_exchange.publish(
                    aio_pika.Message(json.dumps(data).encode()),
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

    async def send_to_dlx(self, url: str, error: str):
        try:
            message_data = {'url': url, 'error': error, 'timestamp': datetime.utcnow().isoformat(), 'worker_number': self.worker_number}
            await db_manager.get_rabbitmq_channel().default_exchange.publish(
                aio_pika.Message(json.dumps(message_data).encode()),
                routing_key=settings.dlx_queue
            )
        except Exception as e:
            logger.error(f"Error sending to DLX: {e}")


