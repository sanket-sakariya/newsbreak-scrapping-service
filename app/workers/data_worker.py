import aio_pika
import json
import logging
import asyncio
from datetime import datetime
from typing import Optional, Dict, Tuple, List
from app.core.config import settings
from app.core.database import db_manager
from app.core.utils import extract_domain_from_url

logger = logging.getLogger(__name__)


class DataWorker:
    def __init__(self):
        self.is_running = False
        self.processed_count = 0
        self._sem = asyncio.Semaphore(getattr(settings, 'data_worker_concurrency', 5))
        self._pending: set[asyncio.Task] = set()

    async def start(self):
        self.is_running = True
        logger.info("Data worker started")
        await self.consume_data_queue()

    async def stop(self):
        self.is_running = False
        # Wait for in-flight tasks to complete gracefully
        try:
            if self._pending:
                await asyncio.gather(*self._pending, return_exceptions=True)
        except Exception:
            pass
        logger.info(f"Data worker stopped. Processed {self.processed_count} records")

    async def consume_data_queue(self):
        try:
            data_queue = await db_manager.get_rabbitmq_channel().declare_queue(
                settings.newsbreak_data_queue,
                durable=True
            )
            async with data_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self.is_running:
                        break
                    # Process concurrently with bounded concurrency
                    task = asyncio.create_task(self._handle_message(message))
                    self._pending.add(task)
                    task.add_done_callback(self._pending.discard)
        except Exception as e:
            logger.error(f"Data worker error: {e}")

    async def _handle_message(self, message: aio_pika.IncomingMessage):
        async with self._sem:
            try:
                data = json.loads(message.body.decode())
                await self.insert_data(data)
                await message.ack()
                self.processed_count += 1
                # Log progress every 100 records instead of every record
                if self.processed_count % 100 == 0:
                    logger.info(f"Data worker processed {self.processed_count} records")
            except Exception as e:
                logger.error(f"Error processing data message: {e}")
                try:
                    await message.nack(requeue=False)
                except Exception:
                    pass

    async def insert_data(self, data: Dict):
        # Inline the existing logic from app/core/workers.py::DataWorker.insert_data
        try:
            # Skip records without a valid origin_url
            origin_url = data.get('origin_url')
            if not origin_url or (isinstance(origin_url, str) and origin_url.strip() == ""):
                logger.warning("Skipping insert: origin_url is missing or empty for ID %s", data.get('id'))
                return
            async with db_manager.get_data_pool().acquire() as conn:
                async with conn.transaction():
                    logger.debug(f"Processing data for ID: {data.get('id')}")  # Changed to debug
                    source_id = await self.get_or_create_source(conn, data.get('source'))
                    city_id = await self.get_or_create_city(conn, data.get('cityName'))
                    text_category = data.get('text_category', {})
                    logger.debug(f"Raw text_category data: {text_category}")  # Changed to debug
                    first_cat_id, first_cat_value = await self.process_category(conn, text_category.get('first_cat', {}))
                    second_cat_id, second_cat_value = await self.process_category(conn, text_category.get('second_cat', {}))
                    third_cat_id, third_cat_value = await self.process_category(conn, text_category.get('third_cat', {}))
                    logger.debug(f"Processed categories:")  # Changed to debug
                    logger.debug(f"  First: ID={first_cat_id}, value={first_cat_value}")  # Changed to debug
                    logger.debug(f"  Second: ID={second_cat_id}, value={second_cat_value}")  # Changed to debug
                    logger.debug(f"  Third: ID={third_cat_id}, value={third_cat_value}")  # Changed to debug
                    entities_id, entities_value = await self.process_entities(conn, data.get('nf_entities', {}))
                    tags_id = await self.process_tags(conn, data.get('nf_tags', []))
                    domain_id = await self.get_or_create_domain(conn, data.get('origin_url'))
                    
                    # Process new fields
                    date_value = self.parse_date(data.get('date'))
                    wordcount = data.get('wordcount') if data.get('wordcount') is not None else None
                    images = data.get('images')
                    await conn.execute(
                        """
                        INSERT INTO newsbreak_data (
                            id, likeCount, commentCount, source_id, city_id, title, origin_url, domain_id, share_count,
                            first_text_category_id, second_text_category_id, third_text_category_id,
                            first_text_category_value, second_text_category_value, third_text_category_value,
                            nf_entities_id, nf_entities_value, nf_tags_id, date, wordcount, images, status
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
                        ON CONFLICT (id) DO UPDATE SET
                            likeCount = EXCLUDED.likeCount,
                            commentCount = EXCLUDED.commentCount,
                            share_count = EXCLUDED.share_count,
                            domain_id = EXCLUDED.domain_id,
                            first_text_category_id = EXCLUDED.first_text_category_id,
                            second_text_category_id = EXCLUDED.second_text_category_id,
                            third_text_category_id = EXCLUDED.third_text_category_id,
                            first_text_category_value = EXCLUDED.first_text_category_value,
                            second_text_category_value = EXCLUDED.second_text_category_value,
                            third_text_category_value = EXCLUDED.third_text_category_value,
                            nf_entities_id = EXCLUDED.nf_entities_id,
                            nf_entities_value = EXCLUDED.nf_entities_value,
                            nf_tags_id = EXCLUDED.nf_tags_id,
                            date = EXCLUDED.date,
                            wordcount = EXCLUDED.wordcount,
                            images = EXCLUDED.images,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        data.get('id'), data.get('likeCount', 0), data.get('commentCount', 0),
                        source_id, city_id, data.get('title'), data.get('origin_url'), domain_id, data.get('share_count', 0),
                        first_cat_id, second_cat_id, third_cat_id,
                        first_cat_value, second_cat_value, third_cat_value,
                        entities_id, entities_value, tags_id,
                        date_value, wordcount, images,
                        'created'
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

    async def get_or_create_domain(self, conn, url: str) -> Optional[int]:
        if not url:
            return None
        domain = extract_domain_from_url(url)
        if not domain:
            return None
        result = await conn.fetchval("SELECT domain_id FROM domain_data WHERE domain_name = $1", domain)
        if result:
            return result
        return await conn.fetchval("INSERT INTO domain_data (domain_name) VALUES ($1) RETURNING domain_id", domain)

    async def process_category(self, conn, category: Dict) -> Tuple[Optional[int], Optional[float]]:
        if not category or not isinstance(category, dict):
            return None, None
        try:
            category_name = list(category.keys())[0]
            category_value = list(category.values())[0]
            try:
                confidence_value = float(category_value) if category_value is not None else None
            except (TypeError, ValueError):
                logger.warning(f"Could not convert category value to float: {category_value}")
                confidence_value = None
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
        if not entities_data or not isinstance(entities_data, dict):
            return None, None
        try:
            entity_name = list(entities_data.keys())[0]
            entity_value = list(entities_data.values())[0]
            entity_id = await conn.fetchval(
                """
                INSERT INTO nf_entities_data (entity_name)
                VALUES ($1)
                ON CONFLICT (entity_name) DO UPDATE SET entity_name = EXCLUDED.entity_name
                RETURNING nf_entities_id
                """,
                entity_name
            )
            try:
                entity_float_value = float(entity_value) if entity_value is not None else None
            except (TypeError, ValueError):
                entity_float_value = None
            return entity_id, entity_float_value
        except (IndexError, KeyError) as e:
            logger.error(f"Error processing entities {entities_data}: {e}")
            return None, None

    async def process_tags(self, conn, tags_data: List) -> Optional[int]:
        if not tags_data or not isinstance(tags_data, list):
            return None
        try:
            tag_name = tags_data[0]
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

    def parse_date(self, date_value) -> Optional[datetime]:
        """Parse date string to full datetime object with time"""
        if not date_value:
            return None
        
        try:
            # Handle different date formats
            if isinstance(date_value, str):
                # Try common datetime formats (with time)
                datetime_formats = [
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%SZ',
                    '%Y-%m-%dT%H:%M:%S.%fZ',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%m/%d/%Y %H:%M:%S',
                    '%d/%m/%Y %H:%M:%S',
                    '%Y-%m-%d'  # Date only - will add default time
                ]
                
                for fmt in datetime_formats:
                    try:
                        parsed_datetime = datetime.strptime(date_value, fmt)
                        return parsed_datetime
                    except ValueError:
                        continue
                
                # If none work, try to parse as timestamp
                try:
                    timestamp = float(date_value)
                    return datetime.fromtimestamp(timestamp)
                except (ValueError, OSError):
                    pass
                    
            elif isinstance(date_value, (int, float)):
                # Assume it's a timestamp
                return datetime.fromtimestamp(date_value)
                
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_value}': {e}")
        
        return None



