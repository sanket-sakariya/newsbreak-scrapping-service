import redis.asyncio as redis
import json
import logging
from typing import Optional, Any, Dict, List
from app.core.config import settings

logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self):
        self.redis: Optional[redis.Redis] = None
        self.is_connected = False
        
    async def connect(self):
        """Connect to Redis with multiple fallback methods"""
        connection_methods = [
            # Method 1: Try URL first
            lambda: redis.from_url(
                settings.redis_url,
                db=settings.redis_db,
                encoding="utf-8",
                decode_responses=True
            ),
            # Method 2: Try host/port with password
            lambda: redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                password=settings.redis_password,
                db=settings.redis_db,
                encoding="utf-8",
                decode_responses=True
            ),
            # Method 3: Try without password (in case auth is disabled)
            lambda: redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                encoding="utf-8",
                decode_responses=True
            )
        ]
        
        for i, method in enumerate(connection_methods, 1):
            try:
                print(f"🔌 Trying Redis connection method {i}...")
                self.redis = method()
                
                # Test connection
                await self.redis.ping()
                self.is_connected = True
                logger.info(f"Redis cache connected successfully using method {i}")
                return
                
            except redis.AuthenticationError as e:
                print(f"❌ Method {i} failed: Authentication error - {e}")
                if i == 2:  # Method 2 failed, try method 3 without password
                    continue
                else:
                    logger.error(f"Redis authentication failed: {e}")
                    logger.error("Please check your REDIS_PASSWORD in .env file")
                    break
            except redis.ConnectionError as e:
                print(f"❌ Method {i} failed: Connection error - {e}")
                if i < len(connection_methods):
                    continue
                else:
                    logger.error(f"Redis connection failed: {e}")
                    logger.error("Please ensure Redis server is running and accessible")
                    break
            except Exception as e:
                print(f"❌ Method {i} failed: {e}")
                if i < len(connection_methods):
                    continue
                else:
                    logger.error(f"Failed to connect to Redis: {e}")
                    break
        
        self.is_connected = False
        logger.error("All Redis connection methods failed")
            
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis:
            await self.redis.close()
            self.is_connected = False
            logger.info("Redis cache disconnected")
            
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if not self.is_connected or not self.redis:
            return None
            
        try:
            value = await self.redis.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Error getting from cache: {e}")
            return None
            
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with optional TTL"""
        if not self.is_connected or not self.redis:
            return False
            
        try:
            ttl = ttl or settings.redis_cache_ttl
            await self.redis.setex(key, ttl, json.dumps(value))
            return True
        except Exception as e:
            logger.error(f"Error setting cache: {e}")
            return False
            
    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        if not self.is_connected or not self.redis:
            return False
            
        try:
            await self.redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Error deleting from cache: {e}")
            return False
            
    async def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        if not self.is_connected or not self.redis:
            return False
            
        try:
            return await self.redis.exists(key) > 0
        except Exception as e:
            logger.error(f"Error checking cache existence: {e}")
            return False
            
    async def clear_pattern(self, pattern: str) -> bool:
        """Clear all keys matching a pattern"""
        if not self.is_connected or not self.redis:
            return False
            
        try:
            keys = await self.redis.keys(pattern)
            if keys:
                await self.redis.delete(*keys)
                logger.info(f"Cleared {len(keys)} cache keys matching pattern: {pattern}")
            return True
        except Exception as e:
            logger.error(f"Error clearing cache pattern: {e}")
            return False

# Global cache instance
cache = RedisCache()
