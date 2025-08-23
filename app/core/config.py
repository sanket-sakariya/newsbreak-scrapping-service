from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    # Application settings
    app_name: str = "NewsBreak Scraper API"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Database URLs
    database_url_1: str = os.getenv("DATABASE_URL_1", "postgresql://sanket:root@localhost:5432/newsbreak_scraper_data")
    database_url_2: str = os.getenv("DATABASE_URL_2", "postgresql://sanket:root@localhost:5432/newsbreak_scraper_urls")
    
    # RabbitMQ settings
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    
    # Queue names
    scraper_queue: str = "scraper_queue"
    newsbreak_urls_queue: str = "newsbreak_urls_queue"
    newsbreak_data_queue: str = "newsbreak_data_queue"
    dlx_queue: str = "dlx_queue"
    
    # Worker settings
    default_batch_size: int = 100
    default_worker_count: int = 5
    worker_timeout: int = 30
    
    # Scraping settings
    feed_interval: int = 30  # seconds
    bootstrap_interval: int = 300  # 5 minutes
    
    class Config:
        env_file = ".env"
        extra = "ignore"  # Ignore extra environment variables
        case_sensitive = False  # Make environment variable matching case-insensitive

settings = Settings()
