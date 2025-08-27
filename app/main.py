from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
from app.core.database import db_manager
from app.workers import data_worker, url_worker, url_feeder
from app.workers.dlx_retry_worker import DLXRetryWorker
from app.core.config import settings
from app.api.v1.router import api_router
from app.core.logging import logger

# Global worker instances
dlx_retry_worker = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global dlx_retry_worker
    
    # Startup
    logger.info("Starting NewsBreak Scraper API...")
    
    # Initialize database and RabbitMQ connections
    await db_manager.initialize_connections()
    await db_manager.setup_queues()
    
    # Setup database tables
    await setup_database()
    
    # Start background workers
    asyncio.create_task(data_worker.start())
    asyncio.create_task(url_worker.start())
    asyncio.create_task(url_feeder.start())
    
    # Start DLX retry worker
    dlx_retry_worker = DLXRetryWorker(retry_delay=300)  # 5 minutes delay
    asyncio.create_task(dlx_retry_worker.start())
    
    logger.info("NewsBreak Scraper API started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down NewsBreak Scraper API...")
    
    # Stop all workers
    await data_worker.stop()
    await url_worker.stop()
    await url_feeder.stop()
    if dlx_retry_worker:
        await dlx_retry_worker.stop()
    
    # Cleanup connections
    await db_manager.cleanup_connections()
    
    logger.info("NewsBreak Scraper API shutdown complete")

async def setup_database():
    """Setup database tables"""
    try:
        data_pool = db_manager.get_data_pool()
        url_pool = db_manager.get_url_pool()
        
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
                CREATE TABLE IF NOT EXISTS domain_data (
                    domain_id SERIAL PRIMARY KEY,
                    domain_name VARCHAR(255) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                    domain_id INTEGER REFERENCES domain_data(domain_id),
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
                    date TIMESTAMP,
                    wordcount INTEGER,
                    images TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    status VARCHAR(50) DEFAULT 'creating',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add missing columns if they don't exist (for existing tables)
            try:
                await conn.execute("ALTER TABLE newsbreak_data ADD COLUMN IF NOT EXISTS date TIMESTAMP")
                await conn.execute("ALTER TABLE newsbreak_data ADD COLUMN IF NOT EXISTS wordcount INTEGER")
                await conn.execute("ALTER TABLE newsbreak_data ADD COLUMN IF NOT EXISTS images TEXT")
                logger.info("Added missing columns to newsbreak_data table")
            except Exception as e:
                logger.info(f"Columns may already exist or error adding them: {e}")
            
            # Create indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_newsbreak_data_created_at ON newsbreak_data(created_at)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_newsbreak_data_status ON newsbreak_data(status)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_newsbreak_data_domain_id ON newsbreak_data(domain_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_newsbreak_data_date ON newsbreak_data(date)")

        async with url_pool.acquire() as conn:
            # Create URLs table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    url TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # No additional columns
            
            
            # No additional indexes
            
        logger.info("Database tables setup successfully")
        
    except Exception as e:
        logger.error(f"Failed to setup database: {e}")
        raise

# Create FastAPI app instance
app = FastAPI(
    title=settings.app_name,
    description="API for scraping NewsBreak.com with cyclic workflow using RabbitMQ queues and PostgreSQL storage",
    version=settings.app_version,
    lifespan=lifespan
)

# Include API router
app.include_router(api_router, prefix="/api/v1")

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "NewsBreak Scraper API",
        "version": settings.app_version,
        "docs": "/docs",
        "health": "/api/v1/health"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
