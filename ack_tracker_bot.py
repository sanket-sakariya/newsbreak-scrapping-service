#!/usr/bin/env python3
"""
ACK Tracker Bot for NewsBreak Scraper
Creates ack table and stores ACK data every 5 minutes from RabbitMQ
Calculates URL processing rates between time periods
"""

import asyncio
import aiohttp
import logging
import asyncpg
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import time

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class ACKTrackerBot:
    def __init__(self, db_url_data: str, rabbitmq_management_url: str):
        self.db_url_data = db_url_data
        self.rabbitmq_management_url = rabbitmq_management_url.rstrip('/')
        self.is_running = False
        self.tracking_interval = 300  # 5 minutes = 300 seconds
        
    async def create_ack_table(self) -> bool:
        """Create the ack table in newsbreak_data database"""
        try:
            conn = await asyncpg.connect(self.db_url_data)
            try:
                # Create the ack table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ack (
                        id SERIAL PRIMARY KEY,
                        queue_name VARCHAR(100) NOT NULL,
                        ack_total BIGINT NOT NULL,
                        ack_rate_per_sec DECIMAL(10,4),
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create indexes for better performance
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_ack_timestamp ON ack(timestamp)
                """)
                
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_ack_queue_name ON ack(queue_name)
                """)
                
                # Insert initial record if table is empty
                await conn.execute("""
                    INSERT INTO ack (queue_name, ack_total, ack_rate_per_sec)
                    SELECT 'scraper_queue', 0, 0.0
                    WHERE NOT EXISTS (SELECT 1 FROM ack)
                """)
                
                logging.info("ACK table created successfully in newsbreak_data database")
                return True
                
            finally:
                await conn.close()
        except Exception as e:
            logging.error(f"Error creating ACK table: {e}")
            return False
    
    async def fetch_rabbitmq_ack_stats(self) -> Optional[Dict[str, Any]]:
        """Fetch ACK statistics from RabbitMQ Management API"""
        url = f"{self.rabbitmq_management_url}/api/queues/%2F/scraper_queue"
        
        # Get credentials from environment
        username = os.getenv("RABBITMQ_USERNAME", "guest")
        password = os.getenv("RABBITMQ_PASSWORD", "guest")
        
        async with aiohttp.ClientSession() as session:
            try:
                # Basic auth for RabbitMQ Management API
                auth = aiohttp.BasicAuth(username, password)
                
                async with session.get(url, auth=auth, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Extract ACK statistics
                        message_stats = data.get('message_stats', {})
                        ack_total = message_stats.get('ack', 0)
                        ack_details = message_stats.get('ack_details', {})
                        ack_rate_per_sec = ack_details.get('rate', 0.0)
                        
                        return {
                            'queue_name': 'scraper_queue',
                            'ack_total': ack_total,
                            'ack_rate_per_sec': ack_rate_per_sec,
                            'timestamp': datetime.now()
                        }
                    else:
                        logging.error(f"RabbitMQ Management API error: {response.status}")
                        return None
            except Exception as e:
                logging.error(f"Error fetching RabbitMQ ACK stats: {e}")
                return None
    
    async def store_ack_data(self, ack_data: Dict[str, Any]) -> bool:
        """Store ACK data in the ack table"""
        try:
            conn = await asyncpg.connect(self.db_url_data)
            try:
                await conn.execute(
                    """
                    INSERT INTO ack (queue_name, ack_total, ack_rate_per_sec, timestamp)
                    VALUES ($1, $2, $3, $4)
                    """,
                    ack_data['queue_name'],
                    ack_data['ack_total'],
                    ack_data['ack_rate_per_sec'],
                    ack_data['timestamp']
                )
                logging.info(f"Stored ACK data: {ack_data['ack_total']} acks, {ack_data['ack_rate_per_sec']:.2f} acks/sec")
                return True
            finally:
                await conn.close()
        except Exception as e:
            logging.error(f"Error storing ACK data: {e}")
            return False
    
    async def get_ack_difference(self, start_time: datetime, end_time: datetime) -> Optional[Dict[str, Any]]:
        """Get ACK difference between two specific timestamps"""
        try:
            conn = await asyncpg.connect(self.db_url_data)
            try:
                # Get ACK stats for start time (closest available)
                start_stats = await conn.fetchrow(
                    """
                    SELECT ack_total, timestamp
                    FROM ack 
                    WHERE timestamp >= $1 
                    ORDER BY timestamp ASC 
                    LIMIT 1
                    """,
                    start_time
                )
                
                # Get ACK stats for end time (closest available)
                end_stats = await conn.fetchrow(
                    """
                    SELECT ack_total, timestamp
                    FROM ack 
                    WHERE timestamp <= $1 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                    """,
                    end_time
                )
                
                if start_stats and end_stats:
                    ack_difference = end_stats['ack_total'] - start_stats['ack_total']
                    time_difference = end_stats['timestamp'] - start_stats['timestamp']
                    
                    return {
                        'queue_name': 'scraper_queue',
                        'start_ack_total': start_stats['ack_total'],
                        'end_ack_total': end_stats['ack_total'],
                        'ack_difference': ack_difference,
                        'time_difference': time_difference,
                        'start_timestamp': start_stats['timestamp'],
                        'end_timestamp': end_stats['timestamp'],
                        'urls_per_hour': (ack_difference / max(time_difference.total_seconds() / 3600, 1)),
                        'urls_per_minute': (ack_difference / max(time_difference.total_seconds() / 60, 1))
                    }
                return None
            finally:
                await conn.close()
        except Exception as e:
            logging.error(f"Error getting ACK difference: {e}")
            return None
    
    async def get_processing_rate_summary(self, hours_back: int = 6) -> Dict[str, Any]:
        """Get processing rate summary for the last N hours"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours_back)
            
            ack_diff = await self.get_ack_difference(start_time, end_time)
            
            if ack_diff:
                return {
                    'period_hours': hours_back,
                    'start_time': start_time,
                    'end_time': end_time,
                    'urls_processed': ack_diff['ack_difference'],
                    'urls_per_hour': ack_diff['urls_per_hour'],
                    'urls_per_minute': ack_diff['urls_per_minute'],
                    'time_span': ack_diff['time_difference']
                }
            else:
                return {
                    'period_hours': hours_back,
                    'start_time': start_time,
                    'end_time': end_time,
                    'urls_processed': 0,
                    'urls_per_hour': 0,
                    'urls_per_minute': 0,
                    'time_span': timedelta(0)
                }
        except Exception as e:
            logging.error(f"Error getting processing rate summary: {e}")
            return {}
    
    async def start_tracking(self):
        """Start the ACK tracking loop"""
        self.is_running = True
        logging.info(f"Starting ACK tracking every {self.tracking_interval} seconds")
        
        # Create table first
        if not await self.create_ack_table():
            logging.error("Failed to create ACK table. Exiting.")
            return
        
        while self.is_running:
            try:
                # Fetch and store ACK data
                ack_data = await self.fetch_rabbitmq_ack_stats()
                if ack_data:
                    await self.store_ack_data(ack_data)
                else:
                    logging.warning("Failed to fetch ACK stats from RabbitMQ")
                
                # Wait for next tracking interval
                await asyncio.sleep(self.tracking_interval)
                
            except asyncio.CancelledError:
                logging.info("ACK tracking cancelled")
                break
            except Exception as e:
                logging.error(f"Error in ACK tracking loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    async def stop_tracking(self):
        """Stop the ACK tracking loop"""
        self.is_running = False
        logging.info("ACK tracking stopped")
    
    async def run_single_track(self):
        """Run a single ACK tracking cycle (for cron jobs)"""
        try:
            logging.info("Running single ACK tracking cycle...")
            
            # Create table if it doesn't exist
            if not await self.create_ack_table():
                logging.error("Failed to create ACK table")
                return False
            
            # Fetch and store ACK data
            ack_data = await self.fetch_rabbitmq_ack_stats()
            if ack_data:
                success = await self.store_ack_data(ack_data)
                if success:
                    logging.info("Single ACK tracking cycle completed successfully")
                    return True
                else:
                    logging.error("Failed to store ACK data")
                    return False
            else:
                logging.warning("Failed to fetch ACK stats from RabbitMQ")
                return False
                
        except Exception as e:
            logging.error(f"Error in single ACK tracking cycle: {e}")
            return False

async def main():
    # Configuration from environment variables
    DB_URL_DATA = os.getenv("DATABASE_URL_1", "postgresql://sanket:root@localhost:5432/newsbreak_scraper_data")
    RABBITMQ_MANAGEMENT_URL = os.getenv("RABBITMQ_MANAGEMENT_URL", "http://localhost:15672")
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ack_tracker_bot.log'),
            logging.StreamHandler()
        ]
    )
    
    # Check command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        # Initialize bot
        tracker = ACKTrackerBot(DB_URL_DATA, RABBITMQ_MANAGEMENT_URL)
        
        if command == "single":
            # Run single tracking cycle (for cron jobs)
            success = await tracker.run_single_track()
            if success:
                print("✅ ACK tracking completed successfully")
                sys.exit(0)
            else:
                print("❌ ACK tracking failed")
                sys.exit(1)
        elif command == "continuous":
            # Run continuous tracking
            try:
                await tracker.start_tracking()
            except KeyboardInterrupt:
                await tracker.stop_tracking()
        elif command == "summary":
            # Show processing rate summary
            summary = await tracker.get_processing_rate_summary(6)
            if summary:
                print("📊 ACK Processing Rate Summary (Last 6 Hours)")
                print("=" * 50)
                print(f"⏱️  Time Period: {summary['period_hours']} hours")
                print(f"🕐 From: {summary['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"🕐 To: {summary['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"🔄 URLs Processed: {summary['urls_processed']:,}")
                print(f"📈 URLs per Hour: {summary['urls_per_hour']:.1f}")
                print(f"⚡ URLs per Minute: {summary['urls_per_minute']:.2f}")
            else:
                print("❌ No ACK data available for summary")
        else:
            print("Usage:")
            print("  python ack_tracker_bot.py [command]")
            print("\nCommands:")
            print("  single     - Run single tracking cycle (for cron)")
            print("  continuous - Run continuous tracking loop")
            print("  summary    - Show processing rate summary")
            print("\nExamples:")
            print("  python ack_tracker_bot.py single")
            print("  python ack_tracker_bot.py continuous")
            print("  python ack_tracker_bot.py summary")
    else:
        # Default: run single tracking cycle
        tracker = ACKTrackerBot(DB_URL_DATA, RABBITMQ_MANAGEMENT_URL)
        success = await tracker.run_single_track()
        if success:
            print("✅ ACK tracking completed successfully")
        else:
            print("❌ ACK tracking failed")

if __name__ == "__main__":
    asyncio.run(main())
