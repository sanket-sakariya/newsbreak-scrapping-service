#!/usr/bin/env python3
"""
Telegram Bot for NewsBreak Scraper Status Monitoring
Fetches status from NewsBreak API and sends updates to Telegram
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
import json
import os
from typing import Optional
import asyncpg

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not installed, will use system environment variables
    pass

# Simple async Telegram bot implementation
class TelegramBot:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        
    async def send_message(self, text: str, parse_mode: str = "HTML"):
        """Send message to Telegram chat"""
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logging.error(f"Telegram API error: {response.status}")
                        return None
            except Exception as e:
                logging.error(f"Error sending Telegram message: {e}")
                return None

class NewsBreakMonitor:
    def __init__(self, api_url: str, bot: TelegramBot, db_url_data: str, db_url_urls: str):
        self.api_url = api_url.rstrip('/')
        self.bot = bot
        self.db_url_data = db_url_data
        self.db_url_urls = db_url_urls
        
    async def fetch_status(self) -> Optional[dict]:
        """Fetch status from NewsBreak API"""
        url = f"{self.api_url}/api/v1/scraper/status"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('data', {})
                    else:
                        logging.error(f"API error: {response.status}")
                        return None
            except Exception as e:
                logging.error(f"Error fetching status: {e}")
                return None
    
    async def get_today_stats_from_db(self) -> dict:
        """Get today's statistics directly from database using created_at field"""
        today = datetime.now().date()
        today_start = datetime.combine(today, datetime.min.time())
        today_end = datetime.combine(today, datetime.max.time())
        
        stats = {
            'urls_collected_today': 0,
            'data_extracted_today': 0
        }
        
        try:
            # Get today's URLs from urls database
            conn_urls = await asyncpg.connect(self.db_url_urls)
            try:
                urls_today = await conn_urls.fetchval(
                    "SELECT COUNT(*) FROM urls WHERE created_at >= $1 AND created_at <= $2",
                    today_start, today_end
                )
                stats['urls_collected_today'] = urls_today or 0
            finally:
                await conn_urls.close()
            
            # Get today's data from newsbreak_data database
            conn_data = await asyncpg.connect(self.db_url_data)
            try:
                data_today = await conn_data.fetchval(
                    "SELECT COUNT(*) FROM newsbreak_data WHERE created_at >= $1 AND created_at <= $2",
                    today_start, today_end
                )
                stats['data_extracted_today'] = data_today or 0
            finally:
                await conn_data.close()
                
        except Exception as e:
            logging.error(f"Error fetching today's stats from database: {e}")
        
        return stats

    async def format_daily_report(self, status: dict) -> str:
        """Format daily status report with today's progress from database"""
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M:%S")
        
        if not status:
            return f"🔴 <b>Daily NewsBreak Report</b>\n📅 {today} {time_str}\n❌ <b>API Unavailable</b>"
        
        scraper_status = status.get('status', 'unknown')
        stats = status.get('statistics', {})
        queue_sizes = stats.get('queue_sizes', {})
        
        # Current totals from API
        current_urls = stats.get('urls_collected', 0)
        current_data = stats.get('data_extracted', 0)
        current_errors = stats.get('errors_count', 0)
        
        # Calculate processed URLs: urls_collected - scraper_queue_size - dlx_queue_size
        scraper_queue_size = queue_sizes.get('scraper_queue', 0)
        dlx_queue_size = queue_sizes.get('dlx_queue', 0)
        current_processed_urls = current_urls - scraper_queue_size - dlx_queue_size
        
        # Get today's stats from database
        today_stats = await self.get_today_stats_from_db()
        today_urls = today_stats.get('urls_collected_today', 0)
        today_data = today_stats.get('data_extracted_today', 0)
        
        # For today's processed URLs, we assume all today's collected URLs are processed
        # (since they would have been processed during the day)
        today_processed_urls = today_urls
        
        # Status emoji
        status_emoji = "🟢" if scraper_status == "running" else "🔴"
        
        message = f"{status_emoji} <b>Daily NewsBreak Report</b>\n"
        message += f"📅 <b>Date:</b> {today} {time_str}\n"
        message += f"🔧 <b>Status:</b> {scraper_status.upper()}\n"
        message += f"👥 <b>Active Workers:</b> {stats.get('active_workers', 0)}\n\n"
        
        message += f"📈 <b>Today's Progress:</b>\n"
        message += f"🔗 URLs Collected: {today_urls:,}\n"
        message += f"🟢 URLs Processed: {today_processed_urls:,}\n"
        message += f"📰 Data Extracted: {today_data:,}\n\n"
        
        message += f"📊 <b>Total Statistics:</b>\n"
        message += f"🔗 Total URLs: {current_urls:,}\n"
        message += f"🟢 Total Processed: {current_processed_urls:,}\n"
        message += f"📰 Total Data: {current_data:,}\n"
        message += f"❌ Total Errors: {current_errors:,}\n\n"
        
        message += f"📋 <b>Current Queue Status:</b>\n"
        message += f"🕷️ Scraper Queue: {queue_sizes.get('scraper_queue', 0):,}\n"
        message += f"🔗 URLs Queue: {queue_sizes.get('newsbreak_urls_queue', 0):,}\n"
        message += f"📰 Data Queue: {queue_sizes.get('newsbreak_data_queue', 0):,}\n"
        message += f"💀 DLX Queue: {queue_sizes.get('dlx_queue', 0):,}"
        
        return message
    
    async def send_daily_report(self):
        """Send single daily report and exit (for cron job)"""
        try:
            logging.info("Fetching status and generating daily report...")
            
            current_status = await self.fetch_status()
            
            if current_status is None:
                # API unavailable
                await self.bot.send_message("🔴 <b>Alert:</b> NewsBreak API is unavailable!")
                return
            
            # Generate and send daily report
            message = await self.format_daily_report(current_status)
            await self.bot.send_message(message)
            logging.info("Daily report sent successfully")
            
        except Exception as e:
            logging.error(f"Error sending daily report: {e}")
            error_msg = f"❌ <b>Error:</b> Failed to generate daily report\n<code>{str(e)}</code>"
            try:
                await self.bot.send_message(error_msg)
            except:
                pass
            raise

async def main():
    # Configuration from environment variables
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    API_URL = os.getenv("NEWSBREAK_API_URL", "http://localhost:8000")
    DB_URL_DATA = os.getenv("DATABASE_URL_1", "postgresql://sanket:root@localhost:5432/newsbreak_scraper_data")
    DB_URL_URLS = os.getenv("DATABASE_URL_2", "postgresql://sanket:root@localhost:5432/newsbreak_scraper_urls")
    
    if not BOT_TOKEN or not CHAT_ID:
        print("Error: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables are required")
        print("\nSetup Instructions:")
        print("1. Create a bot with @BotFather on Telegram")
        print("2. Get your chat ID by messaging @userinfobot")
        print("3. Set environment variables:")
        print("   export TELEGRAM_BOT_TOKEN='your_bot_token'")
        print("   export TELEGRAM_CHAT_ID='your_chat_id'")
        print("   export NEWSBREAK_API_URL='http://your-server:8000'  # optional")
        print("   export DATABASE_URL_1='postgresql://user:pass@host:port/newsbreak_scraper_data'  # optional")
        print("   export DATABASE_URL_2='postgresql://user:pass@host:port/newsbreak_scraper_urls'  # optional")
        return
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('newsbreak-bot.log'),
            logging.StreamHandler()
        ]
    )
    
    # Initialize bot and monitor
    bot = TelegramBot(BOT_TOKEN, CHAT_ID)
    monitor = NewsBreakMonitor(API_URL, bot, DB_URL_DATA, DB_URL_URLS)
    
    try:
        # Send daily report and exit (for cron job)
        await monitor.send_daily_report()
        logging.info("Daily report completed successfully")
        
    except Exception as e:
        logging.error(f"Error: {e}")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
