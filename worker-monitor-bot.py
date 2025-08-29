#!/usr/bin/env python3
"""
Worker Process Monitoring Bot
Monitors scraper_worker, data_worker, and url_worker processes by checking app.log
Sends Telegram alerts if workers are not running or have errors
"""

import aiohttp
import asyncio
import logging
import os
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logging.error(f"Telegram API error: {response.status}")
                        return None
            except Exception as e:
                logging.error(f"Error sending Telegram message: {e}")
                return None

class WorkerMonitor:
    def __init__(self, bot: TelegramBot, log_file: str = "nohup.out", check_interval_minutes: int = 5):
        self.bot = bot
        self.log_file = log_file
        self.check_interval_minutes = check_interval_minutes
        self.last_alert_file = "/tmp/worker_monitor_last_alert.json"
        self.worker_patterns = {
            'scraper_worker': [
                r'scraper_worker.*started',
                r'Worker \d+ processed \d+ URLs',
                r'Scraper worker.*started',
                r'Worker.*scraper.*started'
            ],
            'data_worker': [
                r'data_worker.*started',
                r'Data worker processed \d+ records',
                r'Data worker.*started',
                r'Worker.*data.*started'
            ],
            'url_worker': [
                r'url_worker.*started',
                r'URL worker processed \d+ URLs',
                r'URL worker.*started',
                r'Worker.*url.*started'
            ]
        }
        self.error_patterns = [
            r'ERROR.*worker',
            r'Worker.*error',
            r'worker.*ERROR',
            r'Worker.*stopped',
            r'worker.*stopped',
            r'Worker.*failed',
            r'worker.*failed'
        ]
        
    def get_log_lines(self, minutes_back: int = 10) -> List[str]:
        """Get log lines from the last N minutes"""
        try:
            if not os.path.exists(self.log_file):
                logging.error(f"Log file not found: {self.log_file}")
                return []
            
            # Get file modification time
            file_mtime = os.path.getmtime(self.log_file)
            cutoff_time = datetime.now().timestamp() - (minutes_back * 60)
            
            lines = []
            with open(self.log_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    # Try to extract timestamp from log line
                    timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                    if timestamp_match:
                        try:
                            log_time = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
                            if log_time.timestamp() > cutoff_time:
                                lines.append(line.strip())
                        except ValueError:
                            # If we can't parse timestamp, include the line if file is recent
                            if file_mtime > cutoff_time:
                                lines.append(line.strip())
                    else:
                        # If no timestamp, include if file is recent
                        if file_mtime > cutoff_time:
                            lines.append(line.strip())
            
            return lines
        except Exception as e:
            logging.error(f"Error reading log file: {e}")
            return []
    
    def check_worker_activity(self, log_lines: List[str]) -> Dict[str, Dict]:
        """Check worker activity in log lines"""
        worker_status = {
            'scraper_worker': {'active': False, 'last_seen': None, 'errors': []},
            'data_worker': {'active': False, 'last_seen': None, 'errors': []},
            'url_worker': {'active': False, 'last_seen': None, 'errors': []}
        }
        
        for line in log_lines:
            line_lower = line.lower()
            
            # Check for worker activity
            for worker_name, patterns in self.worker_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, line_lower, re.IGNORECASE):
                        worker_status[worker_name]['active'] = True
                        # Extract timestamp
                        timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                        if timestamp_match:
                            worker_status[worker_name]['last_seen'] = timestamp_match.group(1)
                        break
            
            # Check for errors
            for pattern in self.error_patterns:
                if re.search(pattern, line_lower, re.IGNORECASE):
                    # Determine which worker the error belongs to
                    for worker_name in worker_status.keys():
                        if worker_name.replace('_', '') in line_lower:
                            worker_status[worker_name]['errors'].append(line.strip())
                            break
        
        return worker_status
    
    def should_send_alert(self) -> bool:
        """Check if enough time has passed since last alert to avoid spam"""
        try:
            if not os.path.exists(self.last_alert_file):
                return True
            
            with open(self.last_alert_file, 'r') as f:
                data = json.load(f)
                last_alert_time = datetime.fromisoformat(data['last_alert'])
                
            # Check if cooldown period has passed
            cooldown_period = timedelta(minutes=self.check_interval_minutes)
            return datetime.now() - last_alert_time > cooldown_period
            
        except Exception as e:
            logging.error(f"Error checking last alert time: {e}")
            return True
    
    def update_last_alert_time(self):
        """Update the last alert timestamp"""
        try:
            data = {
                'last_alert': datetime.now().isoformat(),
                'check_interval': self.check_interval_minutes
            }
            with open(self.last_alert_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logging.error(f"Error updating last alert time: {e}")
    
    def format_alert_message(self, worker_status: Dict, log_file_exists: bool) -> str:
        """Format worker status alert message"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"🔍 <b>WORKER STATUS ALERT</b> 🔍\n"
        message += f"🕐 <b>Time:</b> {timestamp}\n"
        message += f"🖥️ <b>Server:</b> {os.uname().nodename}\n\n"
        
        if not log_file_exists:
            message += f"❌ <b>Log File Missing:</b> {self.log_file}\n"
            message += f"⚠️ Cannot monitor workers without log file\n"
            return message
        
        # Check each worker
        inactive_workers = []
        workers_with_errors = []
        
        for worker_name, status in worker_status.items():
            if not status['active']:
                inactive_workers.append(worker_name)
            if status['errors']:
                workers_with_errors.append(worker_name)
        
        if not inactive_workers and not workers_with_errors:
            message += f"✅ <b>All workers are running normally</b>\n\n"
        else:
            if inactive_workers:
                message += f"❌ <b>Inactive Workers:</b>\n"
                for worker in inactive_workers:
                    last_seen = worker_status[worker]['last_seen'] or "Never"
                    message += f"   • <code>{worker}</code> (Last seen: {last_seen})\n"
                message += "\n"
            
            if workers_with_errors:
                message += f"⚠️ <b>Workers with Errors:</b>\n"
                for worker in workers_with_errors:
                    error_count = len(worker_status[worker]['errors'])
                    message += f"   • <code>{worker}</code> ({error_count} errors)\n"
                message += "\n"
        
        # Show all worker statuses
        message += f"📊 <b>Worker Status Summary:</b>\n"
        for worker_name, status in worker_status.items():
            status_emoji = "✅" if status['active'] else "❌"
            last_seen = status['last_seen'] or "Never"
            error_count = len(status['errors'])
            
            message += f"{status_emoji} <code>{worker_name}</code>\n"
            message += f"   Last seen: {last_seen}\n"
            if error_count > 0:
                message += f"   Errors: {error_count}\n"
            message += "\n"
        
        message += f"🔔 <b>Next check:</b> In {self.check_interval_minutes} minutes"
        
        return message
    
    def format_error_details(self, worker_status: Dict) -> str:
        """Format detailed error information"""
        message = "\n🔍 <b>Recent Errors:</b>\n"
        
        for worker_name, status in worker_status.items():
            if status['errors']:
                message += f"\n📝 <b>{worker_name}:</b>\n"
                for error in status['errors'][-3:]:  # Show last 3 errors
                    # Truncate long error messages
                    error_text = error[:100] + "..." if len(error) > 100 else error
                    message += f"   • <code>{error_text}</code>\n"
        
        return message
    
    async def check_and_alert(self):
        """Main method to check worker status and send alert if needed"""
        try:
            # Check if log file exists
            log_file_exists = os.path.exists(self.log_file)
            
            if not log_file_exists:
                logging.error(f"Log file not found: {self.log_file}")
                # Send alert about missing log file
                if self.should_send_alert():
                    alert_message = self.format_alert_message({}, False)
                    result = await self.bot.send_message(alert_message)
                    if result:
                        logging.info("Log file missing alert sent successfully")
                        self.update_last_alert_time()
                return
            
            # Get recent log lines
            log_lines = self.get_log_lines(minutes_back=10)
            logging.info(f"Analyzed {len(log_lines)} log lines from last 10 minutes")
            
            # Check worker activity
            worker_status = self.check_worker_activity(log_lines)
            
            # Check if any workers are inactive or have errors
            inactive_workers = [name for name, status in worker_status.items() if not status['active']]
            workers_with_errors = [name for name, status in worker_status.items() if status['errors']]
            
            # Determine if we need to send an alert
            needs_alert = bool(inactive_workers or workers_with_errors)
            
            if needs_alert:
                # Check cooldown period
                if not self.should_send_alert():
                    logging.info(f"Workers need attention but in cooldown period")
                    return
                
                logging.warning(f"Worker issues detected: Inactive: {inactive_workers}, Errors: {workers_with_errors}")
                
                # Format and send alert message
                alert_message = self.format_alert_message(worker_status, True)
                
                # Add error details if there are errors
                if workers_with_errors:
                    error_details = self.format_error_details(worker_status)
                    alert_message += error_details
                
                # Send alert
                result = await self.bot.send_message(alert_message)
                if result:
                    logging.info("Worker status alert sent successfully")
                    self.update_last_alert_time()
                else:
                    logging.error("Failed to send worker status alert")
            else:
                logging.info("All workers are running normally")
                
        except Exception as e:
            logging.error(f"Error in worker monitoring: {e}")
            # Send error notification
            error_msg = f"❌ <b>Worker Monitor Error</b>\n"
            error_msg += f"🕐 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            error_msg += f"🖥️ <b>Server:</b> {os.uname().nodename}\n"
            error_msg += f"⚠️ <b>Error:</b> <code>{str(e)}</code>"
            
            try:
                await self.bot.send_message(error_msg)
            except:
                pass

async def main():
    # Configuration from environment variables
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    LOG_FILE = os.getenv("WORKER_LOG_FILE", "~/newsbreak-scrapping-service/nohup.out")
    CHECK_INTERVAL = int(os.getenv("WORKER_CHECK_INTERVAL", "5"))
    
    if not BOT_TOKEN or not CHAT_ID:
        print("Error: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables are required")
        print("\nSetup Instructions:")
        print("1. Create a bot with @BotFather on Telegram")
        print("2. Get your chat ID by messaging @userinfobot")
        print("3. Set environment variables:")
        print("   export TELEGRAM_BOT_TOKEN='your_bot_token'")
        print("   export TELEGRAM_CHAT_ID='your_chat_id'")
        print("   export WORKER_LOG_FILE='~/newsbreak-scrapping-service/app.log'  # optional, default")
        print("   export WORKER_CHECK_INTERVAL='5'  # optional, default 5 minutes")
        print("\n4. Add to crontab:")
        print("   * * * * * /usr/bin/python3 /path/to/worker-monitor-bot.py")
        return
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/tmp/worker-monitor-bot.log'),
            logging.StreamHandler()
        ]
    )
    
    # Initialize bot and monitor
    bot = TelegramBot(BOT_TOKEN, CHAT_ID)
    # Expand ~ to home directory
    LOG_FILE = os.path.expanduser(LOG_FILE)
    monitor = WorkerMonitor(bot, log_file=LOG_FILE, check_interval_minutes=CHECK_INTERVAL)
    
    # Run worker check
    await monitor.check_and_alert()

if __name__ == "__main__":
    asyncio.run(main())
