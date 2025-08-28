#!/usr/bin/env python3
"""
CPU Usage Monitoring Bot for Linux Server
Monitors CPU usage and sends Telegram alerts when it exceeds 90%
Designed to run every minute via crontab
"""

import psutil
import aiohttp
import asyncio
import logging
import os
import json
from datetime import datetime, timedelta
from typing import Optional

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not installed, will use system environment variables
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

class CPUMonitor:
    def __init__(self, bot: TelegramBot, threshold: float = 90.0, cooldown_minutes: int = 10):
        self.bot = bot
        self.threshold = threshold
        self.cooldown_minutes = cooldown_minutes
        self.last_alert_file = "/tmp/cpu_monitor_last_alert.json"
        
    def get_cpu_usage(self) -> dict:
        """Get current CPU usage statistics"""
        try:
            # Get overall CPU usage (0.1 second interval for more responsive measurement)
            overall_cpu = psutil.cpu_percent(interval=0.1)
            
            # Get per-core CPU usage
            per_core_cpu = psutil.cpu_percent(interval=0.1, percpu=True)
            
            # Get CPU count
            cpu_count = psutil.cpu_count()
            logical_cpu_count = psutil.cpu_count(logical=True)
            
            # Get load average (Linux specific)
            try:
                load_avg = os.getloadavg()
            except OSError:
                load_avg = (0, 0, 0)  # Fallback for non-Unix systems
            
            # Get CPU frequency
            try:
                cpu_freq = psutil.cpu_freq()
                current_freq = cpu_freq.current if cpu_freq else 0
                max_freq = cpu_freq.max if cpu_freq else 0
            except:
                current_freq = max_freq = 0
            
            return {
                "overall_cpu": overall_cpu,
                "per_core_cpu": per_core_cpu,
                "cpu_count": cpu_count,
                "logical_cpu_count": logical_cpu_count,
                "load_avg_1min": load_avg[0],
                "load_avg_5min": load_avg[1],
                "load_avg_15min": load_avg[2],
                "current_freq_mhz": current_freq,
                "max_freq_mhz": max_freq,
                "timestamp": datetime.now()
            }
        except Exception as e:
            logging.error(f"Error getting CPU usage: {e}")
            return None
    
    def get_memory_usage(self) -> dict:
        """Get current memory usage statistics"""
        try:
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            return {
                "total_memory_gb": round(memory.total / (1024**3), 2),
                "available_memory_gb": round(memory.available / (1024**3), 2),
                "used_memory_gb": round(memory.used / (1024**3), 2),
                "memory_percent": memory.percent,
                "swap_total_gb": round(swap.total / (1024**3), 2),
                "swap_used_gb": round(swap.used / (1024**3), 2),
                "swap_percent": swap.percent
            }
        except Exception as e:
            logging.error(f"Error getting memory usage: {e}")
            return {}
    
    def get_disk_usage(self) -> dict:
        """Get current disk usage for root partition"""
        try:
            disk = psutil.disk_usage('/')
            return {
                "total_disk_gb": round(disk.total / (1024**3), 2),
                "used_disk_gb": round(disk.used / (1024**3), 2),
                "free_disk_gb": round(disk.free / (1024**3), 2),
                "disk_percent": round((disk.used / disk.total) * 100, 2)
            }
        except Exception as e:
            logging.error(f"Error getting disk usage: {e}")
            return {}
    
    def should_send_alert(self) -> bool:
        """Check if enough time has passed since last alert to avoid spam"""
        try:
            if not os.path.exists(self.last_alert_file):
                return True
            
            with open(self.last_alert_file, 'r') as f:
                data = json.load(f)
                last_alert_time = datetime.fromisoformat(data['last_alert'])
                
            # Check if cooldown period has passed
            cooldown_period = timedelta(minutes=self.cooldown_minutes)
            return datetime.now() - last_alert_time > cooldown_period
            
        except Exception as e:
            logging.error(f"Error checking last alert time: {e}")
            return True  # Send alert if we can't read the file
    
    def update_last_alert_time(self):
        """Update the last alert timestamp"""
        try:
            data = {
                'last_alert': datetime.now().isoformat(),
                'threshold': self.threshold
            }
            with open(self.last_alert_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logging.error(f"Error updating last alert time: {e}")
    
    def format_alert_message(self, cpu_stats: dict, memory_stats: dict, disk_stats: dict) -> str:
        """Format CPU usage alert message"""
        timestamp = cpu_stats['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"🚨 <b>HIGH CPU USAGE ALERT</b> 🚨\n"
        message += f"🕐 <b>Time:</b> {timestamp}\n"
        message += f"🖥️ <b>Server:</b> {os.uname().nodename}\n\n"
        
        # CPU Information
        message += f"💻 <b>CPU Usage:</b> {cpu_stats['overall_cpu']:.1f}%\n"
        message += f"⚠️ <b>Threshold:</b> {self.threshold}%\n"
        message += f"🔧 <b>CPU Cores:</b> {cpu_stats['cpu_count']} physical, {cpu_stats['logical_cpu_count']} logical\n"
        
        # Show per-core usage if any core is high
        high_cores = [i for i, usage in enumerate(cpu_stats['per_core_cpu']) if usage > 80]
        if high_cores:
            message += f"🔥 <b>High Usage Cores:</b> "
            core_info = [f"Core{i}({cpu_stats['per_core_cpu'][i]:.1f}%)" for i in high_cores[:5]]
            message += ", ".join(core_info)
            if len(high_cores) > 5:
                message += f" +{len(high_cores)-5} more"
            message += "\n"
        
        # Load Average
        message += f"📊 <b>Load Avg:</b> {cpu_stats['load_avg_1min']:.2f}, {cpu_stats['load_avg_5min']:.2f}, {cpu_stats['load_avg_15min']:.2f}\n"
        
        # CPU Frequency
        if cpu_stats['current_freq_mhz'] > 0:
            message += f"⚡ <b>CPU Freq:</b> {cpu_stats['current_freq_mhz']:.0f}MHz"
            if cpu_stats['max_freq_mhz'] > 0:
                freq_percent = (cpu_stats['current_freq_mhz'] / cpu_stats['max_freq_mhz']) * 100
                message += f" ({freq_percent:.1f}% of max)"
            message += "\n"
        
        # Memory Information
        if memory_stats:
            message += f"\n💾 <b>Memory Usage:</b> {memory_stats['memory_percent']:.1f}%\n"
            message += f"📈 <b>Memory:</b> {memory_stats['used_memory_gb']:.1f}GB / {memory_stats['total_memory_gb']:.1f}GB\n"
            
            if memory_stats['swap_percent'] > 0:
                message += f"🔄 <b>Swap:</b> {memory_stats['swap_percent']:.1f}% ({memory_stats['swap_used_gb']:.1f}GB)\n"
        
        # Disk Information
        if disk_stats:
            message += f"\n💽 <b>Disk Usage:</b> {disk_stats['disk_percent']:.1f}%\n"
            message += f"📂 <b>Disk:</b> {disk_stats['used_disk_gb']:.1f}GB / {disk_stats['total_disk_gb']:.1f}GB\n"
        
        message += f"\n🔔 <b>Next alert:</b> In {self.cooldown_minutes} minutes (if CPU still high)"
        
        return message
    
    def get_top_processes(self, limit: int = 5) -> list:
        """Get top CPU-consuming processes"""
        try:
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                try:
                    proc_info = proc.info
                    if proc_info['cpu_percent'] > 0:
                        processes.append(proc_info)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            
            # Sort by CPU usage and return top processes
            processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            return processes[:limit]
        except Exception as e:
            logging.error(f"Error getting top processes: {e}")
            return []
    
    def format_process_info(self, processes: list) -> str:
        """Format top processes information"""
        if not processes:
            return ""
        
        message = "\n🔍 <b>Top CPU Processes:</b>\n"
        for i, proc in enumerate(processes, 1):
            message += f"{i}. <code>{proc['name']}</code> (PID: {proc['pid']}) - "
            message += f"CPU: {proc['cpu_percent']:.1f}%, RAM: {proc['memory_percent']:.1f}%\n"
        
        return message
    
    async def check_and_alert(self):
        """Main method to check CPU usage and send alert if needed"""
        try:
            # Get system statistics with multiple samples for better accuracy
            cpu_samples = []
            for _ in range(3):  # Take 3 samples
                cpu_stats = self.get_cpu_usage()
                if cpu_stats:
                    cpu_samples.append(cpu_stats['overall_cpu'])
                await asyncio.sleep(0.1)  # Small delay between samples
            
            if not cpu_samples:
                logging.error("Failed to get CPU statistics")
                return
            
            # Use the highest CPU reading from the samples
            max_cpu = max(cpu_samples)
            avg_cpu = sum(cpu_samples) / len(cpu_samples)
            
            logging.info(f"CPU samples: {cpu_samples}, Max: {max_cpu:.1f}%, Avg: {avg_cpu:.1f}%")
            
            # Check if CPU usage exceeds threshold (use max value for more sensitive detection)
            if max_cpu >= self.threshold:
                # Check cooldown period
                if not self.should_send_alert():
                    logging.info(f"CPU usage high ({max_cpu:.1f}%) but in cooldown period")
                    return
                
                logging.warning(f"CPU usage ({max_cpu:.1f}%) exceeds threshold ({self.threshold}%)")
                
                # Get additional system info
                memory_stats = self.get_memory_usage()
                disk_stats = self.get_disk_usage()
                top_processes = self.get_top_processes()
                
                # Update cpu_stats with the max value for alert message
                cpu_stats['overall_cpu'] = max_cpu
                cpu_stats['timestamp'] = datetime.now()
                
                # Format and send alert message
                alert_message = self.format_alert_message(cpu_stats, memory_stats, disk_stats)
                
                # Add top processes info
                process_info = self.format_process_info(top_processes)
                if process_info:
                    alert_message += process_info
                
                # Send alert
                result = await self.bot.send_message(alert_message)
                if result:
                    logging.info("High CPU usage alert sent successfully")
                    self.update_last_alert_time()
                else:
                    logging.error("Failed to send high CPU usage alert")
            else:
                logging.info(f"CPU usage normal: Max {max_cpu:.1f}%, Avg {avg_cpu:.1f}%")
                
        except Exception as e:
            logging.error(f"Error in CPU monitoring: {e}")
            # Send error notification
            error_msg = f"❌ <b>CPU Monitor Error</b>\n"
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
    CPU_THRESHOLD = float(os.getenv("CPU_THRESHOLD", "90.0"))
    COOLDOWN_MINUTES = int(os.getenv("CPU_ALERT_COOLDOWN", "10"))
    
    if not BOT_TOKEN or not CHAT_ID:
        print("Error: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables are required")
        print("\nSetup Instructions:")
        print("1. Create a bot with @BotFather on Telegram")
        print("2. Get your chat ID by messaging @userinfobot")
        print("3. Set environment variables:")
        print("   export TELEGRAM_BOT_TOKEN='your_bot_token'")
        print("   export TELEGRAM_CHAT_ID='your_chat_id'")
        print("   export CPU_THRESHOLD='90.0'  # optional, default 90%")
        print("   export CPU_ALERT_COOLDOWN='10'  # optional, default 10 minutes")
        print("\n4. Add to crontab:")
        print("   * * * * * /usr/bin/python3 /path/to/cpu-usage-bot.py")
        return
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/tmp/cpu-usage-bot.log'),
            logging.StreamHandler()
        ]
    )
    
    # Initialize bot and monitor
    bot = TelegramBot(BOT_TOKEN, CHAT_ID)
    monitor = CPUMonitor(bot, threshold=CPU_THRESHOLD, cooldown_minutes=COOLDOWN_MINUTES)
    
    # Run CPU check
    await monitor.check_and_alert()

if __name__ == "__main__":
    asyncio.run(main())
