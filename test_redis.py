#!/usr/bin/env python3
"""
Redis Connection Test Script
This script tests Redis connectivity to diagnose connection issues.
"""

import asyncio
import sys
import os

async def test_redis_connection():
    """Test Redis connection with detailed error reporting"""
    print("🔍 Testing Redis Connection...")
    print("=" * 40)
    
    # Test 1: Check if redis package is available
    try:
        import redis.asyncio as redis
        print("✅ Redis Python package available")
    except ImportError as e:
        print(f"❌ Redis Python package not available: {e}")
        print("Install with: pip install redis==5.0.1")
        return False
    
    # Test 2: Try to connect to Redis
    try:
        print("🔌 Attempting to connect to Redis...")
        r = redis.from_url("redis://localhost:6379", decode_responses=True)
        
        # Test connection
        await r.ping()
        print("✅ Redis connection successful!")
        
        # Test basic operations
        await r.set("test_key", "test_value", ex=10)
        value = await r.get("test_key")
        print(f"✅ Redis read/write test successful: {value}")
        
        # Cleanup
        await r.delete("test_key")
        await r.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        
        # Provide specific troubleshooting steps
        if "Error 22" in str(e):
            print("\n🔧 Error 22 = 'Invalid argument'")
            print("This usually means Redis server is not running.")
            print("\nTroubleshooting steps:")
            print("1. Check if Redis is installed and running")
            print("2. Start Redis service:")
            print("   - WSL2: sudo service redis-server start")
            print("   - Docker: docker run -d -p 6379:6379 redis:7-alpine")
            print("   - Windows: redis-server")
            
        elif "Connection refused" in str(e):
            print("\n🔧 Connection refused")
            print("Redis server is not running or not accessible on port 6379")
            
        elif "timeout" in str(e).lower():
            print("\n🔧 Connection timeout")
            print("Redis server might be running but slow to respond")
            
        return False

def check_redis_process():
    """Check if Redis process is running"""
    print("\n🔍 Checking Redis Process Status...")
    print("=" * 40)
    
    import subprocess
    
    # Check common Redis process names
    redis_processes = ['redis-server', 'redis', 'redis-server.exe']
    
    for process in redis_processes:
        try:
            result = subprocess.run(['tasklist', '/FI', f'IMAGENAME eq {process}'], 
                                  capture_output=True, text=True, shell=True)
            if process in result.stdout:
                print(f"✅ Redis process found: {process}")
                return True
        except Exception:
            pass
    
    print("❌ No Redis process found running")
    return False

def check_port_6379():
    """Check if port 6379 is in use"""
    print("\n🔍 Checking Port 6379...")
    print("=" * 40)
    
    import subprocess
    
    try:
        result = subprocess.run(['netstat', '-an'], capture_output=True, text=True, shell=True)
        if ':6379' in result.stdout:
            print("✅ Port 6379 is in use (Redis might be running)")
            return True
        else:
            print("❌ Port 6379 is not in use (Redis not running)")
            return False
    except Exception as e:
        print(f"❌ Could not check port: {e}")
        return False

async def main():
    """Main test function"""
    print("🚀 Redis Connection Diagnostic Tool")
    print("=" * 50)
    
    # Check system info
    print(f"🔧 Python version: {sys.version}")
    print(f"🔧 Platform: {sys.platform}")
    print(f"🔧 Working directory: {os.getcwd()}")
    
    # Check Redis process
    redis_running = check_redis_process()
    
    # Check port
    port_in_use = check_port_6379()
    
    # Test connection
    connection_success = await test_redis_connection()
    
    # Summary
    print("\n📊 DIAGNOSTIC SUMMARY")
    print("=" * 30)
    print(f"Redis Process Running: {'✅ Yes' if redis_running else '❌ No'}")
    print(f"Port 6379 In Use: {'✅ Yes' if port_in_use else '❌ No'}")
    print(f"Connection Test: {'✅ Success' if connection_success else '❌ Failed'}")
    
    if not connection_success:
        print("\n🚨 REDIS IS NOT WORKING")
        print("Your application will run without caching (slower performance)")
        print("\nTo fix this:")
        print("1. Install Redis server")
        print("2. Start Redis service")
        print("3. Ensure port 6379 is accessible")
    else:
        print("\n🎉 REDIS IS WORKING PERFECTLY!")
        print("Your application will use Redis caching for optimal performance")

if __name__ == "__main__":
    asyncio.run(main())
