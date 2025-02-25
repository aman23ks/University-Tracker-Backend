# services/redis_fix.py
import os
import time
import redis

def get_redis_connection():
    """Create a Redis connection with proper URL handling for Railway"""
    # First, try getting the Railway Redis URL
    redis_url = os.getenv('REDIS_URL')
    
    # If that's not available, try individual host/port
    if not redis_url:
        redis_host = os.getenv('REDISHOST', os.getenv('REDIS_HOST', 'localhost'))
        redis_port = int(os.getenv('REDISPORT', os.getenv('REDIS_PORT', 6379)))
        redis_url = f"redis://{redis_host}:{redis_port}/0"
    
    max_retries = 5
    retry_delay = 3  # seconds
    
    for attempt in range(max_retries):
        try:
            conn = redis.Redis.from_url(redis_url, decode_responses=True)
            # Test the connection
            conn.ping()
            print(f"Redis connection successful on attempt {attempt+1}")
            return conn
        except (redis.ConnectionError, redis.RedisError) as e:
            if attempt < max_retries - 1:
                print(f"Redis connection failed (attempt {attempt+1}/{max_retries}): {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to Redis after {max_retries} attempts")
                print(f"Redis URL: {redis_url}")
                raise