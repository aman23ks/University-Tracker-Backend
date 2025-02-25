import json
from datetime import datetime
import os
from services.redis_fix import get_redis_connection

# Initialize Redis client
try:
    redis_client = get_redis_connection()
    print("Redis connection established")
except Exception as e:
    print(f"Warning: Could not connect to Redis: {e}")
    # Create a dummy client for fallback
    class DummyRedis:
        def publish(self, *args, **kwargs): 
            print(f"Would publish to Redis: {args}, {kwargs}")
        def set(self, *args, **kwargs): 
            print(f"Would set in Redis: {args}, {kwargs}")
        def hset(self, *args, **kwargs): 
            print(f"Would hset in Redis: {args}, {kwargs}")
        def expire(self, *args, **kwargs): pass
        def delete(self, *args, **kwargs): pass
    redis_client = DummyRedis()

def publish_status_update(university_id: str, status: str, data: dict = None):
    """Publish university status update to Redis"""
    try:
        message = {
            'university_id': university_id,
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            **(data or {})
        }
        
        # Publish to channel for websocket broadcasts
        redis_client.publish('university_updates', json.dumps(message))
        
        # Also store the latest status in a key for polling fallback
        redis_client.set(
            f"latest_status:{university_id}", 
            json.dumps(message),
            ex=3600  # Expire after 1 hour
        )
        
        # Update university_status hash in Redis
        status_data = {
            'status': status,
            'timestamp': datetime.utcnow().isoformat()
        }
        if data:
            for key, value in data.items():
                status_data[key] = str(value)
                
        redis_client.hset(f"university_status:{university_id}", mapping=status_data)
        redis_client.expire(f"university_status:{university_id}", 86400)  # 1 day TTL
        
        print(f"Published status update: {university_id} -> {status}")
        
    except Exception as e:
        print(f"Error publishing status update: {str(e)}")