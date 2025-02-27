# In services/event_handler.py

import json
from datetime import datetime
import os
import time
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
    """Publish university status update to Redis with rate limiting and cell-level granularity"""
    try:
        # Create the message
        message = {
            'university_id': university_id,
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            **(data or {})
        }
        
        # Special handling for column processing events
        if status == 'column_processed':
            # For cell updates, don't rate limit to ensure each update reaches the client
            # This is important for updating individual cells without refreshing
            redis_client.publish('university_updates', json.dumps(message))
            return
        
        # For other status updates, use rate limiting
        current_time = time.time()
        last_update_key = f"last_update:{university_id}"
        
        # Get last update time
        last_update_time = redis_client.get(last_update_key)
        if last_update_time:
            last_update_time = float(last_update_time)
            # If updated less than 1 second ago, skip unless important status
            if (current_time - last_update_time < 1.0 and 
                status not in ['completed', 'failed', 'subscription_reactivated']):
                return
        
        # Update last update time
        redis_client.set(last_update_key, str(current_time), ex=60)
        
        # Publish to channel for websocket broadcasts
        redis_client.publish('university_updates', json.dumps(message))
        
        # Also store the latest status in a key for polling fallback
        redis_client.set(
            f"latest_status:{university_id}", 
            json.dumps(message),
            ex=3600  # Expire after 1 hour
        )
        
        # Only update Redis hash for important statuses
        if status in ['completed', 'failed', 'processing', 'subscription_reactivated']:
            # Update university_status hash in Redis
            status_data = {
                'status': status,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            if data:
                for key, value in data.items():
                    if isinstance(value, (dict, list)):
                        status_data[key] = json.dumps(value)
                    else:
                        status_data[key] = str(value)
                    
            redis_client.hset(f"university_status:{university_id}", mapping=status_data)
            redis_client.expire(f"university_status:{university_id}", 86400)  # 1 day TTL
        
    except Exception as e:
        print(f"Error publishing status update: {str(e)}")
        
def publish_cell_update(university_id: str, column_id: str, value: str, user_email: str = None):
    """
    Publish a single cell update for real-time UI updates.
    This is optimized for individual cell updates without refreshing the entire table.
    """
    try:
        message = {
            'university_id': university_id,
            'status': 'column_processed',
            'column_id': column_id,
            'value': value,
            'user_email': user_email,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Publish directly to university_updates channel
        redis_client.publish('university_updates', json.dumps(message))
        print(f"Published cell update: {university_id} -> {column_id}")
        
    except Exception as e:
        print(f"Error publishing cell update: {str(e)}")
        
def publish_user_update(user_email: str, update_type: str, data: dict = None):
    """Publish user-specific update to Redis"""
    try:
        message = {
            'user_email': user_email,
            'type': update_type,
            'timestamp': datetime.utcnow().isoformat(),
            **(data or {})
        }
        
        # Publish to user updates channel
        redis_client.publish('user_updates', json.dumps(message))
        print(f"Published user update: {user_email} -> {update_type}")
        
    except Exception as e:
        print(f"Error publishing user update: {str(e)}")