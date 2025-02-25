import redis
import json
from datetime import datetime

redis_client = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True
)

def publish_status_update(university_id: str, status: str, data: dict = None):
    """Publish university status update to Redis"""
    try:
        message = {
            'university_id': university_id,
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            **(data or {})
        }
        redis_client.publish('university_updates', json.dumps(message))
    except Exception as e:
        print(f"Error publishing status update: {str(e)}")