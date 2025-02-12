import re
from typing import Dict, List
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def clean_url(url: str) -> str:
    """Clean and normalize URL"""
    url = url.strip().lower()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url

def format_timestamp(dt: datetime) -> str:
    """Format datetime to ISO 8601 string"""
    return dt.replace(tzinfo=timezone.utc).isoformat()

def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def sanitize_input(text: str) -> str:
    """Remove potentially dangerous characters from input"""
    return re.sub(r'[<>"\'&;]', '', text)

def chunk_text(text: str, chunk_size: int = 500) -> List[str]:
    """Split text into chunks of approximately equal size"""
    words = text.split()
    chunks = []
    current_chunk = []
    current_size = 0
    
    for word in words:
        if current_size + len(word) > chunk_size and current_chunk:
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_size = len(word)
        else:
            current_chunk.append(word)
            current_size += len(word) + 1
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    return chunks

def log_error(func_name: str, error: Exception, extra_data: Dict = None) -> None:
    """Standardized error logging"""
    error_data = {
        'function': func_name,
        'error_type': type(error).__name__,
        'error_message': str(error),
        'timestamp': format_timestamp(datetime.utcnow())
    }
    if extra_data:
        error_data.update(extra_data)
    logger.error(error_data)