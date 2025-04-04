from celery import Celery, Task
import os
from datetime import datetime, timedelta
import logging
import asyncio
import redis
from services.event_handler import publish_status_update
from services.hybrid_crawler import HybridCrawler
from services.database import MongoDB
from services.rag_retrieval import RAGRetrieval
from functools import wraps
from services.redis_fix import get_redis_connection
import time
import aiohttp
from bs4 import BeautifulSoup
from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Redis connection based on environment
redis_host = os.getenv('REDISHOST', os.getenv('REDIS_HOST', 'localhost'))
redis_port = int(os.getenv('REDISPORT', os.getenv('REDIS_PORT', 6379)))
redis_url = os.getenv('REDIS_URL', f'redis://{redis_host}:{redis_port}/0')

try:
    redis_client = get_redis_connection()
    print("Redis connection established for Celery worker")
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    # Fallback to dummy client that doesn't break the app
    class DummyRedis:
        def get(self, *args): return None
        def set(self, *args, **kwargs): pass
        def hset(self, *args, **kwargs): pass
        def hgetall(self, *args): return {}
        def publish(self, *args, **kwargs): pass
        def delete(self, *args): pass
        def expire(self, *args, **kwargs): pass
    redis_client = DummyRedis()
    logger.warning("Using dummy Redis client as fallback")

# Initialize Celery with the Redis URL
celery = Celery(
    'uni_tracker',
    broker=redis_url,
    backend=redis_url
)

# Celery settings
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_time_limit=None,
    task_soft_time_limit=None,
    worker_max_tasks_per_child=5,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
    task_track_started=True,
    broker_transport_options={'visibility_timeout': 86400*7},
    redis_max_connections=20,
    worker_concurrency=2
)

# Initialize services in worker
db = MongoDB(os.getenv('MONGODB_URI'))
rag = RAGRetrieval(
    openai_api_key=os.getenv('OPENAI_API_KEY'),
    pinecone_api_key=os.getenv('PINECONE_API_KEY'),
    # cohere_api_key=os.getenv('COHERE_API_KEY'),
    index_name=os.getenv('INDEX_NAME')
)

def ensure_db_connection(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        global db
        if db is None:
            db = MongoDB(os.getenv('MONGODB_URI'))
        db.connect()
        return f(*args, **kwargs)
    return wrapper

def store_progress(university_id: str, status: dict, ttl: int = 3600):
    try:
        redis_data = {}
        for key, value in status.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    redis_data[f"{key}_{sub_key}"] = str(sub_value)
            else:
                redis_data[key] = str(value)

        redis_client.hset(f"university_progress:{university_id}", mapping=redis_data)
        redis_client.expire(f"university_progress:{university_id}", ttl)
        
        db.update_university(university_id, {
            'status': status.get('status'),
            'progress': {
                'pages_crawled': int(status.get('pages_crawled', 0)),
                'total_pages': int(status.get('total_pages', 0)),
                'data_chunks': int(status.get('data_chunks', 0))
            },
            'last_updated': datetime.utcnow(),
            'metadata': status.get('metadata', {})
        })
    except Exception as e:
        logger.error(f"Error storing progress: {str(e)}")

@celery.task(bind=True, name='services.celery_worker.process_university_background')
def process_university_background(self, url: str, program: str, university_id: str, url_limit: int = 20000, email=None):
    logger.info(f"Starting processing for university {university_id} with URL: {url}")
    
    try:
        status = {
            'status': 'initializing',
            'pages_crawled': 0,
            'total_pages': 0,
            'data_chunks': 0,
            'timestamp': datetime.utcnow().isoformat()
        }
        store_progress(university_id, status)
        publish_status_update(university_id, 'processing', {
            'url': url,
            'program': program
        })
        db.update_university(university_id, {
            'status': 'initializing',
            'last_updated': datetime.utcnow()
        })

        crawler = HybridCrawler(
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            pinecone_api_key=os.getenv('PINECONE_API_KEY'),
            index_name=os.getenv('INDEX_NAME')
        )

        def progress_callback(progress_data: dict):
            try:
                current_status = {
                    'status': 'processing',
                    'pages_crawled': str(progress_data.get('processed_urls', 0)),
                    'total_pages': str(progress_data.get('total_urls', 0)),
                    'data_chunks': str(progress_data.get('data_chunks', 0)),
                    'timestamp': datetime.utcnow().isoformat()
                }
                store_progress(university_id, current_status)
                
                db.update_university(university_id, {
                    'status': 'processing',
                    'pages_crawled': progress_data.get('processed_urls', 0),
                    'total_pages': progress_data.get('total_urls', 0),
                    'data_chunks': progress_data.get('data_chunks', 0),
                    'last_updated': datetime.utcnow()
                })
                
                # Also publish status update for real-time updates
                publish_status_update(university_id, 'processing', {
                    'url': url,
                    'program': program,
                    'progress': progress_data
                })
                
            except Exception as e:
                logger.error(f"Error in progress callback: {str(e)}")

        crawler_result = asyncio.run(
            crawler.process_university(
                url=url,
                program=program,
                university_id=university_id,
                progress_callback=progress_callback,
                url_limit=url_limit
            )
        )

        if not crawler_result.get('success'):
            error_msg = crawler_result.get('error', 'Unknown error during processing')
            logger.error(f"Processing failed for {university_id}: {error_msg}")
            
            failure_status = {
                'status': 'failed',
                'error': error_msg,
                'timestamp': datetime.utcnow().isoformat()
            }
            store_progress(university_id, failure_status)
            
            db.update_university(university_id, {
                'status': 'failed',
                'error': error_msg,
                'last_updated': datetime.utcnow()
            })
            
            publish_status_update(university_id, 'failed', {
                'error': error_msg,
                'url': url,
                'program': program
            })
            
            return False

        stored_count = crawler_result.get('stored_count', 0)
        namespace = f"uni_{university_id}"

        success_status = {
            'status': 'completed',
            'pages_crawled': str(crawler_result.get('pages_crawled', 0)),
            'data_chunks': str(stored_count),
            'timestamp': datetime.utcnow().isoformat()
        }
        store_progress(university_id, success_status, ttl=86400)

        db.update_university(university_id, {
            'status': 'completed',
            'pages_crawled': crawler_result.get('pages_crawled', 0),
            'data_chunks': stored_count,
            'last_updated': datetime.utcnow()
        })

        if email:
            process_custom_columns_task.delay(
                university_id=university_id,
                namespace=namespace,
                user_email=email,
                url=url,
                program=program
            )
        else:
            # If no email provided, still publish completion status
            publish_status_update(university_id, 'completed', {
                'url': url,
                'program': program
            })

        return True

    except Exception as e:
        logger.error(f"Error processing university {university_id}: {str(e)}")
        error_status = {
            'status': 'failed',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }
        store_progress(university_id, error_status)
        
        db.update_university(university_id, {
            'status': 'failed',
            'error': str(e),
            'last_updated': datetime.utcnow()
        })

        publish_status_update(university_id, 'failed', {
            'error': str(e),
            'url': url,
            'program': program
        })
        
        return False

def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

def process_column_task(university_id: str, column: dict, namespace: str, user_email: str = None) -> bool:
    try:
        column_id = str(column.get('_id', ''))
        question = f"What is the {column['name']} requirement or information for this university program?"
        
        # Run async RAG query in synchronous context
        try:
            rag_result = run_async(rag.query(question, namespace))
            if not rag_result or 'answer' not in rag_result:
                logger.error(f"No RAG result for column {column['name']}")
                return False
                
            if user_email:
                db.save_column_data({
                    'university_id': university_id,
                    'column_id': column_id,
                    'user_email': user_email,
                    'value': rag_result['answer']
                })
                logger.info(f"Saved column data for user {user_email}")
                
            return True
            
        except Exception as e:
            logger.error(f"RAG query error: {str(e)}")
            if user_email:
                db.save_column_data({
                    'university_id': university_id,
                    'column_id': column_id,
                    'user_email': user_email,
                    'value': "Error processing data"
                })
            return False
            
    except Exception as e:
        logger.error(f"Error processing column task: {str(e)}")
        return False

# This is a partial update for process_custom_columns_task in celery_worker.py

@celery.task(bind=True, name='services.celery_worker.process_custom_columns')
def process_custom_columns_task(self, university_id: str, namespace: str, user_email: str = None, url: str = None, program: str = None):
    logger.info(f"Starting column processing for university {university_id}")
    
    try:
        if not user_email:
            logger.info("No user specified, skipping column processing")
            return True

        columns = db.get_custom_columns(user_email)
        if not columns:
            logger.info(f"No columns found for user {user_email}")
            # Still publish completed status if no columns to process
            publish_status_update(university_id, 'completed', {
                'url': url,
                'program': program
            })
            return True

        total_columns = len(columns)
        processed = 0
        
        progress_key = f"column_progress:{university_id}:{user_email}"
        redis_client.hset(
            progress_key,
            mapping={
                'total': total_columns,
                'processed': 0,
                'status': 'processing',
                'start_time': datetime.utcnow().isoformat()
            }
        )
        
        # Update status to processing columns
        publish_status_update(university_id, 'processing', {
            'url': url,
            'program': program,
            'message': 'Processing column data',
            'column_progress': {
                'total': total_columns,
                'processed': 0
            }
        })
        
        batch_size = 3  # Process in smaller batches for better UX feedback
        for i in range(0, len(columns), batch_size):
            batch = columns[i:i + batch_size]
            
            for column in batch:
                try:
                    existing_data = db.get_column_data_for_university(
                        university_id=university_id,
                        column_id=str(column['_id']),
                        user_email=user_email
                    )
                    
                    if existing_data:
                        processed += 1
                        logger.info(f"Column {column.get('name')} already processed")
                        
                        # Send the existing data to the client for immediate display
                        from services.event_handler import publish_cell_update
                        publish_cell_update(
                            university_id=university_id,
                            column_id=str(column['_id']),
                            value=rag_result['answer'],
                            user_email=user_email
                        )
                        time.sleep(0.05)
                        continue
                        
                    # Process the column
                    column_id = str(column.get('_id', ''))
                    question = f"What is the {column['name']} requirement or information for this university program?"
                    
                    # Run async RAG query in synchronous context
                    try:
                        rag_result = run_async(rag.query(question, namespace))
                        if not rag_result or 'answer' not in rag_result:
                            logger.error(f"No RAG result for column {column['name']}")
                            continue
                            
                        if user_email:
                            # Save to database
                            result = db.save_column_data({
                                'university_id': university_id,
                                'column_id': column_id,
                                'user_email': user_email,
                                'value': rag_result['answer']
                            })
                            
                            logger.info(f"Saved column data for user {user_email}")
                            
                            # Send real-time update for this specific cell
                            from services.event_handler import publish_cell_update
                            publish_cell_update(
                                university_id=university_id,
                                column_id=column_id,
                                value=rag_result['answer'],
                                user_email=user_email
                            )
                            
                            processed += 1
                        
                    except Exception as e:
                        logger.error(f"RAG query error: {str(e)}")
                        if user_email:
                            value = "Error processing data"
                            db.save_column_data({
                                'university_id': university_id,
                                'column_id': column_id,
                                'user_email': user_email,
                                'value': value
                            })
                            
                            # Still send update to client with error message
                            from services.event_handler import publish_cell_update
                            publish_cell_update(
                                university_id=university_id,
                                column_id=column_id,
                                value=value,
                                user_email=user_email
                            )
                            
                except Exception as e:
                    logger.error(f"Error processing column {column.get('name')}: {str(e)}")
                    continue
            
            # Update progress in Redis 
            redis_client.hset(
                progress_key,
                mapping={
                    'total': total_columns,
                    'processed': processed,
                    'current_batch': f"{i+1}/{total_columns}"
                }
            )
            
            # Send overall progress update (less frequently)
            publish_status_update(university_id, 'processing', {
                'url': url,
                'program': program,
                'message': 'Processing column data',
                'column_progress': {
                    'total': total_columns,
                    'processed': processed
                }
            })
            
            # Add a small delay between batches for better UX
            time.sleep(0.2)
        
        redis_client.hset(
            progress_key,
            mapping={
                'total': total_columns,
                'processed': processed,
                'status': 'completed',
                'end_time': datetime.utcnow().isoformat()
            }
        )
        redis_client.expire(progress_key, 86400)
        
        # Send final completed status
        publish_status_update(university_id, 'completed', {
            'url': url,
            'program': program,
            'message': 'All processing completed'
        })
        
        logger.info(f"Completed processing {processed}/{total_columns} columns for university {university_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error in column processing: {str(e)}")
        if user_email:
            redis_client.hset(
                f"column_progress:{university_id}:{user_email}",
                mapping={
                    'status': 'failed',
                    'error': str(e),
                    'end_time': datetime.utcnow().isoformat()
                }
            )
        
        publish_status_update(university_id, 'failed', {
            'url': url,
            'program': program,
            'error': str(e)
        })
        return False
    
@celery.task(name='services.celery_worker.cleanup_stale_tasks')
def cleanup_stale_tasks():
    try:
        two_hours_ago = datetime.utcnow() - timedelta(hours=2)
        
        stuck_unis = db.db.universities.find({
            'status': {'$in': ['initializing', 'processing']},
            'last_updated': {'$lt': two_hours_ago}
        })
        
        cleaned = 0
        for uni in stuck_unis:
            uni_id = uni['id']
            
            db.update_university(uni_id, {
                'status': 'failed',
                'error': 'Processing timed out',
                'last_updated': datetime.utcnow()
            })
            
            redis_client.delete(f"university_progress:{uni_id}")
            redis_client.delete(f"column_progress:{uni_id}")
            
            # Publish status update for frontend
            publish_status_update(uni_id, 'failed', {
                'error': 'Processing timed out after 2 hours'
            })
            
            cleaned += 1
            
        return {'cleaned': cleaned}
        
    except Exception as e:
        logger.error(f"Error in cleanup task: {str(e)}")
        return {'error': str(e)}

@celery.task(bind=True, name='services.celery_worker.process_pending_columns')
def process_pending_columns_task(self, user_email: str):
    """Process columns added during subscription expiry for newly visible universities"""
    logger.info(f"Processing pending columns for user: {user_email}")
    
    try:
        # Get user information
        user = db.get_user(user_email)
        if not user:
            logger.error(f"User not found: {user_email}")
            return {'error': 'User not found'}
            
        # Get pending columns
        pending_columns = db.get_pending_columns(user_email)
        logger.info(f"Found {len(pending_columns)} pending columns")
        
        if not pending_columns:
            logger.info("No pending columns to process")
            return {'success': True, 'message': 'No pending columns to process'}
        
        # Get all university IDs for this user
        selected_urls = user.get('selected_universities', [])
        all_universities = db.get_universities_by_urls(selected_urls)
        
        # Get all universities beyond the first 3 (these were previously hidden)
        previously_hidden_universities = all_universities[3:] if len(all_universities) > 3 else []
        previously_hidden_ids = [uni.get('id', str(uni['_id'])) for uni in previously_hidden_universities]
        
        logger.info(f"Found {len(previously_hidden_ids)} previously hidden universities")
        
        if not previously_hidden_ids:
            logger.info("No hidden universities to process")
            db.mark_pending_columns_processed(user_email)  # Mark all as processed anyway
            return {'success': True, 'message': 'No hidden universities to process'}
            
        # Send initial notification with detailed information
        from services.event_handler import publish_user_update
        publish_user_update(user_email, 'processing_started', {
            'hidden_universities_count': len(previously_hidden_ids),
            'pending_columns_count': len(pending_columns),
            'university_ids': previously_hidden_ids,  # Include university IDs for client-side loading
            'timestamp': datetime.utcnow().isoformat()
        })
        
        # Wait a short moment to ensure the client has time to initialize loading states
        time.sleep(0.5)
        
        # Process universities in smaller batches (2-3 at a time) with delays between batches
        batch_size = 2
        processed_count = 0
        
        # Process in batches
        for i in range(0, len(previously_hidden_ids), batch_size):
            batch_universities = previously_hidden_ids[i:i+batch_size]
            
            # Process each university in the batch
            for university_id in batch_universities:
                # Track processed columns for this university
                processed_columns = []
                
                # Process all columns for this university
                # Process all columns for this university
                for column in pending_columns:
                    try:
                        # Create namespace for RAG query
                        namespace = f"uni_{university_id}"
                        
                        # Process column using RAG
                        question = f"What is the {column['name']} requirement or information for this university program?"
                        
                        # Run async RAG query in synchronous context
                        rag_result = run_async(rag.query(question, namespace))
                        
                        if rag_result and 'answer' in rag_result:
                            # First save to database
                            value = rag_result['answer']
                            db.save_column_data({
                                'university_id': university_id,
                                'column_id': column['column_id'],
                                'user_email': user_email,
                                'value': value
                            })
                            processed_count += 1
                            
                            # Then immediately publish a separate event for this cell
                            from services.event_handler import publish_cell_update
                            publish_cell_update(
                                university_id=university_id,
                                column_id=column['column_id'],
                                value=value,
                                user_email=user_email
                            )
                            
                            # Add to processed columns list
                            processed_columns.append(column['column_id'])
                            logger.info(f"Successfully processed column {column['name']} for university {university_id}")
                            
                            # IMPORTANT: Add a small delay between cell updates
                            # This prevents network congestion and race conditions
                            time.sleep(0.15)
                            
                        else:
                            logger.warning(f"No RAG result for column {column['name']} and university {university_id}")
                            
                            # Still send a failure notification so the UI can update
                            from services.event_handler import publish_cell_update
                            publish_cell_update(
                                university_id=university_id,
                                column_id=column['column_id'],
                                value="No information available",
                                user_email=user_email
                            )
                            
                            # Small delay even after error updates
                            time.sleep(0.1)
                            
                    except Exception as e:
                        logger.error(f"Error processing column {column['name']} for university {university_id}: {str(e)}")
                        
                        # Send error notification
                        try:
                            from services.event_handler import publish_cell_update
                            publish_cell_update(
                                university_id=university_id,
                                column_id=column['column_id'],
                                value="Error processing data",
                                user_email=user_email
                            )
                        except Exception as publish_error:
                            logger.error(f"Error publishing cell update: {str(publish_error)}")
                        
                        # Small delay after error
                        time.sleep(0.1)
                        continue
                # Small delay between universities
                time.sleep(0.3)
            
            # Slightly longer delay between batches
            time.sleep(0.5)
        
        # Mark all pending columns as processed
        db.mark_pending_columns_processed(user_email)
        
        logger.info(f"Completed processing {processed_count} column-university combinations")
        
        # IMPORTANT: The final completion event should come AFTER all individual cell updates
        # Add this small delay to ensure cell updates are processed first
        time.sleep(1)
        
        # Emit a single final event when all processing is complete
        try:
            from services.event_handler import publish_user_update
            publish_user_update(user_email, 'subscription_reactivated', {
                'hidden_universities_processed': len(previously_hidden_ids),
                'columns_processed': len(pending_columns),
                'university_ids': previously_hidden_ids,  # Include university IDs for reference
                'timestamp': datetime.utcnow().isoformat()
            })
        except Exception as socket_error:
            logger.error(f"Error sending final user update: {str(socket_error)}")
        
        return {
            'success': True,
            'processed_count': processed_count,
            'universities_processed': len(previously_hidden_ids)
        }
        
    except Exception as e:
        logger.error(f"Error processing pending columns: {str(e)}")
        return {'error': str(e)}
    
@celery.task(bind=True, name='services.celery_worker.process_urls_task')
def process_urls_task(self, urls: List[str] = None, namespace: str = None, university_id: str = None, user_email: str = None):
    """
    Process specific URLs and store their content in the specified namespace
    
    Args:
        urls: List of URLs to process
        namespace: Vector database namespace
        university_id: University ID
        user_email: User email for tracking
    
    Returns:
        Processing results
    """
    logger.info(f"Processing {len(urls) if urls else 0} URLs for namespace: {namespace}")
    
    try:
        if not urls:
            return {
                'success': False,
                'error': 'No URLs provided'
            }
            
        if not namespace:
            namespace = f"uni_{university_id}" if university_id else "custom_namespace"
            
        # Get university details if available
        university_info = {}
        program = "MS CS"  # Default program
        
        if university_id:
            try:
                university = db.get_university_by_id(university_id)
                if university:
                    university_info = {
                        'name': university.get('name', ''),
                        'url': university.get('url', '')
                    }
                    program = university.get('programs', ['MS CS'])[0] if isinstance(university.get('programs'), list) else 'MS CS'
            except Exception as uni_err:
                logger.error(f"Error getting university info: {str(uni_err)}")
        
        # Initialize progress tracking
        task_id = self.request.id
        
        # Update task status to processing in database
        db.update_url_processing_status(
            task_id=task_id,
            status='processing',
            metadata={
                'total_urls': len(urls),
                'processed_urls': 0,
                'stored_chunks': 0,
                'start_time': datetime.utcnow().isoformat()
            }
        )
        
        # Process URLs using HybridCrawler's specialized method
        # Using a direct function call with asyncio.run rather than creating a crawler instance
        # to avoid the issues with forking processes
        crawler = HybridCrawler(
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            pinecone_api_key=os.getenv('PINECONE_API_KEY'),
            index_name=os.getenv('INDEX_NAME')
        )
        
        # Process URLs
        result = asyncio.run(HybridCrawler.process_specific_urls(
            urls=urls,
            university_id=university_id,
            program=program,
            namespace=namespace
        ))
        
        # Update task as completed
        completion_status = 'completed' if result.get('success') else 'failed'
        
        db.update_url_processing_status(
            task_id=task_id,
            status=completion_status,
            metadata={
                'total_urls': len(urls),
                'processed_urls': result.get('processed', 0),
                'failed_urls': result.get('failed', 0),
                'stored_chunks': result.get('stored_chunks', 0),
                'failures': result.get('failures', []),
                'completed_at': datetime.utcnow().isoformat()
            }
        )
        
        # If this was for a university, update the university record
        if university_id:
            update_data = {
                'metadata': {
                    'last_url_processing': {
                        'timestamp': datetime.utcnow(),
                        'urls_processed': result.get('processed', 0),
                        'stored_chunks': result.get('stored_chunks', 0),
                        'namespace': namespace
                    }
                }
            }
            
            # If URLs were successfully processed, update university status
            if result.get('success') and result.get('stored_chunks', 0) > 0:
                update_data['status'] = 'completed'
                
            db.update_university(university_id, update_data)
            
            # Send status update via Redis for real-time updates
            try:
                from services.event_handler import publish_status_update
                publish_status_update(university_id, completion_status, {
                    'message': f"Processed {result.get('processed', 0)} URLs with {result.get('stored_chunks', 0)} chunks",
                    'urls_processed': result.get('processed', 0),
                    'stored_chunks': result.get('stored_chunks', 0)
                })
            except Exception as notify_err:
                logger.error(f"Error publishing status update: {str(notify_err)}")
        
        # Return processing results
        return {
            'success': result.get('success', False),
            'processed_urls': result.get('processed', 0),
            'stored_chunks': result.get('stored_chunks', 0),
            'failed_urls': result.get('failed', 0),
            'namespace': namespace
        }
        
    except Exception as e:
        logger.error(f"Error processing URLs: {str(e)}")
        
        # Update task as failed
        try:
            db.update_url_processing_status(
                task_id=self.request.id,
                status='failed',
                metadata={
                    'error': str(e)
                }
            )
        except Exception as update_err:
            logger.error(f"Error updating task status: {str(update_err)}")
        
        return {
            'success': False,
            'error': str(e)
        }
