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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Redis
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)

# Initialize Celery
celery = Celery('uni_tracker',
                broker=os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
                backend=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))

# Celery settings
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_time_limit=7200,
    task_soft_time_limit=6900,
    worker_max_tasks_per_child=5,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
    task_track_started=True,
    broker_transport_options={'visibility_timeout': 43200},
    redis_max_connections=20,
    worker_concurrency=2
)

# Initialize services
db = MongoDB(os.getenv('MONGO_URI'))
rag = RAGRetrieval(
    openai_api_key=os.getenv('OPENAI_API_KEY'),
    pinecone_api_key=os.getenv('PINECONE_API_KEY'),
    cohere_api_key=os.getenv('COHERE_API_KEY'),
    index_name=os.getenv('INDEX_NAME')
)

def ensure_db_connection(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        global db
        if db is None:
            db = MongoDB(os.getenv('MONGO_URI'))
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
def process_university_background(self, url: str, program: str, university_id: str, url_limit: int = 15, email=None):
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
            'error': str(e)
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
        
        batch_size = 5
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
                        continue
                        
                    success = process_column_task(
                        university_id=university_id,
                        column=column,
                        namespace=namespace,
                        user_email=user_email
                    )
                    
                    if success:
                        processed += 1
                        logger.info(f"Successfully processed column {column.get('name')}")
                    
                except Exception as e:
                    logger.error(f"Error processing column {column.get('name')}: {str(e)}")
                    continue
            
            redis_client.hset(
                progress_key,
                mapping={
                    'total': total_columns,
                    'processed': processed,
                    'current_batch': f"{i+1}/{total_columns}"
                }
            )
        
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
        
        publish_status_update(university_id, 'completed', {
            'url': url,
            'program': program
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
            'error': str(e)
        })
        return False

@celery.task(name='services.celery_worker.cleanup_stale_tasks')
def cleanup_stale_tasks():
    try:
        two_hours_ago = datetime.utcnow() - timedelta(hours=2)
        
        stuck_unis = db.universities.find({
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
            
            cleaned += 1
            
        return {'cleaned': cleaned}
        
    except Exception as e:
        logger.error(f"Error in cleanup task: {str(e)}")
        return {'error': str(e)}