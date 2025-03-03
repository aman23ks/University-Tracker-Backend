import asyncio
from collections import deque
import hashlib
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import logging
from typing import List, Dict, Set
from openai import OpenAI
from pinecone import Pinecone
import trafilatura
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import cachetools
import time
import os
import redis
import xml.etree.ElementTree as ET
import gzip
from io import BytesIO

logger = logging.getLogger(__name__)

# Initialize Redis client
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)

class HybridCrawler:
    def __init__(self, openai_api_key: str, pinecone_api_key: str, index_name: str):
        self.visited_urls: Set[str] = set()
        self.urls_to_visit: deque = deque()
        self.sitemap_urls: Set[str] = set()  # New: Store sitemap URLs separately
        self.extracted_data: List[Dict] = []
        self.client = OpenAI(api_key=openai_api_key)
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.index = self.pc.Index(index_name)
        self.openai_api_key = openai_api_key
        self.pinecone_api_key = pinecone_api_key
        self.index_name = index_name
        
        # Memory-efficient caching with longer TTL
        self.embedding_cache = cachetools.TTLCache(maxsize=1000, ttl=3600)
        
        # Production-optimized headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Production-optimized limits
        self.batch_size = 20  # Increased to process more URLs per batch
        self.concurrent_requests = 15  # Increased concurrency
        self.timeout = 15  # Longer timeout for complex pages
        self.min_request_interval = 0.05  # Faster requests
        self.max_retries = 3  # Retry failed requests
        
        # Less restrictive URL filtering for better coverage
        self.ignore_patterns = re.compile(
            r'\.(css|js|jpg|jpeg|png|gif|pdf|zip|ico|svg)$|'
            r'(login|logout|signup|signin|register|auth)',
            re.IGNORECASE
        )
        
        # Stats tracking
        self.stats = {
            'pages_crawled': 0,
            'pages_extracted': 0,
            'sitemap_urls_found': 0
        }
        
        # IMPORTANT: Override any URL limit to ensure ALL pages are crawled
        self.enforce_url_limit = False

    async def initialize_crawl(self, root_url: str, program: str, university_id: str):
        """Initialize crawling with sitemap discovery"""
        self.root_domain = urlparse(root_url).netloc
        self.scheme = urlparse(root_url).scheme
        self.program = program
        self.university_id = university_id
        
        # Clear data structures
        self.visited_urls.clear()
        self.urls_to_visit.clear()
        self.sitemap_urls.clear()
        self.extracted_data.clear()
        
        # Reset stats
        self.stats = {
            'pages_crawled': 0,
            'pages_extracted': 0,
            'sitemap_urls_found': 0
        }
        
        # Try to discover sitemaps first
        await self.discover_sitemaps()
        
        # Add all sitemap URLs to the queue
        if self.sitemap_urls:
            for url in self.sitemap_urls:
                if url not in self.urls_to_visit:
                    self.urls_to_visit.append(url)
        
        # Add root URL if queue is empty
        if not self.urls_to_visit:
            self.urls_to_visit.append(root_url)
        
        # Initialize progress in Redis
        await self.update_progress({
            'status': 'initializing',
            'total_urls': len(self.urls_to_visit),
            'processed_urls': 0,
            'current_batch': 0,
            'sitemap_urls': len(self.sitemap_urls)
        })

    async def discover_sitemaps(self):
        """Find and process sitemap files"""
        base_url = f"{self.scheme}://{self.root_domain}"
        sitemap_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap.php',
            '/sitemap/',
            '/sitemaps/',
            '/sitemap.gz',
            '/sitemap.xml.gz',
            '/robots.txt'  # Check robots.txt for sitemap directives
        ]
        
        logger.info(f"Looking for sitemaps at {self.root_domain}")
        
        async with aiohttp.ClientSession() as session:
            # First try robots.txt to find sitemaps
            try:
                async with session.get(f"{base_url}/robots.txt", headers=self.headers, timeout=10) as response:
                    if response.status == 200:
                        text = await response.text()
                        # Parse sitemap URLs from robots.txt
                        sitemap_matches = re.findall(r'Sitemap:\s*(https?://[^\s]+)', text, re.IGNORECASE)
                        for sitemap_url in sitemap_matches:
                            await self.process_sitemap(session, sitemap_url.strip())
            except Exception as e:
                logger.warning(f"Error checking robots.txt: {str(e)}")
            
            # Check common sitemap locations
            for path in sitemap_paths:
                if path == '/robots.txt':  # Already checked
                    continue
                    
                try:
                    sitemap_url = f"{base_url}{path}"
                    logger.info(f"Checking sitemap at {sitemap_url}")
                    await self.process_sitemap(session, sitemap_url)
                except Exception as e:
                    logger.warning(f"Error checking sitemap at {path}: {str(e)}")

    async def process_sitemap(self, session, sitemap_url):
        """Process a sitemap XML file"""
        try:
            async with session.get(sitemap_url, headers=self.headers, timeout=15) as response:
                if response.status != 200:
                    return
                
                # Check for gzipped content
                content_type = response.headers.get('Content-Type', '')
                is_gzipped = 'gzip' in content_type or sitemap_url.endswith('.gz')
                
                if is_gzipped:
                    # Handle gzipped content
                    content_bytes = await response.read()
                    with gzip.GzipFile(fileobj=BytesIO(content_bytes), mode='rb') as f:
                        sitemap_content = f.read().decode('utf-8')
                else:
                    # Normal content
                    sitemap_content = await response.text()
                
                # Process sitemap index (collection of sitemaps)
                if '<sitemapindex' in sitemap_content:
                    logger.info(f"Found sitemap index at {sitemap_url}")
                    try:
                        root = ET.fromstring(sitemap_content)
                        # Process each sitemap in the index
                        for sitemap in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap'):
                            loc = sitemap.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                            if loc is not None and loc.text:
                                child_sitemap = loc.text.strip()
                                await self.process_sitemap(session, child_sitemap)
                    except ET.ParseError:
                        logger.warning(f"XML parsing error in sitemap index: {sitemap_url}")
                
                # Process regular sitemap with URLs
                elif '<urlset' in sitemap_content:
                    logger.info(f"Processing sitemap at {sitemap_url}")
                    try:
                        root = ET.fromstring(sitemap_content)
                        # Extract all URLs
                        count = 0
                        for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                            loc = url.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                            if loc is not None and loc.text:
                                page_url = loc.text.strip()
                                # Filter URLs by domain and patterns
                                if self.root_domain in urlparse(page_url).netloc and not self.ignore_patterns.search(page_url):
                                    self.sitemap_urls.add(page_url)
                                    count += 1
                        
                        logger.info(f"Added {count} URLs from sitemap {sitemap_url}")
                        self.stats['sitemap_urls_found'] += count
                    except ET.ParseError:
                        logger.warning(f"XML parsing error in sitemap: {sitemap_url}")
        
        except Exception as e:
            logger.error(f"Error processing sitemap {sitemap_url}: {str(e)}")

    def is_valid_url(self, url: str) -> bool:
        """Improved URL validation"""
        try:
            parsed = urlparse(url)
            
            # Basic validation
            if not parsed.scheme or not parsed.netloc:
                return False
                
            # Must be HTTP(S)
            if parsed.scheme not in ('http', 'https'):
                return False
                
            # Must be same domain or subdomain
            if self.root_domain not in parsed.netloc:
                return False
                
            # Skip ignored patterns
            if self.ignore_patterns.search(url):
                return False
                
            # Skip URLs that are too long
            if len(url) > 500:
                return False
                
            return True

        except Exception:
            return False

    def get_embedding(self, text: str) -> List[float]:
        """Get OpenAI embedding with caching"""
        # Check cache first
        cache_key = hash(text)
        if cache_key in self.embedding_cache:
            return self.embedding_cache[cache_key]
            
        try:
            response = self.client.embeddings.create(
                model="text-embedding-ada-002",
                input=text
            )
            embedding = response.data[0].embedding
            # Cache the result
            self.embedding_cache[cache_key] = embedding
            return embedding
        except Exception as e:
            logger.error(f"Error getting embedding: {str(e)}")
            return []

    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        try:
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'[^\w\s.,;?!-]', '', text)
            return text.strip()
        except Exception:
            return text

    async def extract_all_links(self, html: str, base_url: str) -> Set[str]:
        """Enhanced link extraction for better coverage"""
        links = set()
        try:
            # Use lxml parser for speed
            soup = BeautifulSoup(html, 'lxml')
            
            # First, get links from navigation elements (high priority)
            for nav in soup.find_all(['nav', 'header', 'menu']):
                for link in nav.find_all('a', href=True):
                    href = link.get('href')
                    if not href:
                        continue
                    
                    url = urljoin(base_url, href)
                    if self.is_valid_url(url):
                        links.add(url)
            
            # Then process all content areas
            content_areas = soup.find_all(['main', 'article', 'div', 'section'])
            
            for area in content_areas:
                for link in area.find_all('a', href=True):
                    href = link.get('href')
                    if not href:
                        continue
                        
                    # Convert to absolute URL
                    url = urljoin(base_url, href)
                    
                    # Validate URL
                    if self.is_valid_url(url):
                        links.add(url)
            
            # Finally, check all remaining links in the page
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if not href:
                    continue
                
                url = urljoin(base_url, href)
                if self.is_valid_url(url):
                    links.add(url)
                
            return links

        except Exception as e:
            logger.error(f"Error extracting links from {base_url}: {str(e)}")
            return links
        
    async def process_content(self, url: str, html_content: str) -> Optional[Dict]:
        """Improved content extraction"""
        try:
            # First try with trafilatura
            extracted_text = trafilatura.extract(
                html_content,
                include_comments=False,
                no_fallback=False,
                include_tables=True,
                target_language=None,
                deduplicate=True,
                favor_recall=True  # Extract more content
            )
            
            # Fallback to BeautifulSoup
            if not extracted_text or len(extracted_text.strip()) < 100:
                soup = BeautifulSoup(html_content, 'lxml')
                
                # Remove noise elements
                for tag in soup.find_all(['script', 'style', 'iframe']):
                    tag.decompose()
                
                # Try to find main content
                main_content = None
                for selector in ['main', 'article', '#content', '.content', '#main', '.main']:
                    found = soup.select(selector)
                    if found:
                        main_content = found[0]
                        break
                
                # If no main content, use body
                if not main_content:
                    main_content = soup.body
                
                if main_content:
                    # Get all text elements
                    paragraphs = main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'td', 'div'])
                    extracted_text = ' '.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20])
                    
            if extracted_text:
                # Clean and normalize text
                cleaned_text = ' '.join(extracted_text.split())
                if len(cleaned_text.split()) > 50:  # Only keep if meaningful content
                    logger.info(f"Successfully extracted {len(cleaned_text.split())} words from {url}")
                    self.stats['pages_extracted'] += 1
                    
                    # Get page title
                    title = ""
                    try:
                        soup = BeautifulSoup(html_content, 'lxml')
                        if soup.title:
                            title = soup.title.string.strip()
                    except:
                        pass
                    
                    return {
                        'url': url,
                        'content': cleaned_text,
                        'metadata': {
                            'program': self.program,
                            'title': title,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    }
            
            logger.warning(f"No meaningful content extracted from {url}")
            return None

        except Exception as e:
            logger.error(f"Error processing content from {url}: {str(e)}")
            return None

    async def crawl_url(self, session: aiohttp.ClientSession, url: str) -> None:
        """Improved URL crawling with better error handling"""
        if url in self.visited_urls:
            return
            
        # Mark as visited before processing to avoid duplicates
        self.visited_urls.add(url)
            
        for attempt in range(self.max_retries):
            try:
                async with session.get(
                    url, 
                    headers=self.headers,
                    timeout=self.timeout,
                    ssl=False,
                    allow_redirects=True
                ) as response:
                    if response.status == 200:
                        # Check if it's HTML content
                        content_type = response.headers.get('Content-Type', '')
                        if 'text/html' not in content_type and 'application/xhtml+xml' not in content_type:
                            return
                            
                        html = await response.text()
                        
                        # Process content first
                        processed_data = await self.process_content(url, html)
                        if processed_data:
                            self.extracted_data.append(processed_data)
                        
                        # Extract and queue new links
                        new_links = await self.extract_all_links(html, url)
                        
                        # Add new links to the queue if not already visited
                        for new_url in new_links:
                            if new_url not in self.visited_urls and new_url not in self.urls_to_visit:
                                self.urls_to_visit.append(new_url)
                        
                        # Update stats
                        self.stats['pages_crawled'] += 1
                        break
                    
                    elif response.status in [301, 302, 303, 307, 308]:
                        # Handle redirects
                        redirect_url = response.headers.get('Location')
                        if redirect_url:
                            absolute_url = urljoin(url, redirect_url)
                            if self.is_valid_url(absolute_url) and absolute_url not in self.visited_urls:
                                self.urls_to_visit.append(absolute_url)
                        break
                    
                    elif response.status in [403, 404, 410, 429, 500, 503]:
                        # Don't retry for these status codes
                        break
                    
            except asyncio.TimeoutError:
                if attempt == self.max_retries - 1:
                    logger.error(f"Timeout on {url} after {self.max_retries} attempts")
                await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                continue
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Error crawling {url}: {str(e)}")
                await asyncio.sleep(1 * (attempt + 1))
                continue

    async def crawl(self, progress_callback=None, url_limit: int = None) -> Dict:
        """Enhanced crawling with no actual URL limit to ensure ALL pages are crawled"""
        actual_limit = None  # IMPORTANT: We're ignoring url_limit to crawl everything
        
        # Only log the original URL limit for debugging, but we won't use it
        print(f"Starting crawl with URL limit: {url_limit or 'unlimited'} (Actual: unlimited)")
        
        try:
            stored_count = 0
            batch_size = 50  # Process in batches of 50
            processed_urls = 0
            total_urls_found = len(self.urls_to_visit) + len(self.sitemap_urls)
            
            print(f"Total URLs to process: {total_urls_found}")
            
            # Start with sitemap URLs if found
            if self.sitemap_urls:
                print(f"Processing ALL {len(self.sitemap_urls)} URLs from sitemap")
            
            connector = aiohttp.TCPConnector(limit=self.concurrent_requests, ssl=False)
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Keep processing until the queue is empty - ALWAYS process ALL URLs
                while self.urls_to_visit:
                    # Process URLs in batches
                    batch_urls = []
                    for _ in range(self.batch_size):
                        if not self.urls_to_visit:
                            break
                        url = self.urls_to_visit.popleft()
                        if url not in self.visited_urls:
                            batch_urls.append(url)
                            processed_urls += 1

                    if not batch_urls:
                        continue
                    
                    total_now = processed_urls + len(self.urls_to_visit)
                    if total_now > total_urls_found:
                        total_urls_found = total_now
                    
                    print(f"Processing batch of {len(batch_urls)} URLs. Progress: {processed_urls}/{total_urls_found}")
                    
                    # Process batch
                    tasks = [self.crawl_url(session, url) for url in batch_urls]
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Update progress
                    if progress_callback:
                        progress_callback({
                            'processed_urls': processed_urls,
                            'total_urls': total_urls_found,
                            'data_chunks': stored_count
                        })

                    # Store batch data when we have enough
                    if len(self.extracted_data) >= batch_size:
                        print(f"Storing batch of {len(self.extracted_data)} documents")
                        namespace = f"uni_{self.university_id}"
                        batch_count = await self.store_batch_in_pinecone(self.extracted_data, namespace)
                        stored_count += batch_count
                        print(f"Stored {batch_count} chunks in Pinecone")
                        self.extracted_data = []  # Clear after storing
                    
                    # Report progress more frequently
                    if processed_urls % 100 == 0:
                        print(f"Progress update: {processed_urls} URLs processed, {len(self.urls_to_visit)} URLs remaining")
                        # Update MongoDB
                        await self.update_progress({
                            'status': 'processing',
                            'processed_urls': processed_urls,
                            'total_urls': total_urls_found,
                            'data_chunks': stored_count
                        })
                    
                    await asyncio.sleep(0.05)  # Small delay between batches
                
                # Store any remaining data
                if self.extracted_data:
                    print(f"Storing final {len(self.extracted_data)} documents")
                    namespace = f"uni_{self.university_id}"
                    batch_count = await self.store_batch_in_pinecone(self.extracted_data, namespace)
                    stored_count += batch_count
                    print(f"Stored final {batch_count} chunks in Pinecone")
                
                print(f"\nCrawl Summary:")
                print(f"Total URLs processed: {processed_urls}")
                print(f"Total pages crawled: {self.stats['pages_crawled']}")
                print(f"Total pages with content: {self.stats['pages_extracted']}")
                print(f"Total data chunks stored: {stored_count}")
                
                return {
                    'success': True,
                    'stored_count': stored_count,
                    'processed_urls': processed_urls,
                    'pages_crawled': len(self.visited_urls),
                    'total_pages': total_urls_found
                }
                
        except Exception as e:
            print(f"Error in crawl: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'processed_urls': processed_urls,
                'stored_count': stored_count
            }

    async def get_embedding_async(self, text: str) -> List[float]:
        """Asynchronous embedding generation"""
        # Limit text length for embedding API
        if len(text) > 8000:
            text = text[:8000]
            
        cache_key = hash(text)
        if cache_key in self.embedding_cache:
            return self.embedding_cache[cache_key]
            
        try:
            response = await asyncio.to_thread(
                self.client.embeddings.create,
                model="text-embedding-ada-002",
                input=text
            )
            embedding = response.data[0].embedding
            self.embedding_cache[cache_key] = embedding
            return embedding
        except Exception as e:
            logger.error(f"Error getting embedding: {str(e)}")
            return []

    def chunk_text(self, text: str, chunk_size: int = 10000) -> List[str]:
        """Optimized text chunking with better semantic boundaries"""
        chunks = []
        
        # First, clean the text
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Split into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            sentence_size = len(sentence)
            
            # Skip very long sentences
            if sentence_size > chunk_size:
                continue
                
            if current_size + sentence_size > chunk_size:
                if current_chunk:
                    chunks.append(' '.join(current_chunk))
                current_chunk = [sentence]
                current_size = sentence_size
            else:
                current_chunk.append(sentence)
                current_size += sentence_size

        if current_chunk:
            chunks.append(' '.join(current_chunk))

        return chunks

    async def store_batch_in_pinecone(self, batch_data: List[Dict], namespace: str) -> int:
        """Store processed data in Pinecone with better error handling"""
        try:
            vectors = []
            stored_count = 0
            
            for doc in batch_data:
                if not doc.get('content'):
                    continue
                
                chunks = self.chunk_text(doc['content'])
                print(f"Created {len(chunks)} chunks from document: {doc['url']}")
                
                for idx, chunk in enumerate(chunks):
                    if len(chunk.split()) < 50:  # Skip very short chunks
                        continue
                    
                    # Use async embedding
                    embedding = await self.get_embedding_async(chunk)
                    if not embedding:
                        continue
                    
                    vector = {
                        'id': f"{hashlib.md5(doc['url'].encode()).hexdigest()}_{idx}_{int(time.time())}",
                        'values': embedding,
                        'metadata': {
                            'text': chunk,
                            'url': doc['url'],
                            'program': doc.get('metadata', {}).get('program', ''),
                            'title': doc.get('metadata', {}).get('title', ''),
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    }
                    vectors.append(vector)
                    
                    if len(vectors) >= 50:
                        # Store batch asynchronously 
                        await asyncio.to_thread(
                            self.index.upsert,
                            vectors=vectors,
                            namespace=namespace
                        )
                        stored_count += len(vectors)
                        vectors = []
                
            # Store remaining vectors
            if vectors:
                await asyncio.to_thread(
                    self.index.upsert,
                    vectors=vectors,
                    namespace=namespace
                )
                stored_count += len(vectors)
                
            await asyncio.sleep(1)  
            return stored_count
            
        except Exception as e:
            print(f"Error in store_batch_in_pinecone: {str(e)}")
            return 0

    async def update_progress(self, progress_data: Dict):
        """Update progress in Redis with TTL and MongoDB"""
        try:
            # Update Redis
            progress_key = f"university_progress:{self.university_id}"
            redis_client.hset(
                progress_key,
                mapping=progress_data
            )
            redis_client.expire(
                progress_key,
                3600  # 1 hour TTL
            )

            # Update MongoDB status
            status_data = {
                'status': progress_data.get('status', 'processing'),
                'progress': {
                    'processed_urls': int(progress_data.get('processed_urls', 0)),
                    'total_urls': int(progress_data.get('total_urls', 0)),
                    'data_chunks': int(progress_data.get('data_chunks', 0))
                },
                'last_updated': datetime.utcnow()
            }

            # Add error if present
            if 'error' in progress_data:
                status_data['error'] = progress_data['error']

            # Update MongoDB through database service
            from services.database import MongoDB
            db = MongoDB(os.getenv('MONGODB_URI'))
            db.update_university(self.university_id, status_data)

        except Exception as e:
            logger.error(f"Error updating progress: {str(e)}")

    @classmethod
    async def process_university(cls, url: str, program: str, university_id: str, progress_callback=None, url_limit: int = None):
        """Main processing method with sitemap discovery and progress tracking
        Note: url_limit is ignored to ensure ALL pages are crawled
        """
        try:
            # Create crawler instance
            instance = cls(
                openai_api_key=os.getenv('OPENAI_API_KEY'),
                pinecone_api_key=os.getenv('PINECONE_API_KEY'),
                index_name=os.getenv('INDEX_NAME')
            )
            
            # IMPORTANT: We will completely ignore the url_limit parameter 
            # and always crawl everything
            
            # Initialize crawl with sitemap discovery
            await instance.initialize_crawl(url, program, university_id)
            namespace = f"uni_{university_id}"
            
            print(f"Processing university {university_id}")
            print(f"Using namespace: {namespace}")
            print(f"Found {len(instance.sitemap_urls)} URLs from sitemaps")
            print(f"Will process ALL URLs regardless of any limit")
            
            # Initial progress
            if progress_callback:
                progress_callback({
                    'processed_urls': 0,
                    'total_urls': max(1, len(instance.urls_to_visit)),
                    'data_chunks': 0,
                    'phase': 'initializing',
                    'sitemap_urls': len(instance.sitemap_urls)
                })
                
            # Crawl with progress updates - IMPORTANT: Pass None for url_limit to crawl everything
            crawl_result = await instance.crawl(
                progress_callback=progress_callback,
                url_limit=None  # Force to None to ensure ALL pages are crawled
            )
            
            if not crawl_result.get('success'):
                return {
                    'success': False,
                    'error': crawl_result.get('error', 'No data extracted from website'),
                    'stored_count': crawl_result.get('stored_count', 0)
                }
                
            return {
                'success': True,
                'pages_crawled': crawl_result.get('pages_crawled', 0),
                'total_pages': crawl_result.get('total_pages', 0),
                'stored_count': crawl_result.get('stored_count', 0),
                'sitemap_urls': len(instance.sitemap_urls)
            }
            
        except Exception as e:
            print(f"Error processing university {url}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'stored_count': 0
            }
            
    def __del__(self):
        """Cleanup method to ensure proper resource release"""
        try:
            # Clear caches
            if hasattr(self, 'embedding_cache'):
                self.embedding_cache.clear()
            
            # Clear collections
            if hasattr(self, 'visited_urls'):
                self.visited_urls.clear()
            if hasattr(self, 'urls_to_visit'):
                self.urls_to_visit.clear()
            if hasattr(self, 'sitemap_urls'):
                self.sitemap_urls.clear()
            if hasattr(self, 'extracted_data'):
                self.extracted_data.clear()
                
        except Exception as e:
            logger.error(f"Error in cleanup: {str(e)}")

    @staticmethod
    async def cleanup_university_data(university_id: str):
        """Clean up university processing data from Redis"""
        try:
            # Remove progress data
            redis_client.delete(f"university_progress:{university_id}")
            redis_client.delete(f"column_progress:{university_id}")
            
            return True
        except Exception as e:
            logger.error(f"Error cleaning up university data: {str(e)}")
            return False