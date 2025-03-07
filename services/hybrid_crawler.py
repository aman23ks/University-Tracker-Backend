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
import time
import os
import redis
import xml.etree.ElementTree as ET
import gzip
from io import BytesIO

logger = logging.getLogger(__name__)

# Define important keywords for targeting specific information
IMPORTANT_KEYWORDS = [
    # Admissions related
    'admission', 'application', 'deadline', 'requirements', 'apply', 'eligibility',
    'selection', 'entry requirements', 'application process',
    
    # Academics related
    'curriculum', 'course', 'syllabus', 'program structure', 'specialization',
    'concentration', 'major', 'minor', 'credit', 'semester', 'academic requirements',
    
    # Fees and financial aid
    'tuition', 'fee', 'cost', 'financial', 'scholarship', 'funding', 'aid',
    'assistantship', 'payment', 'expense', 'waiver',
    
    # Test scores
    'gre', 'gmat', 'toefl', 'ielts', 'sat', 'act', 'test requirement',
    'english proficiency', 'language requirement', 'score', 'minimum score',
    
    # Deadlines
    'deadline', 'due date', 'application timeline', 'important dates',
    'rolling admission', 'early admission', 'priority deadline',
    
    # Documentation requirements
    'prerequisite', 'document', 'transcript', 'recommendation',
    'resume', 'cv', 'statement of purpose', 'personal statement', 'letter of recommendation',
    
    # Placements and career outcomes
    'placement', 'career', 'job', 'employment', 'internship', 'recruitment',
    'salary', 'companies', 'employers', 'industry', 'opportunities', 'alumni',
    
    # Class size and teaching approach
    'class size', 'student-faculty ratio', 'average class', 'faculty-student',
    'classroom size', 'cohort size', 'batch size', 'teaching assistant',
    
    # Faculty information
    'faculty', 'professor', 'instructor', 'teacher', 'staff', 'research interest',
    'expertise', 'department', 'specialization',
    
    # Campus and facilities
    'student life', 'campus', 'housing', 'accommodation', 'dormitory', 'residence',
    'club', 'organization', 'activity', 'event', 'facility',
    
    # International student information
    'international student', 'visa', 'i-20', 'foreign', 'global', 'overseas',
    'international application', 'international admission'
]

class HybridCrawler:
    def __init__(self, openai_api_key: str, pinecone_api_key: str, index_name: str):
        self.visited_urls: Set[str] = set()
        self.urls_to_visit: deque = deque()
        self.sitemap_urls: Set[str] = set()
        self.important_urls: Set[str] = set()  # URLs with important keywords
        self.extracted_data: List[Dict] = []
        
        self.client = OpenAI(api_key=openai_api_key)
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.index = self.pc.Index(index_name)
        
        self.openai_api_key = openai_api_key
        self.pinecone_api_key = pinecone_api_key
        self.index_name = index_name
        
        # Headers for HTTP requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Request configuration
        self.batch_size = 20
        self.concurrent_requests = 15
        self.timeout = 15
        self.max_retries = 3
        
        # URL filtering
        self.ignore_patterns = re.compile(
            r'\.(css|js|jpg|jpeg|png|gif|pdf|zip|ico|svg)$|'
            r'(login|logout|signup|signin|register|auth)',
            re.IGNORECASE
        )
        
        # Stats tracking
        self.stats = {
            'pages_crawled': 0,
            'urls_discovered': 0,
            'important_urls_found': 0,
            'stored_chunks': 0
        }

    async def initialize_crawl(self, root_url: str, program: str, university_id: str):
        """Initialize crawling with root URL and program info"""
        self.root_domain = urlparse(root_url).netloc
        self.scheme = urlparse(root_url).scheme
        self.program = program
        self.university_id = university_id
        
        # Clear data structures
        self.visited_urls.clear()
        self.urls_to_visit.clear()
        self.sitemap_urls.clear()
        self.important_urls.clear()
        self.extracted_data.clear()
        
        # Reset stats
        self.stats = {
            'pages_crawled': 0,
            'urls_discovered': 0,
            'important_urls_found': 0,
            'stored_chunks': 0
        }
        
        # Start with discovering sitemaps for efficient URL collection
        await self.discover_sitemaps()
        
        # Add all sitemap URLs to the queue
        if self.sitemap_urls:
            for url in self.sitemap_urls:
                if url not in self.urls_to_visit:
                    self.urls_to_visit.append(url)
        
        # Add root URL if queue is empty
        if not self.urls_to_visit:
            self.urls_to_visit.append(root_url)

    async def discover_sitemaps(self):
        """Find sitemap files to get initial URLs"""
        base_url = f"{self.scheme}://{self.root_domain}"
        sitemap_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap.php',
            '/sitemap/',
            '/sitemaps/',
            '/sitemap.gz',
            '/sitemap.xml.gz',
            '/robots.txt'
        ]
        
        logger.info(f"Looking for sitemaps at {self.root_domain}")
        
        async with aiohttp.ClientSession() as session:
            # First try robots.txt to find sitemaps
            try:
                async with session.get(f"{base_url}/robots.txt", headers=self.headers, timeout=10) as response:
                    if response.status == 200:
                        text = await response.text()
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
        """Process a sitemap XML file to extract URLs"""
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
                                if self.is_valid_url(page_url):
                                    self.sitemap_urls.add(page_url)
                                    count += 1
                                    
                                    # Check URL path for important keywords
                                    url_lower = page_url.lower()
                                    if any(keyword in url_lower for keyword in IMPORTANT_KEYWORDS):
                                        self.important_urls.add(page_url)
                                        self.stats['important_urls_found'] += 1
                        
                        logger.info(f"Added {count} URLs from sitemap {sitemap_url}")
                        self.stats['urls_discovered'] += count
                    except ET.ParseError:
                        logger.warning(f"XML parsing error in sitemap: {sitemap_url}")
        
        except Exception as e:
            logger.error(f"Error processing sitemap {sitemap_url}: {str(e)}")

    def is_valid_url(self, url: str) -> bool:
        """Validate URL for crawling"""
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

    async def extract_all_links(self, html: str, base_url: str) -> Set[str]:
        """Extract all links from HTML content"""
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

    def contains_important_keywords(self, text: str, url: str) -> bool:
        """Check if text or URL contains any important keywords"""
        text_lower = text.lower()
        url_lower = url.lower()
        
        # Check URL first (faster than checking content)
        for keyword in IMPORTANT_KEYWORDS:
            if keyword in url_lower:
                return True
        
        # Then check content
        for keyword in IMPORTANT_KEYWORDS:
            if keyword in text_lower:
                return True
                
        return False

    async def crawl_url(self, session: aiohttp.ClientSession, url: str, extract_only: bool = True) -> None:
        """
        Crawl URL to either extract links or process content
        
        Args:
            session: HTTP session
            url: URL to crawl
            extract_only: If True, only extract links without processing content
        """
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
                        
                        if extract_only:
                            # Extract links and check for important keywords
                            new_links = await self.extract_all_links(html, url)
                            
                            # Add new links to the queue if not already visited
                            for new_url in new_links:
                                if new_url not in self.visited_urls and new_url not in self.urls_to_visit:
                                    self.urls_to_visit.append(new_url)
                                    self.stats['urls_discovered'] += 1
                            
                            # Check if this page contains important keywords
                            if self.contains_important_keywords(html, url):
                                self.important_urls.add(url)
                                self.stats['important_urls_found'] += 1
                        else:
                            # Process content for storage
                            # This only happens in phase 2 for important URLs
                            content = await self.extract_content(html, url)
                            if content:
                                self.extracted_data.append({
                                    'url': url,
                                    'content': content,
                                    'metadata': {
                                        'program': self.program,
                                        'timestamp': datetime.utcnow().isoformat()
                                    }
                                })
                        
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

    async def extract_content(self, html: str, url: str) -> str:
        """Extract and clean content from HTML"""
        try:
            # First try with trafilatura
            extracted_text = trafilatura.extract(
                html,
                include_comments=False,
                no_fallback=False,
                include_tables=True,
                target_language=None,
                deduplicate=True,
                favor_recall=True  # Extract more content
            )
            
            # Fallback to BeautifulSoup
            if not extracted_text or len(extracted_text.strip()) < 100:
                soup = BeautifulSoup(html, 'lxml')
                
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
                    return cleaned_text
            
            return None

        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
            return None

    async def phase1_discover_urls(self, progress_callback=None):
        """
        Phase 1: Discover all URLs and identify important ones
        """
        print(f"Starting Phase 1: URL discovery")
        
        try:
            processed_urls = 0
            
            connector = aiohttp.TCPConnector(limit=self.concurrent_requests, ssl=False)
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Process URLs until queue is empty
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
                    
                    # Process batch (extract only)
                    tasks = [self.crawl_url(session, url, extract_only=True) for url in batch_urls]
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Update progress
                    if progress_callback:
                        progress_callback({
                            'phase': 'discovery',
                            'processed_urls': processed_urls,
                            'total_discovered': self.stats['urls_discovered'],
                            'important_urls': len(self.important_urls)
                        })
                    
                    # Log progress
                    if processed_urls % 100 == 0:
                        print(f"Phase 1 Progress: {processed_urls} URLs processed, {len(self.important_urls)} important URLs found")
                    
                    await asyncio.sleep(0.05)  # Small delay between batches
            
            print(f"Phase 1 Complete: {processed_urls} URLs processed, {len(self.important_urls)} important URLs found")
            
            # Store important URLs in MongoDB
            await self.store_important_urls_in_mongodb()
            
            return {
                'success': True,
                'processed_urls': processed_urls,
                'important_urls': len(self.important_urls)
            }
            
        except Exception as e:
            print(f"Error in Phase 1: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    async def phase2_process_important_urls(self, progress_callback=None):
        """
        Phase 2: Process and store content from important URLs
        """
        print(f"Starting Phase 2: Processing {len(self.important_urls)} important URLs")
        
        try:
            processed_urls = 0
            stored_count = 0
            
            # Reset visited URLs for phase 2
            self.visited_urls.clear()
            self.extracted_data.clear()
            
            # Create queue from important URLs
            urls_queue = deque(self.important_urls)
            
            connector = aiohttp.TCPConnector(limit=self.concurrent_requests, ssl=False)
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Process important URLs
                while urls_queue:
                    # Process URLs in batches
                    batch_urls = []
                    for _ in range(min(self.batch_size, len(urls_queue))):
                        if not urls_queue:
                            break
                        url = urls_queue.popleft()
                        if url not in self.visited_urls:
                            batch_urls.append(url)
                            processed_urls += 1

                    if not batch_urls:
                        continue
                    
                    # Process batch (full content processing)
                    tasks = [self.crawl_url(session, url, extract_only=False) for url in batch_urls]
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Store batch data when we have enough
                    if len(self.extracted_data) >= 5:
                        print(f"Storing batch of {len(self.extracted_data)} documents")
                        namespace = f"uni_{self.university_id}"
                        batch_count = await self.store_batch_in_pinecone(self.extracted_data, namespace)
                        stored_count += batch_count
                        print(f"Stored {batch_count} chunks in Pinecone")
                        self.stats['stored_chunks'] += batch_count
                        self.extracted_data = []  # Clear after storing
                    
                    # Update progress
                    if progress_callback:
                        progress_callback({
                            'phase': 'processing',
                            'processed_urls': processed_urls,
                            'total_urls': len(self.important_urls),
                            'stored_chunks': stored_count
                        })
                    
                    await asyncio.sleep(0.05)  # Small delay between batches
                
                # Store any remaining data
                if self.extracted_data:
                    print(f"Storing final {len(self.extracted_data)} documents")
                    namespace = f"uni_{self.university_id}"
                    batch_count = await self.store_batch_in_pinecone(self.extracted_data, namespace)
                    stored_count += batch_count
                    self.stats['stored_chunks'] += batch_count
                    print(f"Stored final {batch_count} chunks in Pinecone")
            
            print(f"Phase 2 Complete: {processed_urls} important URLs processed, {stored_count} chunks stored")
            
            return {
                'success': True,
                'processed_urls': processed_urls,
                'stored_chunks': stored_count
            }
            
        except Exception as e:
            print(f"Error in Phase 2: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    async def get_embedding_async(self, text: str) -> List[float]:
        """Get embedding for text asynchronously"""
        try:
            # Limit text length for embedding API
            if len(text) > 8000:
                text = text[:8000]
                
            response = await asyncio.to_thread(
                self.client.embeddings.create,
                model="text-embedding-ada-002",
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error getting embedding: {str(e)}")
            return []

    def chunk_text(self, text: str, chunk_size: int = 8000) -> List[str]:
        """Split text into chunks for embedding"""
        chunks = []
        
        # Clean the text
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
        """Store processed data in Pinecone"""
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
                    
                    # Get embedding
                    embedding = await self.get_embedding_async(chunk)
                    if not embedding:
                        continue
                    
                    # Create vector
                    vector = {
                        'id': f"{hashlib.md5(doc['url'].encode()).hexdigest()}_{idx}_{int(time.time())}",
                        'values': embedding,
                        'metadata': {
                            'text': chunk,
                            'url': doc['url'],
                            'program': doc.get('metadata', {}).get('program', ''),
                            'timestamp': datetime.utcnow().isoformat(),
                            'is_important': True  # Mark all as important
                        }
                    }
                    vectors.append(vector)
                    
                    if len(vectors) >= 50:
                        # Store batch
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
                
            return stored_count
            
        except Exception as e:
            print(f"Error in store_batch_in_pinecone: {str(e)}")
            return 0

    async def store_important_urls_in_mongodb(self):
        """Store collected important URLs in MongoDB"""
        try:
            if not self.important_urls:
                logger.info("No important URLs to store in MongoDB")
                return False
            
            # Import MongoDB class here to avoid circular imports
            from services.database import MongoDB
            db = MongoDB(os.getenv('MONGODB_URI'))
            
            # Prepare data to store
            important_urls_data = {
                'university_id': self.university_id,
                'urls': list(self.important_urls),
                'added_at': datetime.utcnow(),
                'last_updated': datetime.utcnow(),
                'count': len(self.important_urls),
                'root_url': f"{self.scheme}://{self.root_domain}",
                'program': self.program,
                'stats': self.stats
            }
            
            # Upsert to MongoDB
            result = db.db.important_urls.update_one(
                {'university_id': self.university_id},
                {'$set': important_urls_data},
                upsert=True
            )
            
            success = result.modified_count > 0 or result.upserted_id is not None
            logger.info(f"Stored {len(self.important_urls)} important URLs in MongoDB: {success}")
            
            # Also update the university record with important URLs count
            db.update_university(self.university_id, {
                'important_urls_count': len(self.important_urls),
                'metadata': {
                    'important_urls_last_updated': datetime.utcnow()
                }
            })
            
            return success
            
        except Exception as e:
            logger.error(f"Error storing important URLs in MongoDB: {str(e)}")
            return False

    @classmethod
    async def process_university(cls, url: str, program: str, university_id: str, progress_callback=None, url_limit: int = None):
        """
        Main processing method with two-phase approach:
        1. Discover all URLs and identify important ones
        2. Process only important URLs and store their content
        
        Note: url_limit parameter is kept for backward compatibility but not used
        """
        try:
            # Create crawler instance
            instance = cls(
                openai_api_key=os.getenv('OPENAI_API_KEY'),
                pinecone_api_key=os.getenv('PINECONE_API_KEY'),
                index_name=os.getenv('INDEX_NAME')
            )
            
            # Initialize crawl
            await instance.initialize_crawl(url, program, university_id)
            namespace = f"uni_{university_id}"
            
            print(f"Processing university {university_id}")
            print(f"Using namespace: {namespace}")
            print(f"Starting two-phase crawling process...")
            
            # If there's a progress callback, send initial status
            if progress_callback:
                progress_callback({
                    'phase': 'initializing',
                    'processed_urls': 0,
                    'total_urls': 0,
                    'important_urls': 0
                })
            
            # Phase 1: Discover all URLs and identify important ones
            phase1_result = await instance.phase1_discover_urls(progress_callback)
            
            if not phase1_result.get('success'):
                return {
                    'success': False,
                    'error': phase1_result.get('error', 'URL discovery failed'),
                    'phase': 'discovery'
                }
                
            # Exit early if no important URLs found
            if len(instance.important_urls) == 0:
                print(f"No important URLs found for {university_id}")
                return {
                    'success': True,
                    'important_urls': 0,
                    'stored_count': 0,
                    'message': 'No important URLs found'
                }
                
            # Phase 2: Process and store content from important URLs
            phase2_result = await instance.phase2_process_important_urls(progress_callback)
            
            if not phase2_result.get('success'):
                return {
                    'success': False,
                    'error': phase2_result.get('error', 'Processing important URLs failed'),
                    'phase': 'processing',
                    'important_urls': len(instance.important_urls)
                }
                
            # Return combined results
            return {
                'success': True,
                'important_urls': len(instance.important_urls),
                'stored_count': instance.stats['stored_chunks'],  # Make sure this matches expected keys
                'pages_crawled': instance.stats['pages_crawled'],
                'urls_discovered': instance.stats['urls_discovered']
            }
            
        except Exception as e:
            print(f"Error processing university {url}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
            
    def __del__(self):
        """Cleanup method to ensure proper resource release"""
        try:
            # Clear collections
            if hasattr(self, 'visited_urls'):
                self.visited_urls.clear()
            if hasattr(self, 'urls_to_visit'):
                self.urls_to_visit.clear()
            if hasattr(self, 'sitemap_urls'):
                self.sitemap_urls.clear()
            if hasattr(self, 'important_urls'):
                self.important_urls.clear()
            if hasattr(self, 'extracted_data'):
                self.extracted_data.clear()
                
        except Exception as e:
            logger.error(f"Error in cleanup: {str(e)}")

    @staticmethod
    async def add_urls_to_process(university_id: str, urls: List[str]):
        """Manually add URLs to process for a university"""
        try:
            # Import MongoDB class here to avoid circular imports
            from services.database import MongoDB
            db = MongoDB(os.getenv('MONGODB_URI'))
            
            # Get existing important URLs
            existing = db.db.important_urls.find_one({'university_id': university_id})
            
            if existing:
                # Add new URLs
                current_urls = set(existing.get('urls', []))
                new_urls = [url for url in urls if url not in current_urls]
                
                # Update MongoDB
                if new_urls:
                    result = db.db.important_urls.update_one(
                        {'university_id': university_id},
                        {
                            '$addToSet': {'urls': {'$each': new_urls}},
                            '$inc': {'count': len(new_urls)},
                            '$set': {'last_updated': datetime.utcnow()}
                        }
                    )
                    
                    return {
                        'success': True,
                        'added_urls': len(new_urls),
                        'total_urls': len(current_urls) + len(new_urls)
                    }
                else:
                    return {
                        'success': True,
                        'added_urls': 0,
                        'message': 'All URLs already exist'
                    }
            else:
                # Create new document
                doc = {
                    'university_id': university_id,
                    'urls': urls,
                    'count': len(urls),
                    'added_at': datetime.utcnow(),
                    'last_updated': datetime.utcnow(),
                    'stats': {
                        'urls_discovered': len(urls),
                        'important_urls_found': len(urls)
                    }
                }
                
                db.db.important_urls.insert_one(doc)
                
                return {
                    'success': True,
                    'added_urls': len(urls)
                }
                
        except Exception as e:
            logger.error(f"Error adding URLs to process: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }