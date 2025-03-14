import asyncio
from collections import deque
import hashlib
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, parse_qs
import re
import logging
from typing import List, Dict, Set, Optional, Callable, Any
from openai import OpenAI
from pinecone import Pinecone
import trafilatura
from datetime import datetime
import time
import os
import json
import xml.etree.ElementTree as ET
import gzip
import random
from io import BytesIO

logger = logging.getLogger(__name__)

# Define important keywords for targeting specific information
IMPORTANT_KEYWORDS = [
    # MS CS specific keywords - primary focus
    'computer science', 'cs program', 'ms cs', 'masters computer science', 
    'cs masters', 'cs degree', 'computer science curriculum', 'cs requirements',
    'masters in cs', 'graduate cs', 'graduate computer science', 'csci',
    'ms in computer science', 'master of science in computer science',
    'computer science department', 'school of computing', 'computer engineering',
    'software engineering', 'cs courses', 'information technology', 'artificial intelligence',
    'machine learning', 'data science', 'masters program', 'graduate program',
    
    # Explicit MS/Graduate focus - to filter out undergrad/PhD content
    'masters degree', 'ms degree', 'master of science', 'graduate admission',
    'graduate requirements', 'graduate application', 'graduate program',
    'masters students', 'ms students', 'graduate students',
    
    # Admissions related
    'admission', 'application', 'deadline', 'requirements', 'apply', 'eligibility',
    'selection', 'entry requirements', 'application process',
    
    # Academics related
    'curriculum', 'course', 'syllabus', 'program structure', 'specialization',
    'concentration', 'credit', 'semester', 'academic requirements',
    
    # Fees and financial aid
    'tuition', 'fee', 'cost', 'financial', 'scholarship', 'funding', 'aid',
    'assistantship', 'payment', 'expense', 'waiver',
    
    # Test scores
    'gre', 'gmat', 'toefl', 'ielts', 'test requirement',
    'english proficiency', 'language requirement', 'score', 'minimum score',
    
    # Deadlines
    'deadline', 'due date', 'application timeline', 'important dates',
    'rolling admission', 'priority deadline',
    
    # Documentation requirements
    'prerequisite', 'document', 'transcript', 'recommendation',
    'resume', 'cv', 'statement of purpose', 'personal statement', 'letter of recommendation',
    
    # Career outcomes specific to MS CS
    'placement', 'career', 'job', 'employment', 'internship', 'recruitment',
    'salary', 'companies', 'employers', 'industry', 'opportunities', 'alumni'
]

# URLs patterns that are likely to contain important information
IMPORTANT_URL_PATTERNS = [
    # MS CS specific patterns - highest priority
    '/ms-cs/', '/mscs/', '/ms/cs/', '/masters/cs/', '/graduate/cs/', 
    '/cs/masters/', '/cs/graduate/', '/cs/ms/', '/cs/admission/',
    '/master/computer-science/', '/masters-computer-science/',
    '/masters-cs/', '/master-of-science/computer-science/',
    '/mcs/', '/msee/', 
    
    # Graduate-specific to filter out undergrad content
    '/masters/', '/ms/', '/graduate/', '/grad/',
    
    # CS department pages
    '/cs/', '/computer-science/', '/computerscience/',
    '/compsci/', '/cse/', '/eecs/', '/cosc/',
    
    # General program pages with CS focus
    '/cs-program/', '/cs-curriculum/', '/cs-courses/',
    '/cs-requirements/', '/cs-admission/',
    
    # Important sections limited to graduate context
    '/graduate/admission/', '/graduate/program/',
    '/graduate/requirements/', '/graduate/application/',
    '/masters/admission/', '/masters/requirements/',
    '/ms/admission/', '/ms/requirements/',
    
    # General important sections (lower priority)
    '/admission/', '/apply/', '/program/', '/degree/', 
    '/requirement/', '/academic/', '/course/', '/curriculum/',
    '/tuition/', '/fee/', '/cost/', '/financial-aid/', '/scholarship/',
    '/international/', '/deadline/', '/application/'
]

class HybridCrawler:
    def __init__(self, openai_api_key: str, pinecone_api_key: str, index_name: str):
        self.visited_urls: Set[str] = set()
        self.urls_to_visit: deque = deque()
        self.sitemap_urls: Set[str] = set()
        self.important_urls: Set[str] = set()  # URLs with important keywords
        self.extracted_data: List[Dict] = []
        self.stored_chunks_hashes = set()  # To track unique chunks
        self.stored_urls = set() 
        self.stored_chunks_hashes = set()  # Track unique chunks by hash
        self.stored_urls = set()  # Track processed URLs with stored content
        self.processed_count = 0  # Track processed URLs count
        self.success_count = 0 
        
        self.client = OpenAI(api_key=openai_api_key)
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.index = self.pc.Index(index_name)
        
        self.chunk_content_map = {} 
        
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
        
        # Default URL filtering patterns - can be overridden
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
        
        # Program configuration
        self.program = ""
        self.root_domain = ""
        self.scheme = ""
        self.university_id = ""
        
        # Enhanced url storage for MS CS
        self.scored_urls = []  # List of {'url': url, 'score': score, metadata: {}}
        
        # Storage limits
        self.max_urls_to_store = 500 # Default to 1,000 URLs
        
        # MongoDB connection (if provided)
        self.db = None

    def set_mongodb(self, db_instance):
        """Set MongoDB instance for storing URLs directly"""
        self.db = db_instance

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
        self.scored_urls.clear()
        
        # Reset stats
        self.stats = {
            'pages_crawled': 0,
            'urls_discovered': 0,
            'important_urls_found': 0,
            'stored_chunks': 0
        }
        
        # Generate program-specific patterns and keywords
        self.generate_program_specific_patterns(program)
        
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

    def generate_program_specific_patterns(self, program: str):
        """Generate program-specific patterns based on the program name"""
        if not program:
            return
            
        # Handle MS CS case specifically
        if program.lower() in ["ms cs", "computer science", "cs"]:
            # Already covered in IMPORTANT_KEYWORDS and IMPORTANT_URL_PATTERNS
            pass
        else:
            # Generate program-specific keywords from the program name
            program_parts = program.lower().split()
            for part in program_parts:
                if len(part) > 2:  # Skip very short words
                    IMPORTANT_KEYWORDS.append(part)
                    
            # Generate program URL patterns
            program_slug = program.lower().replace(' ', '-')
            program_slug_nospace = program.lower().replace(' ', '')
            
            IMPORTANT_URL_PATTERNS.append(f'/{program_slug}/')
            IMPORTANT_URL_PATTERNS.append(f'/{program_slug_nospace}/')
            
            if "master" in program.lower() or "ms" in program.lower():
                IMPORTANT_URL_PATTERNS.append('/masters/')
                IMPORTANT_URL_PATTERNS.append('/graduate/')
                IMPORTANT_URL_PATTERNS.append('/ms/')

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
    
    def is_duplicate_chunk(self, chunk: str, namespace: str) -> bool:
        """
        Check if a similar chunk already exists in Pinecone
        
        Args:
            chunk: Text chunk to check
            namespace: Pinecone namespace
            
        Returns:
            True if a similar chunk already exists
        """
        try:
            # Generate a hash of the chunk for quick comparison
            chunk_hash = hashlib.md5(chunk.encode()).hexdigest()
            
            # Check if this exact hash exists
            try:
                existing_vectors = self.index.fetch(
                    ids=[chunk_hash],
                    namespace=namespace
                )
                
                if existing_vectors and existing_vectors.get('vectors', {}) and chunk_hash in existing_vectors.get('vectors', {}):
                    print(f"  - Found exact duplicate chunk with hash {chunk_hash}")
                    return True
                
                return False
            except Exception as fetch_err:
                print(f"  - Error fetching potential duplicate: {str(fetch_err)}")
                # If there's an error fetching, assume it's not a duplicate to avoid blocking content
                return False
                
        except Exception as e:
            print(f"  - Error in duplicate chunk detection: {str(e)}")
            # If there's any error, assume it's not a duplicate to be safe
            return False
    
    def is_phd_content_without_ms_relevance(self, content: str, url: str) -> bool:
        """
        Enhanced detection of PhD-specific content with no MS relevance
        
        Args:
            content: Extracted page content
            url: URL of the page
            
        Returns:
            True if this is PhD-specific content with no MS relevance
        """
        url_lower = url.lower()
        
        # Check URL for PhD patterns
        phd_url_patterns = [
            '/phd/', '/doctoral/', 'graduating-phds', '/doctorate/', 
            'doctoral-program', 'phd-program', 'phd-student',
            '/ph-d/', '/ph.d./', 'doctor-of-philosophy'
        ]
        
        is_phd_url = any(pattern in url_lower for pattern in phd_url_patterns)
        
        # If URL suggests PhD content
        if is_phd_url:
            content_lower = content.lower()
            
            # Check for MS relevance - must have specific MS terms
            ms_terms = [
                'masters program', 'ms program', 'ms degree', 
                'ms students', 'masters students', 'masters in computer science',
                'ms in computer science', 'mscs', 'ms cs', 'masters requirement',
                'ms requirement', 'masters admission', 'ms application'
            ]
            
            # For each MS term, check if it appears with context
            ms_context_count = 0
            for term in ms_terms:
                if term in content_lower:
                    # Find surrounding context (30 chars before and after)
                    idx = content_lower.find(term)
                    start = max(0, idx - 30)
                    end = min(len(content_lower), idx + len(term) + 30)
                    context = content_lower[start:end]
                    
                    # If context is specifically about the MS program (not just a mention)
                    if any(subst in context for subst in ['program', 'requirement', 'application', 'course', 'curriculum']):
                        ms_context_count += 1
            
            # If we don't have meaningful MS context, filter it out
            if ms_context_count < 2:
                print(f"Detected PhD content with no MS relevance: {url}")
                return True
        
        # Also check content for PhD focus without MS relevance
        # This catches pages that don't have PhD in the URL but are PhD focused
        if 'phd' in url_lower or 'doctoral' in url_lower or 'doctorate' in url_lower:
            content_lower = content.lower()
            
            # Count PhD references
            phd_terms = ['phd', 'doctoral', 'doctorate', 'ph.d.', 'doctor of philosophy']
            phd_term_count = sum(1 for term in phd_terms if term in content_lower)
            
            # Count MS references
            ms_terms = ['masters', 'ms program', 'ms degree', 'master of science']
            ms_term_count = sum(1 for term in ms_terms if term in content_lower)
            
            # If much more PhD content than MS content, filter out
            if phd_term_count > 5 and ms_term_count < 2:
                print(f"Content is predominantly PhD focused with minimal MS content: {url}")
                return True
        
        return False
    
    def is_faculty_profile_with_no_content(self, content: str, url: str) -> bool:
        """
        Enhanced detection of faculty profile pages with no valuable MS CS content
        
        Args:
            content: Extracted page content
            url: URL of the page
                
        Returns:
            True if this is a faculty page with no valuable MS CS content
        """
        url_lower = url.lower()
        
        # Check URL patterns first (faster)
        faculty_url_patterns = [
            '/faculty/', '/people/', '/staff/', '/professors/', 
            '/instructors/', '/directory/', '/profiles/', 'faculty-profiles',
            '/faculty-', '/professor-', '/people/faculty/'
        ]
        is_faculty_url = any(pattern in url_lower for pattern in faculty_url_patterns)
        
        # If URL doesn't match faculty patterns, do a deeper content check
        if not is_faculty_url:
            # Check if there's a person name pattern in the URL
            name_pattern = re.compile(r'/[a-z\-]+\-[a-z\-]+$')
            if name_pattern.search(url_lower):
                # Could be a person, check content
                is_faculty_url = True
        
        # If URL suggests faculty page, check content
        if is_faculty_url:
            content_lower = content.lower()
            
            # Common navigation patterns to remove before checking content
            nav_patterns = [
                'skip to main content', 'main menu', 'search search', 'back', 
                'faculty profiles', 'in memoriam', 'staff directory',
                'about', 'research', 'graduate', 'undergraduate', 'diversity', 
                'community', 'giving', 'main navigation', 'search', 'menu toggle'
            ]
            
            # Clean content for better assessment
            cleaned_content = content_lower
            for pattern in nav_patterns:
                cleaned_content = cleaned_content.replace(pattern, '')
            
            # Faculty indicators
            faculty_indicators = [
                'professor', 'assistant professor', 'associate professor', 
                'ph.d.', 'research interests', 'publications', 'biography',
                'education', 'cv', 'curriculum vitae', 'research area', 
                'research group', 'lab', 'laboratory', 'email:', 'website'
            ]
            
            # Count faculty indicators in cleaned content
            faculty_indicator_count = sum(1 for ind in faculty_indicators if ind in cleaned_content)
            is_faculty_content = faculty_indicator_count >= 2  # Lowered from 3 to 2
            
            # Check for MS CS relevance - must have these terms
            ms_cs_terms = [
                'graduate program', 'masters program', 'ms program', 'master of science',
                'graduate advisor', 'graduate student', 'masters student', 'ms student',
                'graduate course', 'masters course', 'ms course', 'graduate degree',
                'masters degree', 'ms degree', 'graduate curriculum', 'graduate advisor',
                'ms in computer science', 'master in computer science', 
                'application deadline', 'admission requirements'
            ]
            
            ms_cs_term_count = sum(1 for term in ms_cs_terms if term in cleaned_content)
            
            # Check if content is too short after cleaning
            is_too_short = len(cleaned_content.split()) < 100
            
            # Website with faculty name but no substantial content
            if is_faculty_url and (is_faculty_content or is_too_short):
                # If it lacks MS CS terms, it's likely just a faculty bio
                if ms_cs_term_count < 2:
                    print(f"Detected faculty profile with insufficient MS CS content: {url}")
                    print(f"- Found {ms_cs_term_count} MS CS terms out of {len(ms_cs_terms)}")
                    return True
                
                # If it has MS CS terms but it's just a small amount of content, verify it has substance
                if ms_cs_term_count >= 2 and len(cleaned_content.split()) < 200:
                    # Check if content has substance or just mentions terms
                    # Look for longer MS CS context
                    ms_context_found = False
                    for term in ms_cs_terms:
                        if term in cleaned_content:
                            # Find context around term (50 chars before and after)
                            idx = cleaned_content.find(term)
                            start = max(0, idx - 50)
                            end = min(len(cleaned_content), idx + len(term) + 50)
                            context = cleaned_content[start:end]
                            
                            # If context contains substantial info, not just a mention
                            if any(subst in context for subst in ['information', 'details', 'program', 'course', 'application']):
                                ms_context_found = True
                                break
                    
                    # If no substantial MS context found, filter out
                    if not ms_context_found:
                        print(f"Faculty page has MS terms but no substantial MS content: {url}")
                        return True
        
        # More general menu/navigation only content check
        if self.is_navigation_only_content(content):
            print(f"Detected navigation-only content: {url}")
            return True
        
        return False

    def is_navigation_only_content(self, content: str) -> bool:
        """
        Detect if content is just navigation elements with no actual content
        
        Args:
            content: The extracted text content
        
        Returns:
            True if content is just navigation
        """
        # Convert to lowercase for case-insensitive matching
        content_lower = content.lower()
        
        # Common navigation words and phrases
        nav_terms = [
            'skip to', 'menu', 'navigation', 'search', 'main', 'content',
            'footer', 'header', 'about', 'contact', 'home', 'back',
            'previous', 'next', 'undergraduate', 'graduate'
        ]
        
        # Count words in content
        words = content_lower.split()
        total_words = len(words)
        
        # If content is very short, it's likely just navigation
        if total_words < 50:
            return True
        
        # Count navigation words
        nav_word_count = sum(1 for word in words if word in nav_terms)
        
        # If more than 30% of words are navigation terms, it's likely just navigation
        if nav_word_count / total_words > 0.3:
            return True
        
        # Check for characteristic navigation patterns
        nav_patterns = [
            'skip to main content', 'main menu', 'search search',
            'main navigation', 'menu toggle', 'back to top',
            'faculty profiles', 'in memoriam', 'staff directory'
        ]
        
        # If more than 3 nav patterns are found, it's likely just navigation
        pattern_count = sum(1 for pattern in nav_patterns if pattern in content_lower)
        if pattern_count > 3:
            return True
        
        return False

    def normalize_url(self, url: str) -> str:
        """
        Enhanced URL normalization to prevent duplicates and handle http/https consistently
        
        Args:
            url: URL to normalize
                
        Returns:
            Normalized URL without fragments, trailing slashes, and common session parameters
        """
        try:
            parsed = urlparse(url)
            
            # Split query parameters
            if parsed.query:
                query_params = parse_qs(parsed.query)
                
                # Remove session-specific parameters that create duplicates
                params_to_remove = ['session', 'sid', 'token', 'timestamp', 't', 'ts', 
                                'utm_source', 'utm_medium', 'utm_campaign', 'utm_content',
                                'fbclid', 'gclid', 'msclkid', 'ref', '_ga']
                
                for param in params_to_remove:
                    if param in query_params:
                        del query_params[param]
                
                # Rebuild query string
                query_string = urlencode(query_params, doseq=True) if query_params else ''
            else:
                query_string = ''
            
            # Normalize path (remove trailing slash)
            path = parsed.path
            if path.endswith('/') and len(path) > 1:
                path = path[:-1]
            
            # For faculty pages, extract name and standardize
            if '/people/' in path.lower() or '/faculty/' in path.lower() or 'faculty-profiles' in path.lower():
                # Try to get just the faculty name part
                path_parts = path.split('/')
                if len(path_parts) > 2:
                    # Reconstruct just with people/faculty + name
                    for i, part in enumerate(path_parts):
                        if part.lower() in ['people', 'faculty', 'faculty-profiles'] and i < len(path_parts) - 1:
                            path = f"/{part}/{path_parts[i+1]}"
                            break
            
            # Always use https for consistency
            scheme = 'https'
            
            # Reconstruct URL without fragment and with cleaned query
            normalized = urlunparse((scheme, parsed.netloc, path, 
                                parsed.params, query_string, ''))
            
            return normalized
        except Exception as e:
            print(f"Error normalizing URL {url}: {str(e)}")
            # If parsing fails for any reason, return the original URL
            return url

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
                                
                                # Get priority and lastmod if available
                                priority_elem = url.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}priority')
                                priority = float(priority_elem.text) if priority_elem is not None and priority_elem.text else 0.5
                                
                                lastmod_elem = url.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')
                                lastmod = lastmod_elem.text if lastmod_elem is not None else None
                                
                                # Filter URLs by domain and patterns
                                if self.is_valid_url(page_url):
                                    self.sitemap_urls.add(page_url)
                                    
                                    # Calculate relevance score
                                    relevance_score = self.calculate_url_relevance(page_url)
                                    
                                    # Store URL with metadata
                                    self.add_scored_url(page_url, relevance_score, {
                                        'source': 'sitemap',
                                        'priority': priority,
                                        'lastmod': lastmod,
                                    })
                                    
                                    # Check URL path for important keywords
                                    url_lower = page_url.lower()
                                    if any(keyword in url_lower for keyword in IMPORTANT_KEYWORDS) or \
                                    any(pattern in url_lower for pattern in IMPORTANT_URL_PATTERNS):
                                        self.important_urls.add(page_url)
                                        self.stats['important_urls_found'] += 1
                                    
                                    count += 1
                        
                        logger.info(f"Added {count} URLs from sitemap {sitemap_url}")
                        self.stats['urls_discovered'] += count
                    except ET.ParseError:
                        logger.warning(f"XML parsing error in sitemap: {sitemap_url}")
        
        except Exception as e:
            logger.error(f"Error processing sitemap {sitemap_url}: {str(e)}")
    
    def calculate_url_relevance(self, url: str) -> float:
        """Calculate relevance score for a URL with improved filtering for postdoctoral content and faculty pages"""
        url_lower = url.lower()
        score = 0.0
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        # Debug the URL being scored
        print(f"Scoring URL: {url}")
        
        # Immediately filter out postdoctoral content
        postdoc_patterns = [
            '/postdoc', 'postdoctoral', 'post-doc', 'post-doctoral', 
            'postdoctorate', '/post-doctoral-', '/post-doc-'
        ]
        if any(pattern in url_lower for pattern in postdoc_patterns):
            print(f"  Postdoctoral content detected, scoring 0.0")
            return 0.0
        
        # MS CS specific boost - highest priority
        ms_cs_patterns = [
            '/ms-cs/', '/mscs/', '/ms/cs/', '/masters/cs/', 
            '/cs/masters/', '/cs/graduate/', '/cs/ms/', 
            '/master/computer-science/', '/masters-computer-science/'
        ]
        for pattern in ms_cs_patterns:
            if pattern in path:
                score += 0.4  # Higher boost for explicit MS CS URLs
                print(f"  MS CS pattern match: {pattern}, +0.4 -> {score}")
                break
        
        # Check for program indicators with emphasis on masters
        program_indicators = {
            'computer science': 0.2,
            'cs': 0.15,
            'mscs': 0.3,
            'ms-cs': 0.3, 
            'masters cs': 0.3,
            'master of science in computer science': 0.4,
            'ms in computer science': 0.4,
            'masters in computer science': 0.4,
            'mcs': 0.25,
            'masters program': 0.2,
            'graduate program': 0.15
        }
        
        for indicator, boost in program_indicators.items():
            if indicator in url_lower:
                score += boost
                print(f"  Program indicator match: {indicator}, +{boost} -> {score}")
                break
        
        # Check for important content indicators
        content_indicators = {
            'admission': 0.25,         # Increased from 0.2
            'requirement': 0.25,       # Increased from 0.2
            'application': 0.25,       # Increased from 0.2
            'tuition': 0.20,           # Increased from 0.15
            'deadline': 0.20,          # Increased from 0.15
            'curriculum': 0.20,        # Increased from 0.15
            'course': 0.15,            # Increased from 0.1
            'syllabus': 0.15,          # Increased from 0.1
            'prerequisite': 0.20,      # New
            'graduate-course': 0.25,   # New
            'program-requirement': 0.25 # New
        }
        
        for indicator, boost in content_indicators.items():
            if indicator in url_lower:
                score += boost
                print(f"  Content indicator match: {indicator}, +{boost} -> {score}")
                break
                
        # Check for important URL patterns
        for pattern in IMPORTANT_URL_PATTERNS:
            if pattern in path:
                # Higher scores for more specific patterns
                if pattern.startswith('/ms') or pattern.startswith('/masters') or 'graduate' in pattern:
                    score += 0.25
                    print(f"  Important URL pattern (high): {pattern}, +0.25 -> {score}")
                else:
                    score += 0.15
                    print(f"  Important URL pattern: {pattern}, +0.15 -> {score}")
                break
                
        # Check for general keywords with weighted boosts
        keyword_matches = []
        for keyword in IMPORTANT_KEYWORDS:
            if keyword in url_lower:
                keyword_matches.append(keyword)
                # Higher boosts for MS CS specific keywords
                if keyword in ['ms cs', 'masters computer science', 'master of science in computer science']:
                    score += 0.05
                else:
                    score += 0.02  # Smaller increment for general keywords
                
        # Penalize URLs likely to be about undergraduate programs
        undergrad_indicators = ['undergraduate', 'undergrad', 'bachelor', 'bs-cs', 'bscs']
        for indicator in undergrad_indicators:
            if indicator in url_lower:
                score -= 0.3  # Significant penalty
                print(f"  Undergrad indicator penalty: {indicator}, -0.3 -> {score}")
                break
                
        # Heavier penalty for postdoctoral URLs (although they should be filtered earlier)
        postdoc_indicators = ['postdoc', 'post-doc', 'postdoctoral', 'post-doctoral']
        for indicator in postdoc_indicators:
            if indicator in url_lower:
                score -= 0.5  # Heavy penalty
                print(f"  Postdoc indicator penalty: {indicator}, -0.5 -> {score}")
                break
                
        # Penalize URLs likely to be about PhD programs unless they also mention MS
        phd_indicators = ['/phd', 'doctorate', 'doctoral']
        ms_indicators = ['/ms', '/masters', 'master of science']
        
        has_phd = any(indicator in url_lower for indicator in phd_indicators)
        has_ms = any(indicator in url_lower for indicator in ms_indicators)
        
        if has_phd and not has_ms:
            score -= 0.25  # Penalty for PhD-only content
            print(f"  PhD-only penalty: -0.25 -> {score}")
                
        # Boost score for URLs with file paths related to graduate programs
        path_components = path.split('/')
        graduate_related = ['graduate', 'masters', 'ms', 'master', 'admission']
        for component in path_components:
            if component in graduate_related:
                score += 0.15
                print(f"  Path component boost: {component}, +0.15 -> {score}")
                break
                
        # Faculty page scoring - more selective approach
        faculty_indicators = ['faculty', 'professor', 'instructor', 'teacher', 'staff', 'people', 'profiles']
        cs_indicators = ['computer science', 'cs', 'computing', 'informatics', 'software engineering']
        is_faculty_page = any(indicator in url_lower for indicator in faculty_indicators)
        
        # If it's a faculty page, look for CS relevance but be more selective
        if is_faculty_page:
            print(f"  Faculty page detected")
            
            # Check if it's a faculty directory/listing (these are useful)
            is_directory = any(pattern in url_lower for pattern in ['/directory', '/listing', '/faculty/', '/people/'])
            
            # Check if it's a specific faculty profile
            is_specific_profile = re.search(r'/[^/]+/[^/]+\.html$', path) or re.search(r'/profile/[^/]+$', path)
            
            if is_directory and any(indicator in url_lower for indicator in cs_indicators):
                # Faculty directories are valuable
                score = max(score, 0.5)  # Give it at least 0.5
                print(f"  CS faculty directory boost: score set to at least 0.5")
            elif is_specific_profile:
                # Individual faculty profiles are less valuable unless they have specific MS keywords
                has_ms_keywords = any(kw in url_lower for kw in ['graduate', 'masters', 'ms-', 'advisor'])
                if has_ms_keywords and any(indicator in url_lower for indicator in cs_indicators):
                    score = max(score, 0.4)  # Set minimum score for relevant MS CS faculty
                    print(f"  MS CS relevant faculty profile: score set to at least 0.4")
                else:
                    # Otherwise cap the score for individual faculty profiles
                    score = min(score, 0.3)
                    print(f"  Generic faculty profile: score capped at 0.3")
            
        # Check for recency indicators in URL
        recency_patterns = [r'/20\d\d/', r'fall-20\d\d', r'spring-20\d\d', r'-20\d\d']
        for pattern in recency_patterns:
            if re.search(pattern, url_lower):
                score += 0.1
                print(f"  Recency pattern match: {pattern}, +0.1 -> {score}")
                break
                
        # Ensure score is at least 0 (in case of penalties)
        score = max(0.0, score)
        
        # Cap the score at 1.0
        score = min(1.0, score)
        
        print(f"  Final score: {score}")
        
        # Store matched keywords for reference
        metadata = {
            'keyword_matches': keyword_matches,
            'score': score
        }
        
        return score
    
    @staticmethod
    async def add_urls_to_process(university_id: str, urls: List[str], program: str = "MS CS"):
        """
        Manually add URLs to process for a university
        
        Args:
            university_id: University identifier
            urls: List of URLs to add
            program: Program name (defaults to MS CS)
            
        Returns:
            Status dictionary
        """
        try:
            # Import MongoDB class here to avoid circular imports
            from services.database import MongoDB
            db = MongoDB(os.getenv('MONGODB_URI'))
            
            # Try the new URLs collection first
            url_collection = db.db.university_urls
            existing = url_collection.find_one({'university_id': university_id, 'program': program})
            
            if existing:
                # Get existing URLs
                existing_urls = set(u['url'] for u in existing.get('discovered_urls', []))
                
                # Score new URLs
                new_url_objects = []
                for url in urls:
                    if url not in existing_urls:
                        # Calculate a basic relevance score based on URL patterns
                        score = 0.7  # Default score for manually added URLs
                        url_lower = url.lower()
                        
                        # Boost score for URLs with important patterns
                        for pattern in IMPORTANT_URL_PATTERNS:
                            if pattern in url_lower:
                                score = 0.9
                                break
                                
                        # Create URL object
                        new_url_objects.append({
                            'url': url,
                            'score': score,
                            'metadata': {
                                'source': 'manual',
                                'added_at': datetime.utcnow().isoformat()
                            },
                            'discovered_at': datetime.utcnow().isoformat()
                        })
                
                # If we have new URLs to add
                if new_url_objects:
                    # Update MongoDB - append to discovered_urls array
                    result = url_collection.update_one(
                        {'university_id': university_id, 'program': program},
                        {
                            '$push': {'discovered_urls': {'$each': new_url_objects}},
                            '$inc': {'total_urls': len(new_url_objects)},
                            '$set': {'last_updated': datetime.utcnow()}
                        }
                    )
                    
                    return {
                        'success': True,
                        'added_urls': len(new_url_objects),
                        'collection': 'university_urls'
                    }
                else:
                    return {
                        'success': True,
                        'added_urls': 0,
                        'message': 'All URLs already exist',
                        'collection': 'university_urls'
                    }
            else:
                # Try legacy collection as fallback
                legacy_collection = db.db.important_urls
                existing_legacy = legacy_collection.find_one({'university_id': university_id})
                
                if existing_legacy:
                    # Add new URLs to legacy format
                    current_urls = set(existing_legacy.get('urls', []))
                    new_urls = [url for url in urls if url not in current_urls]
                    
                    # Update MongoDB
                    if new_urls:
                        result = legacy_collection.update_one(
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
                            'total_urls': len(current_urls) + len(new_urls),
                            'collection': 'important_urls'
                        }
                    else:
                        return {
                            'success': True,
                            'added_urls': 0,
                            'message': 'All URLs already exist',
                            'collection': 'important_urls'
                        }
                else:
                    # Create new document in new format
                    url_objects = [{
                        'url': url,
                        'score': 0.7,  # Default score for manually added URLs
                        'metadata': {
                            'source': 'manual',
                            'added_at': datetime.utcnow().isoformat()
                        },
                        'discovered_at': datetime.utcnow().isoformat()
                    } for url in urls]
                    
                    doc = {
                        'university_id': university_id,
                        'program': program,
                        'discovered_urls': url_objects,
                        'total_urls': len(urls),
                        'last_updated': datetime.utcnow(),
                        'stats': {
                            'urls_discovered': len(urls)
                        }
                    }
                    
                    url_collection.insert_one(doc)
                    
                    return {
                        'success': True,
                        'added_urls': len(urls),
                        'collection': 'university_urls'
                    }
                
        except Exception as e:
            logger.error(f"Error adding URLs to process: {str(e)}")
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
            if hasattr(self, 'scored_urls'):
                self.scored_urls.clear()
                
        except Exception as e:
            logger.error(f"Error in cleanup: {str(e)}")
            
    @staticmethod
    async def process_urls_for_content(urls: List[str], university_id: str, program: str = "MS CS", 
                                      namespace: str = None, openai_api_key: str = None, 
                                      pinecone_api_key: str = None, index_name: str = None):
        """
        Process a specific list of URLs for content extraction and storage
        
        Args:
            urls: List of URLs to process
            university_id: University ID
            program: Program name
            namespace: Custom namespace (defaults to uni_{university_id})
            openai_api_key: OpenAI API key (if not provided, uses environment variable)
            pinecone_api_key: Pinecone API key (if not provided, uses environment variable)
            index_name: Pinecone index name (if not provided, uses environment variable)
            
        Returns:
            Processing results
        """
        try:
            # Use provided keys or get from environment
            openai_key = openai_api_key or os.getenv('OPENAI_API_KEY')
            pinecone_key = pinecone_api_key or os.getenv('PINECONE_API_KEY')
            idx_name = index_name or os.getenv('INDEX_NAME')
            
            # Create crawler instance
            crawler = HybridCrawler(openai_key, pinecone_key, idx_name)
            
            # Set up namespace
            ns = namespace or f"uni_{university_id}"
            crawler.program = program
            crawler.university_id = university_id
            
            # Create domain info from first URL
            if urls:
                parsed = urlparse(urls[0])
                crawler.root_domain = parsed.netloc
                crawler.scheme = parsed.scheme
            
            # Process URLs
            print(f"Processing {len(urls)} specific URLs for content extraction")
            
            # Create batch data structure
            batch_data = []
            
            # Create HTTP session
            connector = aiohttp.TCPConnector(limit=crawler.concurrent_requests, ssl=False)
            timeout = aiohttp.ClientTimeout(total=crawler.timeout)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                tasks = [crawler.crawl_url(session, url, extract_only=False) for url in urls]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process successful results
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        print(f"Error processing URL {urls[i]}: {str(result)}")
                        continue
                        
                    if not result.get('success') or not result.get('content'):
                        print(f"No content extracted from URL {urls[i]}")
                        continue
                        
                    # Add to batch data
                    batch_data.append({
                        'url': urls[i],
                        'content': result['content'],
                        'metadata': {
                            'program': program,
                            'timestamp': datetime.utcnow().isoformat(),
                            'title': result.get('title', ''),
                            'keywords_matched': result.get('keywords_matched', []),
                            'source': 'direct_processing'
                        }
                    })
                    
            # Store content in Pinecone
            if batch_data:
                print(f"Storing content from {len(batch_data)} URLs in Pinecone")
                stored_count = await crawler.store_batch_in_pinecone(batch_data, ns)
                
                return {
                    'success': True,
                    'processed_urls': len(batch_data),
                    'stored_chunks': stored_count,
                    'namespace': ns
                }
            else:
                return {
                    'success': True,
                    'processed_urls': 0,
                    'stored_chunks': 0,
                    'message': 'No content was extracted from the provided URLs'
                }
                
        except Exception as e:
            print(f"Error processing URLs for content: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_top_urls(self, count: int = 500, min_score: float = 0.0) -> List[Dict]:
        """
        Get the top N most relevant URLs above a minimum score with normalization
        
        Args:
            count: Number of URLs to return (increased default to 500)
            min_score: Minimum relevance score (0.0 to 1.0)
            
        Returns:
            List of URL objects with metadata
        """
        # Filter by minimum score
        filtered_urls = [url for url in self.scored_urls if url['score'] >= min_score]
        
        # Sort by score (highest first)
        sorted_urls = sorted(filtered_urls, key=lambda x: x['score'], reverse=True)
        
        # Normalize URLs and remove duplicates
        normalized_urls = set()
        deduplicated_urls = []
        
        for url_obj in sorted_urls:
            url = url_obj['url']
            normalized_url = self.normalize_url(url)
            
            # Skip if this normalized URL is already in our list
            if normalized_url in normalized_urls:
                continue
                
            # Add the normalized URL to our tracking set
            normalized_urls.add(normalized_url)
            
            # Add to deduplicated list
            deduplicated_urls.append({
                'url': url,
                'normalized_url': normalized_url,
                'score': url_obj['score'],
                'metadata': url_obj.get('metadata', {})
            })
            
            # Stop when we reach the desired count
            if len(deduplicated_urls) >= count:
                break
        
        print(f"Selected {len(deduplicated_urls)} top URLs after deduplication (from {len(sorted_urls)} total)")
        
        return deduplicated_urls
        
    @classmethod
    async def process_university(cls, url: str, program: str, university_id: str, progress_callback=None, 
                            url_limit: int = None, db_instance=None, max_urls: int = 500,
                            process_content: bool = True, min_relevance_score: float = 0.5,
                            content_batch_size: int = 5, max_content_urls: int = 2000,
                            discovery_target: int = 5000):
        """
        Main processing method with enhanced approach focused on MS CS:
        1. Discover a large pool of URLs (5000 by default)
        2. Score and select the top 500 most relevant URLs 
        3. Store the top relevant URLs in MongoDB
        4. Process content from high-scoring URLs and store in Pinecone
        
        Args:
            url: Root URL for the university
            program: Program name (e.g., "MS CS")
            university_id: Unique identifier for the university
            progress_callback: Optional callback for progress updates
            url_limit: Max URLs to process (kept for backward compatibility)
            db_instance: MongoDB instance for direct storage
            max_urls: Maximum URLs to collect and process (defaults to 500)
            process_content: Whether to extract content and store in Pinecone
            min_relevance_score: Minimum score threshold for content processing
            content_batch_size: Batch size for content processing
            max_content_urls: Maximum URLs to process for content
            discovery_target: Target number of URLs to discover before selection (default: 5000)
        """
        try:
            print(f"\n==== Starting process_university for {university_id} ====")
            print(f"URL: {url}")
            print(f"Program: {program}")
            print(f"Max URLs to store: {max_urls}")
            print(f"Discovery target: {discovery_target}")
            print(f"Process content: {process_content}")
            print(f"Min relevance score: {min_relevance_score}")
            print(f"Content batch size: {content_batch_size}")
            
            # Create crawler instance
            instance = cls(
                openai_api_key=os.getenv('OPENAI_API_KEY'),
                pinecone_api_key=os.getenv('PINECONE_API_KEY'),
                index_name=os.getenv('INDEX_NAME')
            )
            
            # Set MongoDB instance if provided
            if db_instance:
                instance.db = db_instance
                
            # Set max URLs limit
            instance.max_urls_to_store = max_urls
            
            # Initialize crawl
            print(f"Initializing crawl...")
            await instance.initialize_crawl(url, program or "MS CS", university_id)
            namespace = f"uni_{university_id}"
            
            print(f"Using namespace: {namespace}")
            print(f"Will discover up to {discovery_target} URLs, then select top {instance.max_urls_to_store} relevant URLs")
            
            # If there's a progress callback, send initial status
            if progress_callback:
                progress_callback({
                    'phase': 'initializing',
                    'processed_urls': 0,
                    'total_urls': 0,
                    'important_urls': 0,
                    'max_urls': instance.max_urls_to_store,
                    'discovery_target': discovery_target
                })
            
            # Phase 1: Discover many URLs and identify important ones with scoring
            print(f"\n==== Starting Phase 1: URL Discovery (target: {discovery_target} URLs) ====")
            phase1_result = await instance.phase1_discover_urls(progress_callback)
            
            if not phase1_result.get('success'):
                print(f"Phase 1 failed: {phase1_result.get('error', 'Unknown error')}")
                return {
                    'success': False,
                    'error': phase1_result.get('error', 'URL discovery failed'),
                    'phase': 'discovery'
                }
            
            print(f"\n==== Phase 1 Complete ====")
            print(f"Discovered {phase1_result.get('total_discovered', 0)} URLs")
            print(f"Selected top {len(instance.scored_urls)} URLs with relevance scoring")
            print(f"Found {len(instance.important_urls)} important URLs")
            
            # For backward compatibility, check if we need to do the old two-phase processing
            if url_limit is not None and not process_content:
                print(f"\n==== Running in backward compatibility mode ====")
                # Exit early if no important URLs found
                if len(instance.important_urls) == 0:
                    print(f"No important URLs found for {university_id}")
                    return {
                        'success': True,
                        'important_urls': 0,
                        'stored_count': 0,
                        'message': 'No important URLs for content processing found, but URLs were collected and stored'
                    }
                    
                # Phase 2: Process and store content from important URLs (if needed)
                print(f"\n==== Starting Phase 2 (Legacy): Processing Important URLs ====")
                phase2_result = await instance.phase2_process_important_urls(progress_callback)
                
                if not phase2_result.get('success'):
                    print(f"Phase 2 failed: {phase2_result.get('error', 'Unknown error')}")
                    return {
                        'success': False,
                        'error': phase2_result.get('error', 'Processing important URLs failed'),
                        'phase': 'processing',
                        'important_urls': len(instance.important_urls),
                        'scored_urls': len(instance.scored_urls)
                    }
                
                # Return combined results with content processing (backward compatibility)
                print(f"\n==== Processing Complete (Legacy Mode) ====")
                return {
                    'success': True,
                    'important_urls': len(instance.important_urls),
                    'scored_urls': len(instance.scored_urls),
                    'stored_count': instance.stats['stored_chunks'], 
                    'pages_crawled': instance.stats['pages_crawled'],
                    'urls_discovered': instance.stats['urls_discovered']
                }
            
            # Phase 2: Enhanced content processing (if enabled)
            elif process_content and phase1_result.get('scored_urls', 0) > 0:
                print(f"\n==== Starting Phase 2: Enhanced Content Processing ====")
                print(f"Will process high-scoring URLs for content extraction and embedding")
                
                # Process content from discovered URLs
                phase2_result = await instance.process_discovered_urls(
                    batch_size=content_batch_size,
                    max_urls=max_urls,  # Use max_urls instead of max_content_urls to ensure we only process the top ones
                    min_score=min_relevance_score,
                    progress_callback=progress_callback
                )
                
                if not phase2_result.get('success'):
                    print(f"Phase 2 failed: {phase2_result.get('error', 'Unknown error')}")
                    return {
                        'success': False,
                        'error': phase2_result.get('error', 'Content processing failed'),
                        'phase': 'content_processing',
                        'scored_urls': len(instance.scored_urls)
                    }
                    
                # Return combined results with content processing
                print(f"\n==== Processing Complete (Enhanced Mode) ====")
                print(f"Stored {phase2_result.get('stored_chunks', 0)} chunks from {phase2_result.get('processed', 0)} URLs")
                
                return {
                    'success': True,
                    'scored_urls': len(instance.scored_urls),
                    'processed_content_urls': phase2_result.get('processed', 0),
                    'stored_chunks': phase2_result.get('stored_chunks', 0),
                    'failed_urls': phase2_result.get('failed_urls', 0),
                    'pages_crawled': instance.stats['pages_crawled'],
                    'urls_discovered': instance.stats['urls_discovered'], 
                    'total_discovered': phase1_result.get('total_discovered', 0)
                }
            else:
                # If content processing is disabled, just return URL discovery results
                print(f"\n==== Processing Complete (URL Discovery Only) ====")
                return {
                    'success': True,
                    'scored_urls': len(instance.scored_urls),
                    'pages_crawled': instance.stats['pages_crawled'],
                    'urls_discovered': instance.stats['urls_discovered'],
                    'total_discovered': phase1_result.get('total_discovered', 0),
                    'message': 'URL collection completed, content processing skipped'
                }
            
        except Exception as e:
            import traceback
            print(f"Error processing university {url}: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def get_embedding_async(self, text: str) -> List[float]:
        """Get embedding for text asynchronously with improved error handling"""
        try:
            # Ensure text is not empty
            if not text or len(text.strip()) < 10:
                print(f"Text too short for embedding: {len(text) if text else 0} chars")
                return []
                
            # Limit text length for embedding API
            if len(text) > 8000:
                print(f"Truncating text from {len(text)} to 8000 chars for embedding")
                text = text[:8000]
                
            # Add retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = await asyncio.to_thread(
                        self.client.embeddings.create,
                        model="text-embedding-ada-002",
                        input=text
                    )
                    
                    # Validate response
                    if not response or not hasattr(response, 'data') or not response.data:
                        print(f"Invalid embedding response: {response}")
                        if attempt < max_retries - 1:
                            print(f"Retrying embedding generation (attempt {attempt+1}/{max_retries})")
                            await asyncio.sleep(2)
                            continue
                        return []
                        
                    # Check for empty embedding
                    embedding = response.data[0].embedding
                    if not embedding:
                        print(f"Received empty embedding")
                        if attempt < max_retries - 1:
                            print(f"Retrying embedding generation (attempt {attempt+1}/{max_retries})")
                            await asyncio.sleep(2)
                            continue
                        return []
                    
                    # Validate embedding dimensions
                    if len(embedding) != 1536:  # Standard size for text-embedding-ada-002
                        print(f"Warning: Unexpected embedding dimension: {len(embedding)}")
                    
                    return embedding
                    
                except Exception as e:
                    print(f"Error in embedding generation (attempt {attempt+1}/{max_retries}): {str(e)}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
                    else:
                        logger.error(f"Failed to get embedding after {max_retries} attempts: {str(e)}")
                        return []
            
            return []  # Should never reach here, but just in case
            
        except Exception as e:
            logger.error(f"Unexpected error in get_embedding_async: {str(e)}")
            return []


    def chunk_text(self, text: str, chunk_size: int = 8000) -> List[str]:
        """
        Split text into chunks with increased chunk size of 8000 chars
        
        Args:
            text: Text to chunk
            chunk_size: Maximum chunk size (default: 8000)
        
        Returns:
            List of text chunks
        """
        chunks = []
        
        # Clean the text
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove common navigation and useless content
        nav_patterns = [
            'skip to main content', 'main menu', 'search search',
            'main navigation', 'menu toggle', 'back to top',
            'faculty profiles', 'in memoriam', 'staff directory',
            'home page', 'jump to navigation', 'back to', 'click to'
        ]
        
        for pattern in nav_patterns:
            text = text.replace(pattern, '')
        
        # Split into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            sentence_size = len(sentence)
            
            # Skip very long sentences by splitting them further
            if sentence_size > chunk_size:
                # Split long sentence into smaller parts
                parts = [sentence[i:i+chunk_size//2] for i in range(0, len(sentence), chunk_size//2)]
                for part in parts:
                    chunks.append(part)
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
        """
        Store processed data in Pinecone with improved deduplication 
        
        Args:
            batch_data: List of documents to store
            namespace: Pinecone namespace to store data in
            
        Returns:
            Number of chunks successfully stored in Pinecone
        """
        try:
            print(f"\n{'='*50}")
            print(f"STORING CONTENT IN PINECONE")
            print(f"Namespace: {namespace}")
            print(f"Total documents: {len(batch_data)}")
            print(f"{'='*50}")
            
            # RELAXED QUALITY FILTERING - Only filter out completely irrelevant content
            docs_to_process = []
            for doc in batch_data:
                url = doc.get('url', 'unknown')
                
                # Skip already processed URLs
                if hasattr(self, 'stored_urls') and url in self.stored_urls:
                    print(f"Skipping already processed URL: {url}")
                    continue
                
                # Basic check for empty content
                content = doc.get('content', '')
                if not content or len(content) < 100:
                    print(f"Skipping document with no content: {url}")
                    continue
                    
                docs_to_process.append(doc)
            
            if not docs_to_process:
                print("WARNING: No documents passed filtering.")
                return 0
            
            print(f"Processing {len(docs_to_process)} documents for storage")
            
            # Tracking for results
            stored_count = 0
            failed_count = 0
            vectors_to_store = []
            
            # Process documents
            for doc_index, doc in enumerate(docs_to_process):
                url = doc.get('url', 'unknown')
                content = doc.get('content', '')
                
                try:
                    print(f"\nProcessing document {doc_index+1}/{len(docs_to_process)}: {url}")
                    
                    # Create chunks
                    chunks = self.chunk_text(content, chunk_size=8000)
                    
                    # Filter out very short chunks but with a low threshold
                    valid_chunks = [chunk for chunk in chunks if len(chunk.split()) >= 30]
                    
                    print(f"- Created {len(chunks)} chunks, {len(valid_chunks)} valid (>= 30 words)")
                    
                    if not valid_chunks:
                        print(f"- No valid chunks created, skipping document")
                        continue
                    
                    # Process each valid chunk
                    chunk_count = 0
                    for chunk_idx, chunk in enumerate(valid_chunks):
                        try:
                            # Simple cleaning to remove most obvious navigation elements
                            nav_patterns = [
                                'skip to main content', 'main menu', 'search search',
                                'main navigation', 'menu toggle', 'back to top'
                            ]
                            clean_chunk = chunk
                            for pattern in nav_patterns:
                                clean_chunk = clean_chunk.replace(pattern, '')
                            
                            # Generate hash for the chunk
                            chunk_hash = hashlib.md5(clean_chunk.encode()).hexdigest()
                            
                            # Create a composite ID from URL and chunk hash
                            vector_id = f"{hashlib.md5(url.encode()).hexdigest()}_{chunk_idx}"
                            
                            # Get embedding with basic error handling
                            try:
                                embedding = await self.get_embedding_async(clean_chunk)
                                if not embedding:
                                    print(f"- Failed to get embedding for chunk {chunk_idx}, skipping")
                                    continue
                            except Exception as embed_error:
                                print(f"- Error getting embedding: {str(embed_error)}")
                                continue
                            
                            # Get metadata from the document
                            metadata = doc.get('metadata', {}).copy()
                            
                            # Create the vector - use a consistent ID format
                            vector = {
                                'id': vector_id,
                                'values': embedding,
                                'metadata': {
                                    'text': clean_chunk,
                                    'url': url,
                                    'program': metadata.get('program', ''),
                                    'title': metadata.get('title', ''),
                                    'timestamp': datetime.utcnow().isoformat(),
                                    'keywords': metadata.get('keywords_matched', []),
                                    'content_quality': metadata.get('content_quality', 0),
                                    'original_url': metadata.get('original_url', url),
                                    'chunk_hash': chunk_hash
                                }
                            }
                            
                            # Add to vectors batch
                            vectors_to_store.append(vector)
                            chunk_count += 1
                            
                            # Store in smaller batches to avoid API limits
                            if len(vectors_to_store) >= 10:
                                try:
                                    success = await self._store_vectors_with_retry(vectors_to_store, namespace)
                                    if success:
                                        print(f"- Successfully stored batch of {len(vectors_to_store)} vectors")
                                        stored_count += len(vectors_to_store)
                                    else:
                                        print(f"- Failed to store batch of {len(vectors_to_store)} vectors")
                                except Exception as batch_error:
                                    print(f"- Error storing batch: {str(batch_error)}")
                                
                                # Clear vector batch whether successful or not
                                vectors_to_store = []
                                
                                # Small delay between batches
                                await asyncio.sleep(0.5)
                        
                        except Exception as chunk_error:
                            print(f"- Error processing chunk {chunk_idx}: {str(chunk_error)}")
                            continue
                    
                    print(f"- Processed {chunk_count} chunks from document")
                    
                    # Mark URL as processed if we stored any chunks
                    if chunk_count > 0 and hasattr(self, 'stored_urls'):
                        self.stored_urls.add(url)
                
                except Exception as doc_error:
                    print(f"Error processing document {url}: {str(doc_error)}")
                    failed_count += 1
                    continue
            
            # Store any remaining vectors
            if vectors_to_store:
                try:
                    success = await self._store_vectors_with_retry(vectors_to_store, namespace)
                    if success:
                        print(f"Successfully stored final batch of {len(vectors_to_store)} vectors")
                        stored_count += len(vectors_to_store)
                    else:
                        print(f"Failed to store final batch of {len(vectors_to_store)} vectors")
                except Exception as final_error:
                    print(f"Error storing final vector batch: {str(final_error)}")
            
            print(f"\n{'='*50}")
            print(f"STORAGE SUMMARY")
            print(f"- Total vectors stored: {stored_count}")
            print(f"- Failed documents: {failed_count}")
            print(f"{'='*50}")
            
            return stored_count
        
        except Exception as e:
            print(f"Fatal error in store_batch_in_pinecone: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return 0
    
    def clean_chunk_text(self, chunk: str) -> str:
        """
        Clean chunk text by removing navigation elements and common patterns
        
        Args:
            chunk: Text chunk to clean
            
        Returns:
            Cleaned text chunk
        """
        # Clean up navigation patterns
        nav_patterns = [
            'skip to main content', 'main menu', 'search search',
            'main navigation', 'menu toggle', 'back to top',
            'faculty profiles', 'in memoriam', 'staff directory',
            'about', 'research', 'graduate', 'undergraduate', 'diversity', 
            'community', 'giving', 'search', 'main', 'menu', 'navigation',
            'back', 'previous', 'next', 'click to', 'jump to'
        ]
        
        cleaned = chunk
        
        # Remove common navigation patterns
        for pattern in nav_patterns:
            cleaned = cleaned.replace(pattern, '')
        
        # Remove common web page phrases
        web_patterns = [
            'home page', 'website', 'click here', 'learn more',
            'read more', 'contact us', 'email us', 'call us',
            'sign up', 'log in', 'register', 'subscribe',
            'follow us', 'share this', 'print', 'download'
        ]
        
        for pattern in web_patterns:
            cleaned = cleaned.replace(pattern, '')
        
        # Remove URLs
        cleaned = re.sub(r'https?://\S+', '', cleaned)
        
        # Remove excessive whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned

    async def _store_vectors_with_retry(self, vectors: List[Dict], namespace: str, max_retries: int = 3) -> bool:
        """
        Simplified helper method to store vectors with better reliability
        
        Args:
            vectors: List of vectors to store
            namespace: Pinecone namespace
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if successful, False otherwise
        """
        for attempt in range(max_retries):
            try:
                print(f"Storing batch of {len(vectors)} vectors (attempt {attempt+1}/{max_retries})")
                
                # Store in smaller batches always
                batch_size = 10
                success = True
                
                # Process in small batches
                for i in range(0, len(vectors), batch_size):
                    batch = vectors[i:i+batch_size]
                    try:
                        self.index.upsert(vectors=batch, namespace=namespace)
                        print(f"Successfully stored batch {i//batch_size + 1}")
                    except Exception as batch_error:
                        print(f"Error with batch {i//batch_size + 1}: {str(batch_error)}")
                        success = False
                    
                    # Add delay between batches
                    await asyncio.sleep(0.5)
                
                return success
                
            except Exception as e:
                print(f"Error upserting vectors (attempt {attempt+1}): {str(e)}")
                
                # Wait before retrying
                await asyncio.sleep(2 * (attempt + 1))
                
                # Last attempt failed
                if attempt == max_retries - 1:
                    return False
        
        return False
            
    async def process_discovered_urls(self, batch_size=5, max_urls=500, min_score=0.5, progress_callback=None):
        """
        Process URLs by extracting content and storing in Pinecone with stricter filtering
        
        Args:
            batch_size: Number of URLs to process in each batch
            max_urls: Maximum number of URLs to process (default: 500)
            min_score: Minimum relevance score threshold (increased to 0.5)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary with processing results
        """
        print(f"\n{'='*50}")
        print(f"Starting content extraction and vector storage for up to {max_urls} URLs")
        print(f"{'='*50}")
        
        try:
            # Get all URLs sorted by score (highest to lowest)
            all_scored_urls = sorted(self.scored_urls, key=lambda x: x['score'], reverse=True)
            
            # Remove URLs with score below threshold
            all_scored_urls = [url for url in all_scored_urls if url['score'] >= min_score]
            
            if not all_scored_urls:
                print("No URLs with sufficient relevance score to process")
                return {
                    'success': True,
                    'processed': 0,
                    'stored_chunks': 0,
                    'message': 'No URLs with sufficient relevance score'
                }
            
            # Create namespace for vector storage
            namespace = f"uni_{self.university_id}"
            
            # Initialize tracking
            processed_count = 0  # Total URLs attempted
            success_count = 0    # URLs successfully processed
            stored_chunks = 0
            failed_urls = []
            
            # Reset tracking sets for clean run
            self.visited_urls = set()
            self.stored_chunks_hashes = set()
            self.stored_url_chunk_combinations = set()
            self.stored_urls = set()
            self.chunk_content_map = {}
            
            # Process URLs in batches until we reach max_urls successfully processed URLs
            batch_index = 0
            remaining_urls = all_scored_urls.copy()  # Work with a copy to remove processed URLs
            
            print(f"Total candidate URLs: {len(all_scored_urls)}")
            print(f"Will attempt to process URLs until {max_urls} are successful")
            
            while remaining_urls and processed_count < max_urls * 2 and success_count < max_urls:  # Allow up to 2x max_urls attempts
                # Get next batch (always get fresh batch from top of remaining URLs)
                batch_urls = remaining_urls[:batch_size]
                remaining_urls = remaining_urls[batch_size:]  # Remove this batch from remaining
                
                print(f"\n--- Processing batch {batch_index + 1}: {len(batch_urls)} URLs ---")
                batch_index += 1
                
                # Create a fresh session for each batch
                connector = aiohttp.TCPConnector(
                    limit=min(3, len(batch_urls)),
                    force_close=True,
                    ssl=False
                )
                timeout = aiohttp.ClientTimeout(total=120)
                
                batch_data = []
                batch_success = 0
                batch_failures = []
                
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={'Connection': 'close'}
                ) as session:
                    # Process URLs in the batch
                    for url_item in batch_urls:
                        url = url_item['url']
                        normalized_url = self.normalize_url(url)
                        
                        # Skip if already processed
                        if normalized_url in self.visited_urls:
                            print(f"URL already visited, skipping: {url}")
                            continue
                        
                        print(f"\nProcessing URL: {url}")
                        print(f"  Relevance score: {url_item['score']}")
                        
                        # Add a delay between requests
                        await asyncio.sleep(1.0)
                        
                        # Track this as an attempt regardless of outcome
                        processed_count += 1
                        
                        # Process the URL with increased quality threshold (4.0)
                        try:
                            result = await self.crawl_url(session, url, extract_only=False, quality_threshold=4.0)
                            
                            if result.get('success', False) and result.get('content'):
                                print(f"✓ Successfully processed {url}")
                                print(f"  Content length: {len(result['content'])} chars")
                                print(f"  Quality score: {result.get('content_quality', 0)}/10")
                                
                                batch_success += 1
                                success_count += 1
                                
                                # Add to batch data
                                batch_data.append({
                                    'url': normalized_url,
                                    'original_url': url,
                                    'content': result['content'],
                                    'metadata': {
                                        'program': self.program,
                                        'timestamp': datetime.utcnow().isoformat(),
                                        'title': result.get('title', ''),
                                        'keywords_matched': result.get('keywords_matched', []),
                                        'relevance_score': url_item['score'],
                                        'content_quality': result.get('content_quality', 0),
                                        'original_url': url
                                    }
                                })
                                
                                # Mark as visited
                                self.visited_urls.add(normalized_url)
                            else:
                                error_msg = result.get('error_details', 'Unknown error')
                                print(f"✗ Failed to process {url}: {error_msg}")
                                batch_failures.append({
                                    'url': url,
                                    'normalized_url': normalized_url,
                                    'error': error_msg
                                })
                        except Exception as e:
                            print(f"Error processing {url}: {str(e)}")
                            batch_failures.append({
                                'url': url,
                                'normalized_url': normalized_url,
                                'error': str(e)
                            })
                    
                    # Store batch in Pinecone if we have data
                    if batch_data:
                        print(f"\nStoring batch of {len(batch_data)} URLs in Pinecone...")
                        try:
                            batch_chunks = await self.store_batch_in_pinecone(batch_data, namespace)
                            stored_chunks += batch_chunks
                            print(f"Successfully stored {batch_chunks} chunks from {len(batch_data)} URLs")
                        except Exception as store_error:
                            print(f"Error storing batch in Pinecone: {str(store_error)}")
                    
                    # Add batch failures to overall failures
                    failed_urls.extend(batch_failures)
                    
                    # Update progress
                    if progress_callback:
                        progress_callback({
                            'phase': 'content_processing',
                            'processed_urls': processed_count,
                            'successful_urls': success_count,
                            'total_urls': max_urls,
                            'stored_chunks': stored_chunks,
                            'failed_urls': len(failed_urls)
                        })
                    
                    # Print progress
                    print(f"\nProgress: {success_count}/{max_urls} URLs successfully processed")
                    print(f"Total attempts: {processed_count} URLs")
                    print(f"Total stored chunks: {stored_chunks}")
                    
                    # Break if we've reached our target
                    if success_count >= max_urls:
                        print(f"Reached target of {max_urls} successfully processed URLs")
                        break
                    
                    # Longer delay between batches
                    await asyncio.sleep(2.0)
            
            # Update MongoDB with only the processed URLs (max 500)
            await self.store_processed_urls_in_mongodb()
            
            print(f"\n{'='*50}")
            print(f"Content processing complete: {success_count}/{processed_count} URLs processed")
            print(f"Stored {stored_chunks} chunks from {success_count} URLs")
            print(f"Failed URLs: {len(failed_urls)}")
            print(f"{'='*50}")
            
            # Update database with processing results
            if self.db:
                processing_result = {
                    'processed_urls_count': processed_count,
                    'successful_urls_count': success_count,
                    'stored_chunks': stored_chunks,
                    'failed_urls_count': len(failed_urls),
                    'content_processed_at': datetime.utcnow()
                }
                
                # Update university record
                self.db.update_university(self.university_id, {
                    'content_processing': processing_result,
                    'stored_chunks': stored_chunks,
                    'last_updated': datetime.utcnow()
                })
            
            return {
                'success': True,
                'processed': processed_count,
                'successful': success_count,
                'stored_chunks': stored_chunks,
                'failed_urls': len(failed_urls)
            }
        
        except Exception as e:
            import traceback
            print(f"Fatal error in process_discovered_urls: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def store_processed_urls_in_mongodb(self, final=True):
        """
        Store only the processed URLs in MongoDB with university name
        """
        try:
            # Get the URLs that were actually processed
            processed_urls = list(self.stored_urls)
            print(f"Storing {len(processed_urls)} processed URLs in MongoDB")
            
            # Find these URLs in our scored_urls list to get original metadata
            processed_url_objects = []
            for url in processed_urls:
                for scored_url in self.scored_urls:
                    if self.normalize_url(scored_url['url']) == url:
                        processed_url_objects.append(scored_url)
                        break
            
            # If we have fewer than self.max_urls_to_store, add highest scored unprocessed URLs
            if len(processed_url_objects) < self.max_urls_to_store:
                needed = self.max_urls_to_store - len(processed_url_objects)
                print(f"Adding {needed} more high-scoring URLs to reach {self.max_urls_to_store} total")
                
                # Sort remaining URLs by score
                remaining_urls = [u for u in self.scored_urls if self.normalize_url(u['url']) not in self.stored_urls]
                remaining_urls.sort(key=lambda x: x['score'], reverse=True)
                
                # Add top needed URLs
                for url_obj in remaining_urls[:needed]:
                    processed_url_objects.append(url_obj)
                    
                    if len(processed_url_objects) >= self.max_urls_to_store:
                        break
            
            # Use provided DB connection or initialize new one
            db_instance = self.db
            if not db_instance:
                # Import MongoDB class here to avoid circular imports
                from services.database import MongoDB
                db_instance = MongoDB(os.getenv('MONGODB_URI'))
            
            # Try to get university name
            university_name = ""
            try:
                # Query the university record to get the name
                university_record = db_instance.get_university_by_id(self.university_id)
                if university_record and 'name' in university_record:
                    university_name = university_record['name']
                elif university_record and 'university_name' in university_record:
                    university_name = university_record['university_name']
            except Exception as e:
                print(f"Error getting university name: {str(e)}")
            
            # Prepare data structure with university name
            urls_data = {
                'university_id': self.university_id,
                'university_name': university_name,  # Include university name
                'program': self.program or 'MS Computer Science',
                'root_url': f"{self.scheme}://{self.root_domain}",
                'discovered_urls': processed_url_objects[:self.max_urls_to_store],  # Store exactly max_urls_to_store URLs
                'total_urls': len(self.scored_urls),
                'last_updated': datetime.utcnow(),
                'stats': {
                    'pages_crawled': self.stats['pages_crawled'],
                    'urls_discovered': self.stats['urls_discovered'],
                    'important_urls_found': self.stats['important_urls_found'],
                    'processed_urls_count': len(self.stored_urls),
                    'stored_chunks_count': len(self.stored_chunks_hashes)
                }
            }
            
            # Add more metadata for final storage
            if final:
                urls_data['stats']['stored_urls'] = list(self.stored_urls)
                urls_data['stats']['processed_at'] = datetime.utcnow().isoformat()
                urls_data['crawl_completed_at'] = datetime.utcnow()
            
            # Store in university_urls collection
            collection = db_instance.db.university_urls
            
            # Upsert to MongoDB
            result = collection.update_one(
                {'university_id': self.university_id, 'program': urls_data['program']},
                {'$set': urls_data},
                upsert=True
            )
            
            success = result.modified_count > 0 or result.upserted_id is not None
            logger.info(f"Stored {len(processed_url_objects[:self.max_urls_to_store])} URLs in MongoDB: {success}")
            
            # Update the university record with URLs count
            db_instance.update_university(self.university_id, {
                'discovered_urls_count': len(processed_url_objects[:self.max_urls_to_store]),
                'stored_urls_count': len(self.stored_urls),
                'stored_chunks_count': len(self.stored_chunks_hashes),
                'metadata': {
                    'urls_last_updated': datetime.utcnow(),
                    'program': self.program
                }
            })
            
            return success
            
        except Exception as e:
            logger.error(f"Error storing processed URLs in MongoDB: {str(e)}")
            return False

    def is_valid_url(self, url: str) -> bool:
        """Validate URL for crawling with enhanced filtering for postdoctoral content"""
        try:
            # Normalize URL first to strip fragments
            normalized_url = self.normalize_url(url)
            
            parsed = urlparse(normalized_url)
            
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
            if self.ignore_patterns.search(normalized_url):
                return False
                
            # Skip URLs that are too long
            if len(normalized_url) > 500:
                return False
            
            path_lower = parsed.path.lower()
            
            # Filter out postdoctoral content
            postdoc_patterns = [
                '/postdoc', 'postdoctoral', 'post-doc', 'post-doctoral', 
                'postdoctorate', '/post-doctoral-', '/post-doc-'
            ]
            if any(pattern in path_lower for pattern in postdoc_patterns):
                return False
            
            # Filter out explicit undergraduate content
            undergrad_patterns = [
                '/undergraduate', '/undergrad', '/bachelor', '/bs/', 
                '/bsc/', '/b.s.', '/b.sc', '/ug/', '/bscs/', '/bsee/',
                'freshman', 'sophomore', 'junior', 'senior'
            ]
            if any(pattern in path_lower for pattern in undergrad_patterns):
                # Only allow if it also contains graduate/masters keywords
                grad_patterns = ['/ms/', '/masters/', '/graduate/', '/grad/']
                if not any(pattern in path_lower for pattern in grad_patterns):
                    return False
            
            # Filter out explicit PhD content without masters focus
            phd_patterns = [
                '/phd/', '/doctorate', '/doctoral', '/ph.d', 
                '/philosophiae', '/doctor-of-philosophy'
            ]
            if any(pattern in path_lower for pattern in phd_patterns):
                # Only allow if it also contains graduate/masters keywords
                ms_patterns = ['/ms/', '/masters/', '/master-of-science/']
                if not any(pattern in path_lower for pattern in ms_patterns):
                    return False
            
            return True

        except Exception as e:
            print(f"URL validation error for {url}: {str(e)}")
            return False
        
    async def extract_all_links(self, html: str, base_url: str) -> Set[str]:
        """Extract all links from HTML content with URL normalization"""
        links = set()
        normalized_base = self.normalize_url(base_url)
        
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
                    normalized_url = self.normalize_url(url)
                    if self.is_valid_url(normalized_url) and normalized_url != normalized_base:
                        links.add(normalized_url)
            
            # Then process all content areas
            content_areas = soup.find_all(['main', 'article', 'div', 'section'])
            
            for area in content_areas:
                for link in area.find_all('a', href=True):
                    href = link.get('href')
                    if not href:
                        continue
                        
                    # Convert to absolute URL
                    url = urljoin(base_url, href)
                    normalized_url = self.normalize_url(url)
                    
                    # Validate URL and avoid self-reference
                    if self.is_valid_url(normalized_url) and normalized_url != normalized_base:
                        links.add(normalized_url)
            
            # Finally, check all remaining links in the page
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if not href:
                    continue
                
                url = urljoin(base_url, href)
                normalized_url = self.normalize_url(url)
                if self.is_valid_url(normalized_url) and normalized_url != normalized_base:
                    links.add(normalized_url)
            
            # Debug print for URLs with fragments
            fragment_links = [link for link in links if '#' in link]
            if fragment_links:
                print(f"Warning: Found {len(fragment_links)} links with fragments after normalization!")
                for link in fragment_links[:5]:  # Show a few examples
                    print(f"  Fragment link after normalization: {link}")
                    
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

    def extract_content_keywords(self, content: str) -> List[str]:
        """Extract important keywords found in the content"""
        if not content:
            return []
            
        content_lower = content.lower()
        matched_keywords = []
        
        for keyword in IMPORTANT_KEYWORDS:
            if keyword in content_lower:
                matched_keywords.append(keyword)
                
        return matched_keywords
    
    async def crawl_url(self, session: aiohttp.ClientSession, url: str, extract_only: bool = True, quality_threshold: float = 3.0) -> Dict:
        """
        Crawl URL with better faculty profile detection but without using is_navigation_only_content
        
        Args:
            session: HTTP session
            url: URL to crawl
            extract_only: If True, only extract links without processing content
            quality_threshold: Quality threshold (reduced to 3.0)
            
        Returns:
            Dictionary with crawl results
        """
        # Normalize URL
        normalized_url = self.normalize_url(url)
        
        result = {
            'url': url,
            'normalized_url': normalized_url,
            'success': False,
            'new_links': [],
            'content': None,
            'is_important': False,
            'keywords_matched': [],
            'error_details': None,
            'content_quality': 0
        }
        
        # Check if already visited
        if normalized_url in self.visited_urls:
            if not extract_only:
                print(f"URL already visited, skipping: {url}")
            return result
        
        # Debug logging
        if not extract_only:
            print(f"\n=== Processing content for URL: {url} ===")
            print(f"  Normalized URL: {normalized_url}")
                
        for attempt in range(self.max_retries):
            try:
                # Add timeout
                timeout = aiohttp.ClientTimeout(total=90 if not extract_only else self.timeout)
                
                if not extract_only:
                    print(f"  Attempt {attempt+1}/{self.max_retries}: Fetching URL")
                
                # Randomize user agent
                user_agents = [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
                ]
                
                headers = {
                    'User-Agent': random.choice(user_agents),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'no-cache'
                }
                
                try:
                    # Use original URL for request
                    async with session.get(
                        url, 
                        headers=headers,
                        timeout=timeout,
                        ssl=False,
                        allow_redirects=True
                    ) as response:
                        if response.status == 200:
                            # Check if it's HTML content
                            content_type = response.headers.get('Content-Type', '')
                            if 'text/html' not in content_type and 'application/xhtml+xml' not in content_type:
                                if not extract_only:
                                    print(f"  Skipping: Not HTML content (Content-Type: {content_type})")
                                result['error_details'] = f"Not HTML content: {content_type}"
                                return result
                                
                            # Get HTML content
                            html = await response.text()
                            
                            if not html:
                                if not extract_only:
                                    print(f"  Error: Empty HTML content")
                                result['error_details'] = "Empty HTML content"
                                return result
                            
                            # Extract page title
                            title = ""
                            soup = BeautifulSoup(html, 'lxml')
                            if soup.title and soup.title.string:
                                title = soup.title.string.strip()
                            
                            result['title'] = title
                            
                            if extract_only:
                                # Extract links and check for important keywords
                                new_links = await self.extract_all_links(html, url)
                                result['new_links'] = list(new_links)
                                
                                # Check if this page contains important keywords
                                is_important = self.contains_important_keywords(html, url)
                                result['is_important'] = is_important
                                
                                if is_important:
                                    # Store the normalized URL in important_urls
                                    if normalized_url not in self.important_urls:
                                        self.important_urls.add(normalized_url)
                                        self.stats['important_urls_found'] += 1
                                    
                                    # Extract matched keywords
                                    result['keywords_matched'] = self.extract_content_keywords(html)
                            else:
                                # Content extraction
                                
                                # 1. Remove navigation and menu elements
                                for nav_element in soup.find_all(['nav', 'header', 'footer', 'aside']):
                                    nav_element.decompose()
                                
                                # 2. Remove common menu items by class or id
                                for menu_class in ['menu', 'navigation', 'navbar', 'sidebar', 'footer']:
                                    for element in soup.find_all(class_=lambda c: c and menu_class in c.lower()):
                                        element.decompose()
                                    for element in soup.find_all(id=lambda i: i and menu_class in i.lower()):
                                        element.decompose()
                                
                                # 3. Try content extraction with trafilatura
                                content = None
                                try:
                                    import trafilatura
                                    content = trafilatura.extract(
                                        html,
                                        include_comments=False,
                                        no_fallback=False,
                                        include_tables=True,
                                        favor_recall=True
                                    )
                                except Exception as e:
                                    print(f"  Trafilatura error: {str(e)}")
                                
                                # If trafilatura fails, try container detection
                                if not content or len(content) < 200:
                                    for selector in ['main', 'article', '#content', '.content', '#main-content', '.main-content', 'section']:
                                        if content and len(content) > 200:
                                            break
                                            
                                        try:
                                            elements = soup.select(selector)
                                            if elements:
                                                container_text = elements[0].get_text(separator=' ', strip=True)
                                                if len(container_text) > 200:
                                                    content = container_text
                                                    break
                                        except Exception:
                                            continue
                                    
                                    # If still no content, try paragraphs
                                    if not content or len(content) < 200:
                                        paragraphs = []
                                        for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'li']):
                                            if tag:
                                                text = tag.get_text(strip=True)
                                                if len(text) > 20:
                                                    paragraphs.append(text)
                                        
                                        if paragraphs:
                                            para_text = ' '.join(paragraphs)
                                            if len(para_text) > 200:
                                                content = para_text
                                    
                                    # Last resort: body text
                                    if not content or len(content) < 200:
                                        if soup.body:
                                            body_text = soup.body.get_text(separator=' ', strip=True)
                                            if len(body_text) > 200:
                                                content = body_text
                                
                                # If we have content, process it further
                                if content and len(content) > 100:  # Reduced threshold
                                    # Clean content
                                    content = re.sub(r'\s+', ' ', content).strip()
                                    
                                    # Remove common navigation text
                                    nav_patterns = [
                                        'skip to main content', 'main menu', 'search search',
                                        'main navigation', 'menu toggle', 'back to top'
                                    ]
                                    
                                    for pattern in nav_patterns:
                                        content = content.replace(pattern, '')
                                    
                                    # Skip if content is too short after cleaning
                                    if len(content) < 100:
                                        if not extract_only:
                                            print(f"  Content too short after cleaning: {len(content)} chars")
                                        result['error_details'] = "Content too short after cleaning"
                                        return result
                                    
                                    # Check for faculty profile without MS CS relevance
                                    if self.is_faculty_profile_with_no_content(content, url):
                                        if not extract_only:
                                            print(f"  Faculty profile with no MS CS relevance")
                                        result['error_details'] = "Faculty profile with no MS CS relevance"
                                        self.visited_urls.add(normalized_url)  # Mark as visited
                                        return result
                                    
                                    # Check for PhD content without MS relevance
                                    if self.is_phd_content_without_ms_relevance(content, url):
                                        if not extract_only:
                                            print(f"  PhD content with no MS relevance")
                                        result['error_details'] = "PhD content with no MS relevance"
                                        self.visited_urls.add(normalized_url)  # Mark as visited
                                        return result
                                    
                                    # Calculate content quality with basic criteria
                                    keywords = self.extract_content_keywords(content)
                                    quality_score = 5.0  # Just use a default score of 5.0 to ensure content is stored
                                    result['content_quality'] = quality_score
                                    
                                    # Skip quality check to make sure content gets stored
                                    # But still have error check for very low quality
                                    if quality_score < 2.0:
                                        if not extract_only:
                                            print(f"  Content quality very low: {quality_score}/10")
                                        result['error_details'] = f"Very low content quality: {quality_score}/10"
                                        return result
                                    
                                    # Save processed content
                                    result['content'] = content
                                    result['keywords_matched'] = keywords
                                else:
                                    if not extract_only:
                                        print(f"  No meaningful content extracted")
                                    result['error_details'] = "No meaningful content extracted"
                                    return result
                            
                            # Mark as successful
                            result['success'] = True
                            
                            # Update stats
                            self.stats['pages_crawled'] += 1
                            
                            # Mark normalized URL as visited
                            self.visited_urls.add(normalized_url)
                            
                            return result
                        else:
                            # Handle non-200 status codes
                            if not extract_only:
                                print(f"  Failed with HTTP status {response.status}")
                            result['error_details'] = f"HTTP status {response.status}"
                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(2 * (attempt + 1))
                            else:
                                return result
                            
                except aiohttp.ClientError as client_err:
                    if not extract_only:
                        print(f"  Client error on attempt {attempt+1}: {str(client_err)}")
                    result['error_details'] = f"Client error: {str(client_err)}"
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 * (attempt + 1))
                    else:
                        return result
                except asyncio.TimeoutError:
                    if not extract_only:
                        print(f"  Timeout on attempt {attempt+1}")
                    result['error_details'] = f"Timeout on attempt {attempt+1}"
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 * (attempt + 1))
                    else:
                        return result
                except Exception as e:
                    if not extract_only:
                        print(f"  Error on attempt {attempt+1}: {str(e)}")
                    result['error_details'] = f"Error: {str(e)}"
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 * (attempt + 1))
                    else:
                        return result
            
            except Exception as e:
                if not extract_only:
                    print(f"  Fatal error on attempt {attempt+1}: {str(e)}")
                result['error_details'] = f"Fatal error: {str(e)}"
                return result
        
        return result

    def assess_content_quality(self, content: str, keywords: List[str], url: str) -> float:
        """
        Very basic content quality assessment that ensures content gets stored
        
        Args:
            content: Extracted text content
            keywords: Matched keywords found in content
            url: URL being processed
                
        Returns:
            Quality score from 0-10 (mostly returning high scores)
        """
        # Give a high default score to ensure content gets stored
        score = 7.0
        
        # Adjust based on very basic checks
        
        # Content length
        if len(content) < 500:
            score -= 2.0
        
        # Keyword presence
        if not keywords or len(keywords) < 2:
            score -= 1.0
        
        # URL relevance
        url_lower = url.lower()
        relevant_terms = ['graduate', 'admission', 'program', 'computer', 'science', 'course']
        if not any(term in url_lower for term in relevant_terms):
            score -= 1.0
        
        # Ensure score is between 0-10
        score = max(0, min(10, score))
        
        return score

    def add_scored_url(self, url: str, score: float, metadata: Dict = None):
        """Add a URL with its relevance score and metadata to the scored URLs list"""
        entry = {
            'url': url,
            'score': score,
            'metadata': metadata or {},
            'discovered_at': datetime.utcnow().isoformat()
        }
        
        self.scored_urls.append(entry)
        
        # Sort scored_urls by relevance (higher scores first)
        self.scored_urls.sort(key=lambda x: x['score'], reverse=True)
        
        # Keep only the top URLs if we exceed our limit
        if len(self.scored_urls) > self.max_urls_to_store * 1.2:  # Keep 20% buffer
            self.scored_urls = self.scored_urls[:self.max_urls_to_store]

    async def process_content(self, html: str, url: str) -> Dict:
        """Process content and extract text from HTML with enhanced MS CS filtering"""
        try:
            print(f"Processing content from {url}")
            
            # Try trafilatura first for better extraction
            try:
                import trafilatura
                print("Using trafilatura for extraction...")
                extracted_text = trafilatura.extract(
                    html,
                    include_comments=False,
                    no_fallback=False,
                    include_tables=True,
                    favor_recall=True
                )
                if extracted_text and len(extracted_text) > 100:
                    print(f"Trafilatura extracted {len(extracted_text)} characters")
                    # Check if content is related to MS CS
                    keywords = self.extract_content_keywords(extracted_text)
                    ms_cs_keywords = [kw for kw in keywords 
                                     if kw in ['computer science', 'ms cs', 'masters computer science', 
                                               'graduate program', 'graduate cs', 'master of science']]
                    
                    # Only return if content has MS CS related keywords or URL indicates MS CS
                    path = urlparse(url).path.lower()
                    is_mscs_url = any(pattern in path for pattern in ['/ms/', '/masters/', '/graduate/cs/', '/cs/masters/'])
                    
                    if ms_cs_keywords or is_mscs_url:
                        print(f"Found MS CS relevant content with keywords: {ms_cs_keywords}")
                        # Clean content
                        extracted_text = re.sub(r'\s+', ' ', extracted_text).strip()
                        extracted_text = ''.join(c for c in extracted_text if c.isprintable() or c == ' ')
                        
                        return {
                            'content': extracted_text,
                            'method': 'trafilatura',
                            'keywords': keywords,
                            'ms_cs_keywords': ms_cs_keywords
                        }
                    else:
                        print(f"Content may not be MS CS specific, falling back to other methods")
            except Exception as e:
                print(f"Error using trafilatura: {str(e)}")
                
            # Fallback to BeautifulSoup
            print("Falling back to BeautifulSoup content extraction")
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract main content from containers
            print("Checking main content containers...")
            for container in ['article', 'main', '.content', '#content', '.main-content', 'section']:
                try:
                    elements = soup.select(container)
                    if elements:
                        print(f"Found {len(elements)} elements with selector: {container}")
                        text = elements[0].get_text(separator=' ', strip=True)
                        if len(text) > 100:
                            # Check for MS CS relevance
                            keywords = self.extract_content_keywords(text)
                            ms_cs_keywords = [kw for kw in keywords 
                                             if kw in ['computer science', 'ms cs', 'masters computer science', 
                                                      'graduate program', 'graduate cs', 'master of science']]
                            
                            # Clean content
                            text = re.sub(r'\s+', ' ', text).strip()
                            text = ''.join(c for c in text if c.isprintable() or c == ' ')
                            
                            print(f"Container {container} has {len(ms_cs_keywords)} MS CS keywords")
                            return {
                                'content': text,
                                'method': f'container-{container}',
                                'keywords': keywords,
                                'ms_cs_keywords': ms_cs_keywords
                            }
                except Exception as container_error:
                    print(f"Error with container {container}: {str(container_error)}")
            
            # If no container found, extract from paragraphs
            print("Extracting from paragraphs...")
            paragraphs = []
            for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'li']):
                try:
                    text = tag.get_text(strip=True)
                    if len(text) > 20:  # Skip short snippets
                        paragraphs.append(text)
                except Exception:
                    continue
                    
            print(f"Found {len(paragraphs)} meaningful paragraphs")
            if paragraphs:
                content = ' '.join(paragraphs)
                keywords = self.extract_content_keywords(content)
                ms_cs_keywords = [kw for kw in keywords 
                                 if kw in ['computer science', 'ms cs', 'masters computer science', 
                                          'graduate program', 'graduate cs', 'master of science']]
                
                # Clean content
                content = re.sub(r'\s+', ' ', content).strip()
                content = ''.join(c for c in content if c.isprintable() or c == ' ')
                
                print(f"Paragraph method found {len(ms_cs_keywords)} MS CS keywords")
                return {
                    'content': content,
                    'method': 'paragraphs',
                    'keywords': keywords,
                    'ms_cs_keywords': ms_cs_keywords
                }
                
            # Last resort: body text
            print("Trying body text extraction as last resort...")
            if soup.body:
                body_text = soup.body.get_text(separator=' ', strip=True)
                if len(body_text) > 100:
                    keywords = self.extract_content_keywords(body_text)
                    ms_cs_keywords = [kw for kw in keywords 
                                     if kw in ['computer science', 'ms cs', 'masters computer science', 
                                              'graduate program', 'graduate cs', 'master of science']]
                    
                    # Clean content
                    body_text = re.sub(r'\s+', ' ', body_text).strip()
                    body_text = ''.join(c for c in body_text if c.isprintable() or c == ' ')
                    
                    print(f"Body method found {len(ms_cs_keywords)} MS CS keywords")
                    return {
                        'content': body_text,
                        'method': 'body',
                        'keywords': keywords,
                        'ms_cs_keywords': ms_cs_keywords
                    }
            
            print("No suitable content found")
            return {'error': 'No content found'}
            
        except Exception as e:
            print(f"Error processing content: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            logger.error(f"Error processing content: {str(e)}")
            return {'error': str(e)}

    async def store_urls_in_mongodb(self, final=False):
        """
        Store only important URLs in MongoDB with rich metadata
        
        Args:
            final: If True, this is the final storage call and we should include all stats
        """
        try:
            # Only store top 500 URLs (most relevant ones)
            top_urls = self.get_top_urls(count=self.max_urls_to_store, min_score=0.0)
            
            if not top_urls:
                logger.info("No URLs to store in MongoDB")
                return False
            
            # Use provided DB connection or initialize new one for backward compatibility
            db_instance = self.db
            if not db_instance:
                # Import MongoDB class here to avoid circular imports
                from services.database import MongoDB
                db_instance = MongoDB(os.getenv('MONGODB_URI'))
            
            # Prepare data structure with MS CS focused metadata
            urls_data = {
                'university_id': self.university_id,
                'university_name': '', # Could be populated from university details if available
                'program': self.program or 'MS Computer Science',
                'root_url': f"{self.scheme}://{self.root_domain}",
                'discovered_urls': top_urls,  # Only store the top 500 URLs
                'total_urls': len(self.scored_urls),  # Keep total count for reference
                'last_updated': datetime.utcnow(),
                'stats': {
                    'pages_crawled': self.stats['pages_crawled'],
                    'urls_discovered': self.stats['urls_discovered'],
                    'important_urls_found': self.stats['important_urls_found'],
                    'stored_urls_count': len(self.stored_urls),  # Add count of URLs with stored content
                    'stored_chunks_count': len(self.stored_chunks_hashes)  # Add count of unique chunks
                }
            }
            
            # Add more metadata if this is the final storage
            if final:
                # Get count of URLs by relevance bands
                high_relevance = len([u for u in top_urls if u['score'] > 0.75])
                medium_relevance = len([u for u in top_urls if 0.5 <= u['score'] <= 0.75])
                low_relevance = len([u for u in top_urls if u['score'] < 0.5])
                
                urls_data['stats']['high_relevance_count'] = high_relevance
                urls_data['stats']['medium_relevance_count'] = medium_relevance
                urls_data['stats']['low_relevance_count'] = low_relevance
                urls_data['stats']['avg_relevance_score'] = sum(u['score'] for u in top_urls) / len(top_urls) if top_urls else 0
                urls_data['crawl_completed_at'] = datetime.utcnow()
            
            # Store in a new collection specifically for MS CS URLs
            collection = db_instance.db.university_urls
            
            # Upsert to MongoDB
            result = collection.update_one(
                {'university_id': self.university_id, 'program': urls_data['program']},
                {'$set': urls_data},
                upsert=True
            )
            
            success = result.modified_count > 0 or result.upserted_id is not None
            logger.info(f"Stored {len(top_urls)} URLs in MongoDB: {success}")
            
            # Update the university record with URLs count
            db_instance.update_university(self.university_id, {
                'discovered_urls_count': len(top_urls),
                'stored_urls_count': len(self.stored_urls),
                'stored_chunks_count': len(self.stored_chunks_hashes),
                'metadata': {
                    'urls_last_updated': datetime.utcnow(),
                    'program': self.program
                }
            })
            
            return success
            
        except Exception as e:
            logger.error(f"Error storing URLs in MongoDB: {str(e)}")
            return False

    async def store_important_urls_in_mongodb(self):
        """Store collected important URLs in MongoDB (backward compatibility method)"""
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

    async def phase1_discover_urls(self, progress_callback=None):
        """
        Phase 1: Discover all URLs and identify important ones
        Modified to discover URLs but not store all 5000 in MongoDB
        """
        print(f"Starting Phase 1: URL discovery - targeting 5000 candidate URLs")
        
        try:
            processed_urls = 0
            discovered_urls = set()
            
            # Increase discovery target to 5000 URLs
            discovery_target = 5000
            
            connector = aiohttp.TCPConnector(limit=self.concurrent_requests, ssl=False)
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Process URLs until queue is empty or we've discovered enough URLs
                while self.urls_to_visit and len(discovered_urls) < discovery_target:
                    # Process URLs in batches
                    batch_urls = []
                    for _ in range(self.batch_size):
                        if not self.urls_to_visit:
                            break
                        url = self.urls_to_visit.popleft()
                        if url not in self.visited_urls:
                            batch_urls.append(url)

                    if not batch_urls:
                        continue
                    
                    # Process batch (extract only)
                    tasks = [self.crawl_url(session, url, extract_only=True) for url in batch_urls]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Process results
                    for result in results:
                        if isinstance(result, Exception):
                            continue
                            
                        if not result['success']:
                            continue
                            
                        processed_urls += 1
                        
                        # For all URLs (not just important ones), calculate and store score
                        url = result['url']
                        score = self.calculate_url_relevance(url)
                        
                        # Add metadata
                        self.add_scored_url(url, score, {
                            'title': result.get('title', ''),
                            'keyword_matches': result.get('keywords_matched', []),
                            'source': 'crawl',
                            'discovered_at': datetime.utcnow().isoformat()
                        })
                        
                        # If it's an important URL, add to important_urls set
                        if result['is_important']:
                            self.important_urls.add(url)
                            self.stats['important_urls_found'] += 1
                            
                        # Add new links to the queue
                        for new_url in result.get('new_links', []):
                            if new_url not in discovered_urls and new_url not in self.visited_urls:
                                discovered_urls.add(new_url)
                                self.urls_to_visit.append(new_url)
                                self.stats['urls_discovered'] += 1
                    
                    # Update progress
                    if progress_callback:
                        progress_callback({
                            'phase': 'discovery',
                            'processed_urls': processed_urls,
                            'total_discovered': self.stats['urls_discovered'],
                            'important_urls': len(self.important_urls),
                            'scored_urls_count': len(self.scored_urls)
                        })
                    
                    # Log progress
                    if processed_urls % 100 == 0:
                        print(f"Phase 1 Progress: {processed_urls} URLs processed, {len(self.important_urls)} important URLs found, {len(self.scored_urls)} scored URLs collected, {len(discovered_urls)}/{discovery_target} URLs discovered")
                    
                    await asyncio.sleep(0.05)  # Small delay between batches
                
                print(f"Phase 1 Complete: {processed_urls} URLs processed, {len(self.important_urls)} important URLs found, {len(self.scored_urls)} scored URLs collected")
                
                # Now select the top 500 URLs by relevance score
                selected_urls = self.get_top_urls(count=self.max_urls_to_store, min_score=0.0)
                print(f"Selected top {len(selected_urls)} URLs from {len(self.scored_urls)} total scored URLs")
                
                # Replace the full list with only the top URLs for processing in phase 2
                self.scored_urls = selected_urls
                
                # Don't store all 5000 URLs in MongoDB yet, wait for content processing
                # We'll only store the actually processed URLs after phase 2
                
                return {
                    'success': True,
                    'processed_urls': processed_urls,
                    'important_urls': len(self.important_urls),
                    'scored_urls': len(self.scored_urls),
                    'total_discovered': len(discovered_urls)
                }
        except Exception as e:
            logger.error(f"Error in phase1_discover_urls: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    async def phase2_process_important_urls(self, progress_callback=None):
        """
        Phase 2: Process and store content from important URLs with URL normalization
        """
        print(f"Starting Phase 2: Processing {len(self.important_urls)} important URLs")
        
        try:
            processed_urls = 0
            stored_count = 0
            failed_urls = []
            
            # IMPORTANT: Clear visited URLs for phase 2
            self.visited_urls.clear()
            self.extracted_data.clear()
            
            # Create a set for normalized URLs to avoid duplicates
            normalized_important_urls = list(self.important_urls)
            print(f"Processing {len(normalized_important_urls)} normalized important URLs")
            
            # Sort URLs by relevance score
            sorted_important_urls = []
            for url in normalized_important_urls:
                # Find the URL's relevance score from scored_urls if available
                score = 0.5  # Default score
                for scored_url in self.scored_urls:
                    if self.normalize_url(scored_url['url']) == url:
                        score = scored_url['score']
                        break
                sorted_important_urls.append((url, score))
            
            # Sort by relevance score (higher first)
            sorted_important_urls.sort(key=lambda x: x[1], reverse=True)
            urls_to_process = [url for url, score in sorted_important_urls]
            
            print(f"Sorted {len(urls_to_process)} URLs by relevance score for processing")
            
            # Process URLs in smaller batches for better reliability
            batch_size = 5  # Reduced batch size for reliability
            
            # Create a progress tracking dictionary
            progress = {
                'total': len(urls_to_process),
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'stored_chunks': 0
            }
            
            # Tracking normalized URLs that have been processed
            processed_normalized_urls = set()
            
            # Process in batches
            for batch_start in range(0, len(urls_to_process), batch_size):
                batch_urls = urls_to_process[batch_start:batch_start + batch_size]
                print(f"\n--- Processing batch {batch_start//batch_size + 1}/{(len(urls_to_process) + batch_size - 1)//batch_size}: {len(batch_urls)} URLs ---")
                
                # Reset batch data
                batch_data = []
                batch_success = 0
                batch_failed = 0
                
                # Create a new connector and session for each batch
                connector = aiohttp.TCPConnector(
                    limit=3,  # Limit to 3 concurrent connections
                    force_close=True,  # Don't keep connections open
                    ssl=False  # Skip SSL verification
                )
                timeout = aiohttp.ClientTimeout(total=120)  # 2 minute timeout for the entire operation
                
                async with aiohttp.ClientSession(
                    connector=connector, 
                    timeout=timeout,
                    headers={'Connection': 'close'}  # Ensure connections are closed
                ) as session:
                    # Process URLs sequentially within the batch
                    for normalized_url in batch_urls:
                        # Skip if already processed in this run
                        if normalized_url in processed_normalized_urls:
                            print(f"Normalized URL already processed in this run, skipping: {normalized_url}")
                            continue
                        
                        print(f"Processing normalized URL: {normalized_url}")
                        
                        # Add a delay before each request to avoid rate limiting
                        await asyncio.sleep(1.0)
                        
                        # Process the URL with multiple retries if needed
                        for retry in range(3):  # Try up to 3 times
                            try:
                                result = await self.crawl_url(session, normalized_url, extract_only=False)
                                
                                if result.get('success') and result.get('content'):
                                    print(f"✓ Successfully processed {normalized_url} - content length: {len(result['content'])}")
                                    batch_success += 1
                                    processed_urls += 1
                                    progress['successful'] += 1
                                    
                                    # Mark as processed to prevent duplicates
                                    processed_normalized_urls.add(normalized_url)
                                    
                                    # Use normalized URL in batch_data
                                    batch_data.append({
                                        'url': normalized_url,  # Store normalized URL
                                        'content': result['content'],
                                        'metadata': {
                                            'program': self.program,
                                            'timestamp': datetime.utcnow().isoformat(),
                                            'title': result.get('title', ''),
                                            'keywords_matched': result.get('keywords_matched', []),
                                            'normalized_url': normalized_url  # Store for reference
                                        }
                                    })
                                    break  # Success, exit retry loop
                                else:
                                    error_msg = result.get('error_details', 'Unknown error')
                                    print(f"✗ Failed to process {normalized_url}: {error_msg} (attempt {retry+1})")
                                    
                                    if retry == 2:  # Last retry attempt
                                        batch_failed += 1
                                        progress['failed'] += 1
                                        failed_urls.append({
                                            'url': normalized_url,
                                            'error': error_msg
                                        })
                                    else:
                                        # Wait longer between retries
                                        await asyncio.sleep(2 * (retry + 1))
                            
                            except Exception as e:
                                print(f"Error on retry {retry+1} for {normalized_url}: {str(e)}")
                                
                                if retry == 2:  # Last retry attempt
                                    batch_failed += 1
                                    progress['failed'] += 1
                                    failed_urls.append({
                                        'url': normalized_url,
                                        'error': str(e)
                                    })
                                else:
                                    # Wait longer between retries
                                    await asyncio.sleep(2 * (retry + 1))
                        
                        # Update progress after each URL
                        progress['processed'] += 1
                        
                        # Update progress callback if provided
                        if progress_callback:
                            progress_callback({
                                'phase': 'processing',
                                'processed_urls': progress['processed'],
                                'total_urls': progress['total'],
                                'stored_chunks': progress['stored_chunks'],
                                'failed_urls': progress['failed']
                            })
                    
                    # Process this batch's data after all URLs are processed
                    if batch_data:
                        print(f"Storing content from {len(batch_data)} URLs to Pinecone...")
                        namespace = f"uni_{self.university_id}"
                        
                        try:
                            batch_chunks = await self.store_batch_in_pinecone(batch_data, namespace)
                            stored_count += batch_chunks
                            progress['stored_chunks'] += batch_chunks
                            
                            print(f"Successfully stored {batch_chunks} chunks in Pinecone from this batch")
                            
                            # Clear batch data after storing to save memory
                            batch_data = []
                        except Exception as store_error:
                            print(f"Error storing batch data in Pinecone: {str(store_error)}")
                            # Continue processing other batches even if this one fails
                
                # Print batch summary
                print(f"\nBatch {batch_start//batch_size + 1} results:")
                print(f"- Successful: {batch_success}/{len(batch_urls)}")
                print(f"- Failed: {batch_failed}/{len(batch_urls)}")
                print(f"- Overall progress: {processed_urls}/{len(urls_to_process)} ({int(processed_urls/len(urls_to_process)*100)}%)")
                
                # Longer delay between batches to avoid overwhelming the server
                await asyncio.sleep(3.0)
            
            print(f"\nPhase 2 Complete: {processed_urls}/{len(urls_to_process)} URLs processed")
            print(f"- {stored_count} chunks stored")
            print(f"- {len(failed_urls)} URLs failed")
            
            # Store failed URL details for debugging
            if failed_urls and len(failed_urls) > 0:
                print("\nFailed URLs sample (first 5):")
                for i, failed in enumerate(failed_urls[:5]):
                    print(f"{i+1}. {failed['url']}: {failed['error']}")
            
            return {
                'success': True,
                'processed_urls': processed_urls,
                'stored_chunks': stored_count,
                'failed_urls': len(failed_urls)
            }
            
        except Exception as e:
            import traceback
            print(f"Error in Phase 2: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e)
            }

    @staticmethod
    async def process_specific_urls(urls: List[str], university_id: str, program: str = "MS Computer Science", 
                                namespace: str = None, openai_api_key: str = None, 
                                pinecone_api_key: str = None, index_name: str = None):
        """
        Process a list of specific URLs with improved reliability
        
        Args:
            urls: List of URLs to process
            university_id: University ID
            program: Program name
            namespace: Custom namespace (defaults to uni_{university_id})
            openai_api_key: OpenAI API key (if not provided, uses environment variable)
            pinecone_api_key: Pinecone API key (if not provided, uses environment variable)
            index_name: Pinecone index name (if not provided, uses environment variable)
        
        Returns:
            Processing results
        """
        try:
            # Use provided keys or get from environment
            openai_key = openai_api_key or os.getenv('OPENAI_API_KEY')
            pinecone_key = pinecone_api_key or os.getenv('PINECONE_API_KEY')
            idx_name = index_name or os.getenv('INDEX_NAME')
            
            # Set namespace
            ns = namespace or f"uni_{university_id}"
            
            # Create clients
            openai_client = OpenAI(api_key=openai_key)
            pc = Pinecone(api_key=pinecone_key)
            index = pc.Index(idx_name)
            
            logger.info(f"Processing {len(urls)} specific URLs for university {university_id}")
            
            # Track results
            processed = 0
            successful = 0
            failed = 0
            failed_urls = []
            stored_chunks = 0
            
            # Process in smaller batches (10 URLs per batch)
            batch_size = 10
            
            for i in range(0, len(urls), batch_size):
                batch = urls[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(urls) + batch_size - 1)//batch_size}")
                
                # Set up connector with limited concurrent connections
                connector = aiohttp.TCPConnector(limit=5, ssl=False)
                timeout = aiohttp.ClientTimeout(total=60)
                
                batch_results = []
                
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    # Process batch in parallel but with limited concurrency
                    for url in batch:
                        # Process with retries
                        for attempt in range(3):
                            try:
                                # Random delay to avoid overwhelming the server
                                await asyncio.sleep(0.2 + random.random() * 0.5)
                                
                                # Fetch URL with improved headers
                                headers = {
                                    'User-Agent': random.choice([
                                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
                                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15',
                                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Firefox/89.0'
                                    ]),
                                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                                    'Accept-Language': 'en-US,en;q=0.5',
                                    'Connection': 'keep-alive',
                                    'Upgrade-Insecure-Requests': '1'
                                }
                                
                                async with session.get(url, headers=headers, allow_redirects=True) as response:
                                    if response.status != 200:
                                        logger.warning(f"HTTP error {response.status} for {url}")
                                        if attempt < 2:  # Try again if not last attempt
                                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                                            continue
                                        batch_results.append({
                                            'url': url,
                                            'success': False,
                                            'error': f"HTTP error {response.status}"
                                        })
                                        break
                                    
                                    # Check content type
                                    content_type = response.headers.get('Content-Type', '').lower()
                                    if 'text/html' not in content_type and 'application/xhtml+xml' not in content_type:
                                        logger.warning(f"Not HTML content for {url}: {content_type}")
                                        batch_results.append({
                                            'url': url,
                                            'success': False,
                                            'error': f"Not HTML content: {content_type}"
                                        })
                                        break
                                    
                                    # Get HTML content
                                    html = await response.text()
                                    
                                    # Extract content
                                    extracted_text = None
                                    
                                    # Try trafilatura first
                                    try:
                                        extracted_text = trafilatura.extract(
                                            html,
                                            include_comments=False,
                                            no_fallback=False,
                                            include_tables=True,
                                            favor_recall=True
                                        )
                                    except Exception as e:
                                        logger.warning(f"Trafilatura extraction failed for {url}: {str(e)}")
                                    
                                    # If trafilatura fails, try BeautifulSoup
                                    if not extracted_text or len(extracted_text) < 200:
                                        soup = BeautifulSoup(html, 'lxml')
                                        
                                        # Remove unwanted elements
                                        for tag in soup.find_all(['script', 'style', 'nav', 'footer']):
                                            tag.decompose()
                                        
                                        # Try content containers
                                        main_content = ""
                                        for container in ['main', 'article', '#content', '.content', 'section']:
                                            elements = soup.select(container)
                                            if elements:
                                                content = elements[0].get_text(separator=' ', strip=True)
                                                if len(content) > 200:
                                                    main_content = content
                                                    break
                                        
                                        # If no container, try paragraphs
                                        if not main_content:
                                            paragraphs = []
                                            for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'li']):
                                                text = tag.get_text(strip=True)
                                                if len(text) > 20:
                                                    paragraphs.append(text)
                                            
                                            if paragraphs:
                                                main_content = ' '.join(paragraphs)
                                        
                                        # If still no content, use body
                                        if not main_content and soup.body:
                                            main_content = soup.body.get_text(separator=' ', strip=True)
                                        
                                        extracted_text = main_content
                                    
                                    # Check if we got useful content
                                    if not extracted_text or len(extracted_text) < 200:
                                        logger.warning(f"No meaningful content extracted from {url}")
                                        batch_results.append({
                                            'url': url,
                                            'success': False,
                                            'error': "No meaningful content extracted"
                                        })
                                        break
                                    
                                    # Clean text
                                    cleaned_text = re.sub(r'\s+', ' ', extracted_text).strip()
                                    cleaned_text = ''.join(c for c in cleaned_text if c.isprintable() or c.isspace())
                                    
                                    # Create chunks
                                    chunks = []
                                    sentences = re.split(r'(?<=[.!?])\s+', cleaned_text)
                                    current_chunk = []
                                    current_size = 0
                                    chunk_size = 800  # words
                                    
                                    for sentence in sentences:
                                        words = sentence.split()
                                        sentence_size = len(words)
                                        
                                        if current_size + sentence_size > chunk_size and current_chunk:
                                            chunks.append(' '.join(current_chunk))
                                            current_chunk = [sentence]
                                            current_size = sentence_size
                                        else:
                                            current_chunk.append(sentence)
                                            current_size += sentence_size
                                    
                                    if current_chunk:
                                        chunks.append(' '.join(current_chunk))
                                    
                                    # Check if we have chunks
                                    if not chunks:
                                        logger.warning(f"Failed to create chunks from {url}")
                                        batch_results.append({
                                            'url': url,
                                            'success': False,
                                            'error': "Failed to create text chunks"
                                        })
                                        break
                                    
                                    # Process chunks and create vectors
                                    vectors = []
                                    chunks_stored = 0
                                    
                                    for idx, chunk in enumerate(chunks):
                                        # Generate ID
                                        vector_id = f"{hashlib.md5(url.encode()).hexdigest()}_{idx}_{int(time.time())}"
                                        
                                        # Get embedding
                                        try:
                                            response = openai_client.embeddings.create(
                                                model="text-embedding-ada-002",
                                                input=chunk[:8000]  # Limit to 8000 chars for API
                                            )
                                            embedding = response.data[0].embedding
                                        except Exception as e:
                                            logger.error(f"Error creating embedding for chunk {idx} of {url}: {str(e)}")
                                            continue
                                        
                                        # Create vector
                                        vector = {
                                            'id': vector_id,
                                            'values': embedding,
                                            'metadata': {
                                                'text': chunk,
                                                'url': url,
                                                'chunk_index': idx,
                                                'total_chunks': len(chunks),
                                                'program': program,
                                                'university_id': university_id,
                                                'timestamp': datetime.utcnow().isoformat()
                                            }
                                        }
                                        
                                        vectors.append(vector)
                                        chunks_stored += 1
                                        
                                        # Store in smaller batches to avoid API limits
                                        if len(vectors) >= 100:
                                            try:
                                                index.upsert(vectors=vectors, namespace=ns)
                                                vectors = []
                                            except Exception as e:
                                                logger.error(f"Error upserting vectors: {str(e)}")
                                    
                                    # Store any remaining vectors
                                    if vectors:
                                        try:
                                            index.upsert(vectors=vectors, namespace=ns)
                                        except Exception as e:
                                            logger.error(f"Error upserting final vectors: {str(e)}")
                                    
                                    # Add result
                                    batch_results.append({
                                        'url': url,
                                        'success': True,
                                        'chunks_stored': chunks_stored
                                    })
                                    break  # Success, break retry loop
                                    
                            except asyncio.TimeoutError:
                                logger.warning(f"Timeout for {url} (attempt {attempt+1}/3)")
                                if attempt < 2:  # Try again if not last attempt
                                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                                else:
                                    batch_results.append({
                                        'url': url,
                                        'success': False,
                                        'error': "Request timed out"
                                    })
                            except Exception as e:
                                logger.error(f"Error processing {url} (attempt {attempt+1}/3): {str(e)}")
                                if attempt < 2:  # Try again if not last attempt
                                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                                else:
                                    batch_results.append({
                                        'url': url,
                                        'success': False,
                                        'error': str(e)
                                    })
                
                # Process batch results
                for result in batch_results:
                    processed += 1
                    if result['success']:
                        successful += 1
                        stored_chunks += result.get('chunks_stored', 0)
                    else:
                        failed += 1
                        failed_urls.append({
                            'url': result['url'],
                            'error': result.get('error', 'Unknown error')
                        })
                
                # Log batch progress
                logger.info(f"Batch {i//batch_size + 1} completed: {successful}/{processed} successful, {stored_chunks} chunks stored")
                
                # Wait between batches
                await asyncio.sleep(1)
            
            # Create final results
            return {
                'success': successful > 0,
                'processed': processed,
                'successful': successful,
                'failed': failed,
                'stored_chunks': stored_chunks,
                'failures': failed_urls[:20],  # Limit failures list
                'namespace': ns
            }
            
        except Exception as e:
            logger.error(f"Error in process_specific_urls: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _process_single_url(self, session: aiohttp.ClientSession, url: str, 
                             university_id: str, program: str) -> dict:
        """Process a single URL with better error handling"""
        try:
            # Use a dynamic user agent
            headers = {
                'User-Agent': random.choice([
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
                ]),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            # Fetch the URL with timeout
            async with session.get(url, headers=headers, timeout=30, ssl=False) as response:
                if response.status != 200:
                    return {
                        'url': url,
                        'success': False,
                        'error': f"HTTP Error: {response.status}"
                    }
                
                # Check content type
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type and 'application/xhtml+xml' not in content_type:
                    return {
                        'url': url,
                        'success': False,
                        'error': f"Not HTML content: {content_type}"
                    }
                
                # Extract content
                html = await response.text()
                
                # Try trafilatura for extraction (best quality)
                extracted_text = None
                try:
                    extracted_text = trafilatura.extract(
                        html,
                        include_comments=False,
                        no_fallback=False,
                        include_tables=True,
                        favor_recall=True
                    )
                except Exception as e:
                    logger.warning(f"Trafilatura extraction failed for {url}: {str(e)}")
                
                # If trafilatura failed, use BeautifulSoup
                if not extracted_text or len(extracted_text) < 200:
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Get title
                    title = soup.title.string.strip() if soup.title else ""
                    
                    # Remove unwanted elements
                    for tag in soup.find_all(['script', 'style', 'nav', 'footer', 'header']):
                        tag.decompose()
                    
                    # Try content containers
                    main_content = ""
                    for container in ['main', 'article', '#content', '.content', '#main-content']:
                        elements = soup.select(container)
                        if elements:
                            content = elements[0].get_text(separator=' ', strip=True)
                            if len(content) > 200:
                                main_content = content
                                break
                    
                    # If no container found, get paragraphs
                    if not main_content:
                        paragraphs = []
                        for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'li']):
                            text = tag.get_text(strip=True)
                            if len(text) > 20:
                                paragraphs.append(text)
                        
                        if paragraphs:
                            main_content = ' '.join(paragraphs)
                    
                    # If still no content, use body text
                    if not main_content and soup.body:
                        main_content = soup.body.get_text(separator=' ', strip=True)
                    
                    extracted_text = main_content
                
                # Check if extraction succeeded
                if not extracted_text or len(extracted_text) < 200:
                    return {
                        'url': url,
                        'success': False,
                        'error': "Failed to extract meaningful content"
                    }
                
                # Clean text
                cleaned_text = re.sub(r'\s+', ' ', extracted_text).strip()
                
                # Create document for storage
                document = {
                    'url': url,
                    'content': cleaned_text,
                    'metadata': {
                        'university_id': university_id,
                        'program': program,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                }
                
                # Store in Pinecone
                namespace = f"uni_{university_id}"
                chunks = self.chunk_text(cleaned_text)
                
                # Create vectors
                vectors = []
                for idx, chunk in enumerate(chunks):
                    # Get embedding
                    embedding = await self.get_embedding_async(chunk)
                    if not embedding:
                        continue
                    
                    # Create vector ID
                    vector_id = f"{hashlib.md5(url.encode()).hexdigest()}_{idx}_{int(time.time())}"
                    
                    # Create vector
                    vector = {
                        'id': vector_id,
                        'values': embedding,
                        'metadata': {
                            'text': chunk,
                            'url': url,
                            'chunk_index': idx,
                            'total_chunks': len(chunks),
                            'program': program,
                            'university_id': university_id,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    }
                    
                    vectors.append(vector)
                
                # Store vectors
                if vectors:
                    await asyncio.to_thread(
                        self.index.upsert,
                        vectors=vectors,
                        namespace=namespace
                    )
                
                return {
                    'url': url,
                    'success': True,
                    'chunks_stored': len(vectors)
                }
                
        except asyncio.TimeoutError:
            return {
                'url': url,
                'success': False,
                'error': "Request timed out"
            }
        except Exception as e:
            return {
                'url': url,
                'success': False,
                'error': str(e)
            }
