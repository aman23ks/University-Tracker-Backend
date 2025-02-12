import asyncio
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

logger = logging.getLogger(__name__)

class HybridCrawler:
    def __init__(self, openai_api_key: str, pinecone_api_key: str, index_name: str):
        self.visited_urls: Set[str] = set()
        self.urls_to_visit: Set[str] = set()
        self.extracted_data: List[Dict] = []
        self.client = OpenAI(api_key=openai_api_key)
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.index = self.pc.Index(index_name)
        
        # Configure session
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        self.url_limit = 50
        
    async def initialize_crawl(self, root_url: str, program: str):
        """Initialize crawling parameters"""
        self.root_domain = urlparse(root_url).netloc
        self.program = program
        self.urls_to_visit = {root_url}
        self.visited_urls.clear()
        self.extracted_data.clear()

    def is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and belongs to root domain"""
        try:
            parsed = urlparse(url)
            return bool(
                parsed.netloc and
                parsed.scheme in ('http', 'https') and
                self.root_domain in parsed.netloc and
                not any(ext in parsed.path.lower() for ext in ['.pdf', '.jpg', '.png', '.zip'])
            )
        except Exception:
            return False

    def get_embedding(self, text: str) -> List[float]:
        """Get OpenAI embedding for text"""
        try:
            response = self.client.embeddings.create(
                model="text-embedding-ada-002",
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error getting embedding: {str(e)}")
            return []

    def clean_text(self, text: str) -> str:
        """Clean extracted text"""
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s.,;?!-]', '', text)
        return text.strip()

    async def extract_links(self, html: str, base_url: str) -> Set[str]:
        """Extract and validate links from HTML"""
        links = set()
        soup = BeautifulSoup(html, 'html.parser')
        
        for a in soup.find_all('a', href=True):
            url = urljoin(base_url, a['href'])
            if self.is_valid_url(url):
                links.add(url)
        
        return links

    def process_content(self, url: str, html_content: str) -> Dict:
        """Process and clean webpage content"""
        try:
            # Use trafilatura for main content extraction
            extracted_text = trafilatura.extract(html_content, include_comments=False)
            if not extracted_text:
                # Fallback to BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                main_content = soup.find('main') or soup.find('article') or soup.find('body')
                extracted_text = main_content.get_text() if main_content else ""
            
            # Clean text
            cleaned_text = self.clean_text(extracted_text)
            
            # Get metadata
            soup = BeautifulSoup(html_content, 'html.parser')
            title = soup.title.string if soup.title else ''
            meta_desc = ''
            for meta in soup.find_all('meta'):
                if meta.get('name') == 'description':
                    meta_desc = meta.get('content', '')
                    break
            
            return {
                'url': url,
                'content': cleaned_text,
                'metadata': {
                    'title': title,
                    'description': meta_desc,
                    'program': self.program,
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error processing content from {url}: {str(e)}")
            return None

    async def crawl_url(self, session: aiohttp.ClientSession, url: str) -> None:
        """Crawl a single URL"""
        if url in self.visited_urls:
            return
        
        try:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Extract and add new links
                    new_links = await self.extract_links(html, url)
                    self.urls_to_visit.update(new_links)
                    
                    # Process content
                    processed_data = self.process_content(url, html)
                    if processed_data:
                        self.extracted_data.append(processed_data)
                
                self.visited_urls.add(url)
                
        except Exception as e:
            logger.error(f"Error crawling {url}: {str(e)}")
            self.visited_urls.add(url)  # Mark as visited to avoid retries

    async def crawl(self) -> List[Dict]:
        """Main crawling function with configurable limit"""
        async with aiohttp.ClientSession() as session:
            while self.urls_to_visit:
                # Check if we've reached the limit
                if self.url_limit is not None and len(self.visited_urls) >= self.url_limit:
                    print(f"Reached URL limit of {self.url_limit}. Stopping crawl.")
                    break

                try:
                    # Calculate remaining URLs we can crawl
                    if self.url_limit is not None:
                        remaining_slots = self.url_limit - len(self.visited_urls)
                        if remaining_slots <= 0:
                            break
                        batch_size = min(10, remaining_slots, len(self.urls_to_visit))
                    else:
                        batch_size = min(10, len(self.urls_to_visit))

                    # Get next batch of URLs
                    batch_urls = set(list(self.urls_to_visit)[:batch_size])
                    self.urls_to_visit -= batch_urls
                    
                    # Crawl batch of URLs concurrently
                    tasks = [self.crawl_url(session, url) for url in batch_urls]
                    await asyncio.gather(*tasks)
                    
                    # Log progress
                    limit_info = f"/{self.url_limit}" if self.url_limit is not None else ""
                    print(f"Crawled {len(self.visited_urls)}{limit_info} pages, {len(self.urls_to_visit)} remaining to crawl")
                    
                except Exception as e:
                    logger.error(f"Error in crawl batch: {str(e)}")
                    continue
        
            print(f"Crawling completed. Total pages crawled: {len(self.visited_urls)}")
            return self.extracted_data
        
    def chunk_text(self, text: str, chunk_size: int = 3000) -> List[str]:
        """Split text into chunks"""
        chunks = []
        sentences = re.split(r'(?<=[.!?])\s+', text)
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            sentence_size = len(sentence)
            if current_size + sentence_size > chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = [sentence]
                current_size = sentence_size
            else:
                current_chunk.append(sentence)
                current_size += sentence_size
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks

    async def store_in_pinecone(self, data: List[Dict], namespace: str):
        """Store processed data in Pinecone"""
        print("Initializing vectors list...")
        vectors = []

        for doc in data:
            print(f"Processing document: {doc['url']}")

            # Split content into chunks
            chunks = self.chunk_text(doc['content'])
            print(f"Chunks created for document {doc['url']}: {chunks}")

            # Process chunks in parallel
            chunk_embeddings = []
            for chunk in chunks:
                print(f"Generating embedding for chunk: {chunk}")
                embedding = self.get_embedding(chunk)
                if embedding:  # Only add if embedding was generated successfully
                    chunk_embeddings.append({
                        'text': chunk,
                        'embedding': embedding
                    })
                    print(f"Embedding generated and added: {embedding}")
                else:
                    print(f"No embedding generated for chunk: {chunk}")

            # Create vectors for each chunk
            for idx, chunk_data in enumerate(chunk_embeddings):
                print(f"Creating vector for chunk {idx} of document {doc['url']}...")
                vector = {
                    'id': f"{doc['url']}_{idx}",
                    'values': chunk_data['embedding'],
                    'metadata': {
                        'text': chunk_data['text'],
                        'url': doc['url'],
                        'title': doc['metadata'].get('title', ''),
                        'description': doc['metadata'].get('description', ''),
                        'program': doc['metadata'].get('program', ''),
                        'timestamp': doc['metadata'].get('timestamp', ''),
                        'chunk_index': idx
                    }
                }
                vectors.append(vector)
                print(f"Vector created: {vector}")

                # Batch upsert when we have enough vectors
                if len(vectors) >= 100:
                    print(f"Batching upsert with {len(vectors)} vectors...")
                    try:
                        print("----------------namespace-------------")
                        print(namespace)
                        self.index.upsert(vectors=vectors, namespace=namespace)
                        print(f"Batch upsert successful for {len(vectors)} vectors.")
                        vectors = []
                    except Exception as e:
                        print(f"Error upserting vectors to Pinecone: {str(e)}")

        # Upsert any remaining vectors
        if vectors:
            print(f"Upserting remaining {len(vectors)} vectors...")
            try:
                self.index.upsert(vectors=vectors, namespace=namespace)
                print("Final upsert successful.")
            except Exception as e:
                print(f"Error upserting final vectors to Pinecone: {str(e)}")

        print(f"Total vectors processed: {len(vectors)}")
        return len(vectors)

    async def process_university(self, url: str, program: str, url_limit: Optional[int] = 50) -> Dict:
        """Main function to process a university with optional URL limit"""
        try:
            # Set URL limit
            self.set_url_limit(url_limit)
            
            # Initialize crawl
            await self.initialize_crawl(url, program)
            
            # Crawl pages
            crawled_data = await self.crawl()
            print("--------crawled data---------")
            print(f"URLs crawled: {len(self.visited_urls)}")
            print(f"Data entries extracted: {len(crawled_data)}")
            
            if not crawled_data:
                return {
                    'success': False,
                    'error': 'No data extracted from website'
                }
            
            # Create namespace from URL
            namespace = f"university_{urlparse(url).netloc.replace('.', '_')}"
            
            # Store in Pinecone
            await self.store_in_pinecone(crawled_data, namespace)
            
            return {
                'success': True,
                'pages_crawled': len(self.visited_urls),
                'data_chunks': sum(len(self.chunk_text(doc['content'])) for doc in crawled_data),
                'namespace': namespace,
                'limit_reached': self.url_limit is not None and len(self.visited_urls) >= self.url_limit
            }
            
        except Exception as e:
            logger.error(f"Error processing university {url}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def set_url_limit(self, limit: Optional[int]) -> None:
        """Set the URL limit. Use None for unlimited."""
        self.url_limit = limit
        print(f"URL limit set to: {'Unlimited' if limit is None else limit}")