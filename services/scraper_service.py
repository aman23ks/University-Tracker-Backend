import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
from typing import List, Dict
import asyncio
import aiohttp
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScraperService:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Dict:
        try:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    text = await response.text()
                    return {'url': url, 'html': text}
                return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None

    def extract_content(self, page_data: Dict) -> Dict:
        try:
            soup = BeautifulSoup(page_data['html'], 'html.parser')
            
            # Remove unwanted elements
            for element in soup.find_all(['script', 'style', 'nav', 'footer', 'header']):
                element.decompose()
            
            # Extract main content
            content = []
            for p in soup.find_all(['p', 'div', 'section', 'article']):
                text = p.get_text(strip=True)
                if len(text) > 50:  # Filter out short snippets
                    content.append(text)
            
            # Extract metadata
            metadata = {
                'title': soup.title.string if soup.title else '',
                'meta_description': '',
                'meta_keywords': ''
            }
            
            for meta in soup.find_all('meta'):
                if meta.get('name') == 'description':
                    metadata['meta_description'] = meta.get('content', '')
                elif meta.get('name') == 'keywords':
                    metadata['meta_keywords'] = meta.get('content', '')
            
            return {
                'url': page_data['url'],
                'content': ' '.join(content),
                'metadata': metadata
            }
        except Exception as e:
            logger.error(f"Error extracting content from {page_data['url']}: {str(e)}")
            return None

    def find_program_links(self, html: str, base_url: str, program: str) -> List[str]:
        soup = BeautifulSoup(html, 'html.parser')
        program_links = set()
        program_keywords = program.lower().split()
        
        for link in soup.find_all('a', href=True):
            url = urljoin(base_url, link['href'])
            if url.startswith(base_url) and not url.endswith(('.pdf', '.doc', '.jpg')):
                link_text = link.get_text().lower()
                if any(keyword in link_text for keyword in program_keywords):
                    program_links.add(url)
        
        return list(program_links)

    async def scrape_university(self, base_url: str, program: str) -> List[Dict]:
        async with aiohttp.ClientSession() as session:
            try:
                # Fetch main page
                main_page = await self.fetch_page(session, base_url)
                if not main_page:
                    return []
                
                # Find program-related links
                program_links = self.find_program_links(main_page['html'], base_url, program)
                
                # Fetch and process program pages
                tasks = [self.fetch_page(session, url) for url in program_links]
                pages = await asyncio.gather(*tasks)
                pages = [p for p in pages if p]
                
                # Extract content from pages
                results = []
                for page in pages:
                    content = self.extract_content(page)
                    if content:
                        results.append(content)
                
                return results
                
            except Exception as e:
                logger.error(f"Error scraping university {base_url}: {str(e)}")
                return []

    def clean_text(self, text: str) -> str:
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters
        text = re.sub(r'[^\w\s.,;?!-]', '', text)
        return text.strip()

    def validate_url(self, url: str) -> bool:
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False