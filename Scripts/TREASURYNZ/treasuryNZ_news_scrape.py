#!/usr/bin/env python3
"""
New Zealand Treasury News Scraper
Scrapes all news articles from Treasury NZ website with pagination support,
PDF processing, anti-bot measures, and deduplication for daily runs.
Enhanced with recursive content extraction from linked pages and PDFs.
"""

import json
import logging
import os
import re
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
import io
import random

# Configure logging first before any other imports
os.makedirs('data', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/treasuryNZ_news.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import PyPDF2

# Try to import selenium-stealth, but continue without it if not available
try:
    from selenium_stealth import stealth
    STEALTH_AVAILABLE = True
except ImportError:
    logger.warning("selenium-stealth not available, using basic Selenium setup")
    STEALTH_AVAILABLE = False

# Try to import cloudscraper for better anti-bot evasion
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    logger.warning("cloudscraper not available, using standard requests")
    CLOUDSCRAPER_AVAILABLE = False

class TreasuryNZNewsScraper:
    """Main scraper class for Treasury NZ news articles"""
    
    def __init__(self, max_pages=None):
        self.base_url = "https://www.treasury.govt.nz"
        self.news_url = "https://www.treasury.govt.nz/news-and-events/news"
        
        # Set maximum pages to scrape (None = all pages, number = limit pages)
        self.MAX_PAGES = max_pages if max_pages is not None else 3
        
        # Create data directory
        Path("data").mkdir(exist_ok=True)
        
        # Initialize session with better anti-bot measures
        self._initialize_session()
        
        # Load existing data for deduplication
        self.existing_articles = self._load_existing_data()
        self.seen_urls = self._load_seen_urls()
        
        # Setup Selenium with stealth configuration
        self.driver = None
        self._setup_selenium_stealth()
        
    def _initialize_session(self):
        """Initialize session with enhanced anti-bot measures"""
        if CLOUDSCRAPER_AVAILABLE:
            logger.info("Using cloudscraper for better anti-bot evasion")
            self.session = cloudscraper.create_scraper(
                browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
            )
        else:
            self.session = requests.Session()
            
        # Rotate user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0'
        ]
        
        # Enhanced headers
        self._enhanced_requests_session()

    def _enhanced_requests_session(self):
        """Enhanced session setup for better anti-bot evasion"""
        base_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,en-NZ;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        # Only update if not using cloudscraper (it handles headers automatically)
        if not CLOUDSCRAPER_AVAILABLE:
            self.session.headers.update(base_headers)
            self.session.headers['User-Agent'] = random.choice(self.user_agents)

    def _make_robust_request(self, url: str, max_retries: int = 3) -> requests.Response:
        """Make a request with multiple retry strategies"""
        if not self.session:
            raise Exception("Session not initialized - cannot make requests")
        
        # Try Selenium first if available and previous attempts failed
        if self.driver and hasattr(self, '_failed_with_requests') and self._failed_with_requests:
            try:
                logger.info(f"Using Selenium for {url}")
                self.driver.get(url)
                time.sleep(random.uniform(2, 4))
                
                # Get cookies from Selenium and add to session
                for cookie in self.driver.get_cookies():
                    self.session.cookies.set(cookie['name'], cookie['value'])
                
                # Now try with session that has cookies
                self._failed_with_requests = False
                
            except Exception as e:
                logger.warning(f"Selenium attempt failed: {e}")
            
        for attempt in range(max_retries):
            try:
                # Randomize delay between requests
                if attempt > 0:
                    time.sleep(random.uniform(3, 6))
                
                # Rotate user agent for each attempt
                if not CLOUDSCRAPER_AVAILABLE:
                    self.session.headers['User-Agent'] = random.choice(self.user_agents)
                
                # Add referer header
                if '/news-and-events' in url:
                    self.session.headers['Referer'] = self.base_url
                else:
                    self.session.headers['Referer'] = self.news_url
                
                response = self.session.get(url, timeout=30, allow_redirects=True)
                
                if response.status_code == 403:
                    logger.warning(f"403 error on attempt {attempt + 1} for {url}")
                    
                    # Mark that requests failed
                    self._failed_with_requests = True
                    
                    # If we have Selenium, try using it
                    if self.driver and attempt == max_retries - 1:
                        logger.info("Falling back to Selenium due to 403 errors")
                        self.driver.get(url)
                        time.sleep(random.uniform(3, 5))
                        
                        # Create a mock response object with Selenium content
                        class MockResponse:
                            def __init__(self, text, status_code=200):
                                self.text = text
                                self.status_code = status_code
                                self.content = text.encode('utf-8')
                            def raise_for_status(self):
                                pass
                        
                        return MockResponse(self.driver.page_source)
                    
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 10
                        logger.info(f"Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                        continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 5)
                    continue
                raise
                
        raise requests.exceptions.RequestException(f"Failed to fetch {url} after {max_retries} attempts")

    def _setup_selenium_stealth(self):
        """Setup Selenium WebDriver with stealth configuration"""
        try:
            # Check if Chrome is available
            import shutil
            chrome_path = shutil.which('google-chrome') or shutil.which('chrome') or shutil.which('chromium')
            
            if not chrome_path:
                logger.warning("Chrome browser not found. Selenium will not be available.")
                logger.info("To install Chrome on Ubuntu/Debian: sudo apt-get install google-chrome-stable")
                logger.info("To install Chrome on macOS: brew install --cask google-chrome")
                logger.info("To install Chrome on Windows: Download from https://www.google.com/chrome/")
                self.driver = None
                return
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-plugins-discovery')
            chrome_options.add_argument('--disable-images')
            chrome_options.add_argument('--disable-javascript')
            chrome_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Additional anti-detection arguments
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            # Try to use webdriver-manager for automatic ChromeDriver management
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                from selenium.webdriver.chrome.service import Service
                
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                logger.warning(f"WebDriver Manager failed: {e}, trying default ChromeDriver...")
                self.driver = webdriver.Chrome(options=chrome_options)
            
            # Apply stealth settings if available
            if STEALTH_AVAILABLE:
                stealth(self.driver,
                    languages=["en-US", "en"],
                    vendor="Google Inc.",
                    platform="Win32",
                    webgl_vendor="Intel Inc.",
                    renderer="Intel Iris OpenGL Engine",
                    fix_hairline=True,
                )
                logger.info("Selenium with stealth configured successfully")
            else:
                logger.info("Selenium configured successfully (without stealth)")
                # Basic anti-detection measures
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Test Selenium by visiting the homepage
            self.driver.get(self.base_url)
            time.sleep(3)
            logger.info("Selenium test successful")
            
        except Exception as e:
            logger.warning(f"Failed to setup Selenium: {e}")
            logger.info("Continuing with requests-only mode (may be limited by anti-bot measures)")
            self.driver = None

    def _load_existing_data(self) -> List[Dict]:
        """Load existing articles for deduplication"""
        try:
            with open('data/treasuryNZ_news.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return []

    def _load_seen_urls(self) -> Set[str]:
        """Load previously seen URLs for deduplication"""
        try:
            with open('data/seen_urls.json', 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except FileNotFoundError:
            return set()

    def _save_seen_urls(self):
        """Save seen URLs for future deduplication"""
        with open('data/seen_urls.json', 'w', encoding='utf-8') as f:
            json.dump(list(self.seen_urls), f, indent=2)

    def _generate_content_hash(self, content: str) -> str:
        """Generate hash for content deduplication"""
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    def _session_initiation(self):
        """Perform session initiation to gather cookies and establish session"""
        try:
            logger.info("Starting session initiation...")
            
            # If using Selenium, get cookies from it first
            if self.driver:
                logger.info("Using Selenium for initial cookie gathering...")
                self.driver.get(self.base_url)
                time.sleep(random.uniform(3, 5))
                
                # Transfer cookies from Selenium to requests session
                for cookie in self.driver.get_cookies():
                    self.session.cookies.set(cookie['name'], cookie['value'])
                
                logger.info("Cookies transferred from Selenium to session")
                
                # Visit news section with Selenium
                self.driver.get(self.news_url)
                time.sleep(random.uniform(3, 5))
            
            else:
                # Try with requests only
                response = self._make_robust_request(self.base_url)
                logger.info(f"Homepage visit successful: {response.status_code}")
                time.sleep(random.uniform(2, 4))
                
                # Visit news section
                news_events_url = f"{self.base_url}/news-and-events"
                response = self._make_robust_request(news_events_url)
                logger.info(f"News section visit successful: {response.status_code}")
                time.sleep(random.uniform(2, 4))
            
            logger.info("Session initiation completed successfully")
            
        except Exception as e:
            logger.error(f"Session initiation failed: {e}")
            logger.info("Continuing anyway, but success may be limited")

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object"""
        if not date_str:
            return None
            
        try:
            # Clean the date string
            date_str = date_str.strip()
            
            # Handle various date formats used by Treasury NZ
            formats = [
                "%A, %d %B %Y",  # "Thursday, 3 July 2025"
                "%d %B %Y",      # "3 July 2025"
                "%d %b %Y",      # "3 Jul 2025"
                "%Y-%m-%d",      # "2025-07-03"
                "%d/%m/%Y"       # "03/07/2025"
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
                    
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.error(f"Date parsing error: {e}")
            return None

    def _extract_pdf_text_enhanced(self, pdf_url: str) -> Dict:
        """Extract text and tables from PDF URL with enhanced parsing"""
        try:
            logger.info(f"Extracting enhanced PDF content from: {pdf_url}")
            
            response = self._make_robust_request(pdf_url)
            
            # Read PDF content
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            tables = []
            
            for page_num, page in enumerate(pdf_reader.pages):
                try:
                    page_text = page.extract_text()
                    if page_text:
                        # Clean and normalize the text
                        page_text = self._clean_text_content(page_text)
                        text += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
                        
                        # Try to detect and extract tables (basic approach)
                        table_data = self._extract_tables_from_text(page_text)
                        if table_data:
                            tables.extend(table_data)
                            
                except Exception as e:
                    logger.warning(f"Failed to extract text from page {page_num} of {pdf_url}: {e}")
            
            logger.info(f"Successfully extracted {len(text)} characters and {len(tables)} tables from PDF")
            return {
                'text': text,
                'tables': tables
            }
            
        except Exception as e:
            logger.error(f"Failed to extract PDF content from {pdf_url}: {e}")
            return {'text': '', 'tables': []}

    def _find_main_content_area(self, soup: BeautifulSoup):
        """Find the main content area of a page using multiple strategies"""
        # Try various selectors in order of preference
        selectors = [
            ('div', {'class': 'prose article__body'}),
            ('div', {'class': 'article__body'}),
            ('div', {'class': 'node__content'}),
            ('div', {'class': 'content'}),
            ('div', {'class': 'field--name-body'}),
            ('article', {}),
            ('main', {}),
            ('div', {'class': 'region-content'}),
            ('div', {'id': 'content'}),
            ('div', {'class': 'node--view-mode-full'})
        ]
        
        for tag, attrs in selectors:
            content_area = soup.find(tag, attrs)
            if content_area:
                return content_area
                
        return None
    
    def _clean_text_content(self, text: str) -> str:
        """Clean and normalize text content for LLM processing"""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove multiple newlines
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Remove common artifacts
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\xff]', '', text)
        
        # Remove page numbers and footers
        text = re.sub(r'Page \d+ of \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\d+\s*\|\s*Page', '', text)
        
        # Clean up common PDF artifacts
        text = re.sub(r'\.{3,}', '...', text)  # Replace multiple dots
        text = re.sub(r'-{3,}', '---', text)   # Replace multiple dashes
        
        return text.strip()
    
    def _extract_tables_from_text(self, text: str) -> List[str]:
        """Extract potential tables from text (basic implementation)"""
        tables = []
        lines = text.split('\n')
        
        # Look for patterns that might indicate tables
        table_lines = []
        in_table = False
        
        for line in lines:
            # Simple heuristic: lines with multiple consecutive spaces or tabs
            if re.search(r'(\s{2,}|\t)', line) and len(line.split()) > 2:
                table_lines.append(line)
                in_table = True
            elif in_table and line.strip() == '':
                # Empty line might end the table
                if len(table_lines) > 2:  # At least 3 rows to be considered a table
                    tables.append('\n'.join(table_lines))
                table_lines = []
                in_table = False
                
        # Don't forget the last table if we ended in one
        if len(table_lines) > 2:
            tables.append('\n'.join(table_lines))
            
        return tables
    
    def _extract_all_relevant_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract all relevant links including publications, PDFs, and related content"""
        links = []
        seen_urls = set()
        
        # Find all links in the page
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if not href or href in seen_urls:
                continue
                
            # Convert relative URLs to absolute
            if href.startswith('/'):
                full_url = urljoin(base_url, href)
            elif href.startswith('http'):
                full_url = href
            else:
                continue
                
            # Skip if already seen
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)
            
            link_text = link.get_text(strip=True)
            
            # Skip unwanted links
            if any(skip in href.lower() for skip in ['mailto:', 'javascript:', '#', 'twitter.com', 'facebook.com', 'linkedin.com']):
                continue
                
            # Only follow treasury.govt.nz links
            if 'treasury.govt.nz' not in full_url:
                continue
                
            # Determine link type and relevance
            link_type = "related_page"
            is_relevant = False
            
            # PDFs are always relevant
            if href.lower().endswith('.pdf'):
                link_type = "pdf_document"
                is_relevant = True
                
            # Check URL patterns for relevance
            elif any(pattern in href.lower() for pattern in ['/publications/', '/research/', '/reports/', '/analysis/', '/media-statement/', '/press-release/']):
                is_relevant = True
                if 'publication' in href.lower():
                    link_type = "publication"
                elif 'media' in href.lower() or 'press' in href.lower():
                    link_type = "media_statement"
                    
            # Check link text for relevance
            elif any(keyword in link_text.lower() for keyword in ['report', 'publication', 'analysis', 'paper', 'document', 'pdf', 'download', 'full text', 'read more']):
                is_relevant = True
                
            if is_relevant:
                links.append({
                    'type': link_type,
                    'url': full_url,
                    'text': link_text or 'No title'
                })
                
        return links

    def _scrape_related_content(self, links: List[Dict], visited_urls: Set[str] = None, depth: int = 0, max_depth: int = 3) -> Tuple[str, List[Dict]]:
        """Recursively scrape content from related links up to max_depth"""
        if visited_urls is None:
            visited_urls = set()
            
        related_content = ""
        all_extracted_content = []
        
        # Limit recursion depth
        if depth >= max_depth:
            logger.info(f"Reached maximum recursion depth of {max_depth}")
            return related_content, all_extracted_content
        
        for link in links:
            try:
                # Skip if already visited
                if link['url'] in visited_urls:
                    continue
                    
                visited_urls.add(link['url'])
                
                if link['type'] == 'pdf_document':
                    logger.info(f"[Depth {depth}] Extracting PDF: {link['url']}")
                    pdf_content = self._extract_pdf_text_enhanced(link['url'])
                    if pdf_content['text'].strip():
                        all_extracted_content.append({
                            'type': 'pdf',
                            'url': link['url'],
                            'title': link['text'],
                            'content': pdf_content['text'],
                            'tables': pdf_content.get('tables', []),
                            'depth': depth
                        })
                        
                elif link['type'] in ['related_page', 'media_statement', 'publication'] or 'treasury.govt.nz' in link['url']:
                    # Skip external links (not treasury.govt.nz)
                    if 'treasury.govt.nz' not in link['url']:
                        continue
                        
                    logger.info(f"[Depth {depth}] Scraping page: {link['url']}")
                    
                    response = self._make_robust_request(link['url'])
                    page_soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Extract main content
                    content_area = self._find_main_content_area(page_soup)
                    
                    if content_area:
                        # Remove unwanted elements
                        for unwanted in content_area.find_all(['nav', 'aside', 'footer', 'header', 'script', 'style']):
                            unwanted.decompose()
                            
                        page_content = self._clean_text_content(content_area.get_text(separator='\n', strip=True))
                        
                        if page_content and len(page_content) > 50:  # Only add substantial content
                            all_extracted_content.append({
                                'type': 'webpage',
                                'url': link['url'],
                                'title': link['text'],
                                'content': page_content,
                                'depth': depth
                            })
                            
                        # Extract links from this page for recursive scraping
                        sub_links = self._extract_all_relevant_links(page_soup, link['url'])
                        
                        if sub_links and depth + 1 < max_depth:
                            logger.info(f"[Depth {depth}] Found {len(sub_links)} sub-links to explore")
                            sub_content, sub_extracted = self._scrape_related_content(
                                sub_links, visited_urls, depth + 1, max_depth
                            )
                            all_extracted_content.extend(sub_extracted)
                    
                    time.sleep(random.uniform(1, 3))  # Random delay
                    
            except Exception as e:
                logger.warning(f"Failed to scrape content from {link['url']}: {e}")
                continue
        
        # Compile all content into a structured format
        for item in all_extracted_content:
            if item['type'] == 'pdf':
                related_content += f"\n\n{'='*80}\n"
                related_content += f"PDF Document: {item['title']}\n"
                related_content += f"Source: {item['url']}\n"
                related_content += f"Depth Level: {item['depth']}\n"
                related_content += f"{'='*80}\n\n"
                related_content += item['content']
                
                # Add tables if any
                if item.get('tables'):
                    related_content += "\n\n--- Tables Found in PDF ---\n"
                    for i, table in enumerate(item['tables'], 1):
                        related_content += f"\nTable {i}:\n{table}\n"
                        
            elif item['type'] == 'webpage':
                related_content += f"\n\n{'='*80}\n"
                related_content += f"Related Page: {item['title']}\n"
                related_content += f"Source: {item['url']}\n"
                related_content += f"Depth Level: {item['depth']}\n"
                related_content += f"{'='*80}\n\n"
                related_content += item['content']
                
        return related_content, all_extracted_content

    def _scrape_article_detail(self, article_url: str) -> Dict:
        """Scrape detailed content from individual article with recursive link following"""
        try:
            logger.info(f"Scraping article: {article_url}")
            
            response = self._make_robust_request(article_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title = ""
            title_elem = soup.find('h1', class_='page-header') or soup.find('h1')
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Extract date
            published_date = ""
            date_elem = soup.find('time')
            if date_elem:
                published_date = date_elem.get_text(strip=True)
            
            # Extract category/theme
            theme = ""
            theme_elem = soup.find('div', class_='article__type')
            if theme_elem:
                theme = theme_elem.get_text(strip=True)
            
            # Extract main article content
            article_content = ""
            content_area = self._find_main_content_area(soup)
            if content_area:
                # Clean the content area
                for unwanted in content_area.find_all(['nav', 'aside', 'footer', 'header', 'script', 'style']):
                    unwanted.decompose()
                    
                article_content = self._clean_text_content(content_area.get_text(separator='\n', strip=True))
            
            # Extract image URL
            image_url = ""
            img_elem = soup.find('img')
            if img_elem and img_elem.get('src'):
                image_url = urljoin(article_url, img_elem.get('src'))
            
            # Extract ALL relevant links from the page
            all_links = self._extract_all_relevant_links(soup, article_url)
            logger.info(f"Found {len(all_links)} relevant links to explore")
            
            # Recursively scrape content from related links and PDFs
            related_content, all_extracted_content = self._scrape_related_content(all_links, max_depth=3)
            
            # Compile complete content in LLM-friendly format
            complete_content = f"=== MAIN ARTICLE ===\n"
            complete_content += f"Title: {title}\n"
            complete_content += f"Published: {published_date}\n"
            complete_content += f"Theme: {theme}\n"
            complete_content += f"URL: {article_url}\n\n"
            complete_content += f"--- Article Content ---\n{article_content}\n"
            
            if related_content:
                complete_content += f"\n\n=== RELATED CONTENT ===\n"
                complete_content += related_content
            
            # Create article object with structured data
            article = {
                'title': title,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'theme': theme,
                'content': complete_content,
                'associated_image_url': image_url,
                'url': article_url,
                'related_links': [{'url': link['url'], 'text': link['text'], 'type': link['type']} for link in all_links],
                'extracted_content_summary': {
                    'total_items': len(all_extracted_content),
                    'pdfs': len([x for x in all_extracted_content if x['type'] == 'pdf']),
                    'webpages': len([x for x in all_extracted_content if x['type'] == 'webpage']),
                    'max_depth_reached': max([x['depth'] for x in all_extracted_content]) if all_extracted_content else 0
                }
            }
            
            # Add individual extracted content as separate fields for easy access
            for i, content_item in enumerate(all_extracted_content, 1):
                field_name = f"{content_item['type']}_content_{i}"
                article[field_name] = {
                    'url': content_item['url'],
                    'title': content_item['title'],
                    'depth': content_item['depth'],
                    'content_preview': content_item['content'][:500] + '...' if len(content_item['content']) > 500 else content_item['content']
                }
            
            logger.info(f"Successfully scraped article with {len(all_extracted_content)} related content items")
            return article
            
        except Exception as e:
            logger.error(f"Failed to scrape article {article_url}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {}

    def _get_pagination_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract pagination URLs from the news listing page"""
        pagination_urls = []
        
        # Find pagination section
        pagination = soup.find('nav', {'aria-label': 'Pagination'})
        if not pagination:
            pagination = soup.find('ul', class_='pager')
        if not pagination:
            pagination = soup.find('div', class_='pagination')
            
        if not pagination:
            return pagination_urls
            
        # Find all page links
        page_links = pagination.find_all('a')
        
        for link in page_links:
            href = link.get('href')
            if href and ('?page=' in href or '/page/' in href):
                full_url = urljoin(self.news_url, href)
                if full_url not in pagination_urls:
                    pagination_urls.append(full_url)
                    
        # Sort by page number
        def extract_page_num(url):
            try:
                if '?page=' in url:
                    return int(url.split('?page=')[1].split('&')[0])
                elif '/page/' in url:
                    return int(url.split('/page/')[1].split('/')[0])
            except:
                return 0
                
        pagination_urls.sort(key=extract_page_num)
        return pagination_urls

    def _scrape_news_listing_page(self, page_url: str) -> Tuple[List[str], List[str]]:
        """Scrape article URLs from a news listing page"""
        article_urls = []
        
        try:
            logger.info(f"Scraping news listing page: {page_url}")
            
            response = self._make_robust_request(page_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find article links - try multiple selectors
            article_elements = []
            
            # Try different selectors based on common patterns
            selectors = [
                ('div', {'class': 'slat'}),
                ('article', {}),
                ('div', {'class': 'node'}),
                ('div', {'class': 'views-row'}),
                ('div', {'class': 'field-content'}),
                ('li', {'class': 'news-item'}),
                ('div', {'class': 'news-item'}),
                ('div', {'class': 'item-list'}),
            ]
            
            for tag, attrs in selectors:
                article_elements = soup.find_all(tag, attrs)
                if article_elements:
                    logger.info(f"Found {len(article_elements)} article elements using selector: {tag} {attrs}")
                    break
            
            if not article_elements:
                # Try to find any links in the main content area
                main_content = soup.find('main') or soup.find('div', {'class': 'content'})
                if main_content:
                    article_elements = main_content.find_all('a', href=True)
                    logger.info(f"Found {len(article_elements)} links in main content area")
            
            for element in article_elements:
                # Try multiple ways to find the article link
                link = None
                
                if element.name == 'a':
                    link = element
                else:
                    # Look for links within the element
                    link = (
                        element.find('h3', class_='slat__title') or
                        element.find('h2') or
                        element.find('h3') or
                        element.find('a')
                    )
                    
                    if link and link.name != 'a':
                        link = link.find('a')
                
                if link and link.get('href'):
                    href = link.get('href')
                    
                    # Filter for news article URLs
                    if any(pattern in href for pattern in ['/news/', '/media-statement/', '/press-release/']):
                        article_url = urljoin(self.base_url, href)
                        
                        if article_url not in self.seen_urls:
                            article_urls.append(article_url)
                            title_text = link.get_text(strip=True) or "No title"
                            logger.debug(f"Found new article: {title_text[:50]}...")
                        else:
                            logger.info(f"Skipping already seen article: {article_url}")
            
            logger.info(f"Found {len(article_urls)} new articles on page")
            
            # Get pagination URLs if this is the first page
            if '?page=' not in page_url and '/page/' not in page_url:
                pagination_urls = self._get_pagination_urls(soup)
                logger.info(f"Found {len(pagination_urls)} pagination pages")
                return article_urls, pagination_urls
            
            return article_urls, []
            
        except Exception as e:
            logger.error(f"Failed to scrape listing page {page_url}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return [], []

    def scrape_all_news(self) -> List[Dict]:
        """Scrape news articles with pagination support and MAX_PAGES limit"""
        all_articles = []
        
        # Perform session initiation
        self._session_initiation()
        
        try:
            # Start with the main news page
            article_urls, pagination_urls = self._scrape_news_listing_page(self.news_url)
            
            # Apply MAX_PAGES limit
            if self.MAX_PAGES is None:
                # Scrape all pages
                all_page_urls = [self.news_url] + pagination_urls
                logger.info(f"MAX_PAGES set to None - scraping ALL {len(all_page_urls)} pages")
            else:
                # Limit to MAX_PAGES (including the first page)
                limited_pagination = pagination_urls[:self.MAX_PAGES-1] if len(pagination_urls) >= self.MAX_PAGES-1 else pagination_urls
                all_page_urls = [self.news_url] + limited_pagination
                logger.info(f"MAX_PAGES set to {self.MAX_PAGES} - scraping {len(all_page_urls)} pages (limited)")
            
            # Process pagination
            for page_num, page_url in enumerate(all_page_urls, 1):
                if page_url != self.news_url:  # Skip first page as already processed
                    logger.info(f"Processing pagination page {page_num}/{len(all_page_urls)}: {page_url}")
                    page_articles, _ = self._scrape_news_listing_page(page_url)
                    article_urls.extend(page_articles)
                else:
                    logger.info(f"Processing main page {page_num}/{len(all_page_urls)}")
                
                time.sleep(random.uniform(2, 4))  # Random delay between pagination requests
            
            logger.info(f"Total articles to scrape: {len(article_urls)}")
            
            # Scrape each article
            for i, article_url in enumerate(article_urls, 1):
                try:
                    logger.info(f"Processing article {i}/{len(article_urls)}")
                    
                    article = self._scrape_article_detail(article_url)
                    if article:
                        all_articles.append(article)
                        self.seen_urls.add(article_url)
                    
                    # Add random delay between article requests
                    time.sleep(random.uniform(3, 6))
                    
                except Exception as e:
                    logger.error(f"Failed to process article {article_url}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to scrape news articles: {e}")
            
        return all_articles

    def test_setup(self):
        """Test the scraper setup and basic functionality"""
        logger.info("🧪 Testing scraper setup...")
        
        # Test session
        if hasattr(self, 'session') and self.session:
            logger.info("✅ Requests session: OK")
        else:
            logger.error("❌ Requests session: FAILED or missing")
            return False
            
        # Test Chrome/Selenium
        if hasattr(self, 'driver') and self.driver:
            logger.info("✅ Selenium with Chrome: OK")
        else:
            logger.warning("⚠️  Selenium: Not available (will use requests-only mode)")
            
        # Test basic connectivity
        try:
            response = self._make_robust_request(self.base_url)
            if response.status_code == 200:
                logger.info("✅ Website connectivity: OK")
            else:
                logger.warning(f"⚠️  Website connectivity: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ Website connectivity: FAILED - {e}")
            return False
            
        logger.info("🎉 Setup test completed!")
        return True

    def save_results(self, articles: List[Dict]):
        """Save results to JSON file with deduplication"""
        try:
            # Merge with existing data, avoiding duplicates
            existing_urls = {article.get('url') for article in self.existing_articles}
            
            new_articles = []
            for article in articles:
                if article.get('url') not in existing_urls:
                    new_articles.append(article)
            
            # Combine and save
            all_articles = self.existing_articles + new_articles
            
            with open('data/treasuryNZ_news.json', 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
                
            # Save seen URLs
            self._save_seen_urls()
            
            logger.info(f"Saved {len(new_articles)} new articles. Total articles: {len(all_articles)}")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

    def cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'driver') and self.driver:
                self.driver.quit()
                logger.info("Selenium driver closed")
        except Exception as e:
            logger.warning(f"Error closing Selenium driver: {e}")
        
        try:
            if hasattr(self, 'session') and self.session:
                self.session.close()
                logger.info("Requests session closed")
        except Exception as e:
            logger.warning(f"Error closing requests session: {e}")

def main():
    """Main execution function with MAX_PAGES configuration"""
    
    # Configuration for MAX_PAGES
    # Set to None for initial full scrape (gets all articles)
    # Set to 3 for daily runs (gets only recent articles from first 3 pages)
    MAX_PAGES = 1  # Change this value as needed
    
    # You can also set this via command line argument
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == 'test':
            # Test mode
            scraper = TreasuryNZNewsScraper(max_pages=1)
            success = scraper.test_setup()
            scraper.cleanup()
            if success:
                print("✅ Setup test passed! Ready to scrape.")
                sys.exit(0)
            else:
                print("❌ Setup test failed! Check the logs for details.")
                sys.exit(1)
        
        try:
            MAX_PAGES = int(sys.argv[1]) if sys.argv[1] != 'all' else None
            print(f"MAX_PAGES set via command line: {MAX_PAGES}")
        except ValueError:
            print(f"Invalid MAX_PAGES value: {sys.argv[1]}. Using default: {MAX_PAGES}")
    
    scraper = TreasuryNZNewsScraper(max_pages=MAX_PAGES)
    
    try:
        if MAX_PAGES is None:
            logger.info("Starting Treasury NZ news scraping - FULL SCRAPE (all pages)...")
        else:
            logger.info(f"Starting Treasury NZ news scraping - LIMITED SCRAPE ({MAX_PAGES} pages)...")
            
        articles = scraper.scrape_all_news()
        scraper.save_results(articles)
        
        logger.info(f"Scraping completed. New articles scraped: {len(articles)}")
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        
    finally:
        scraper.cleanup()

if __name__ == "__main__":
    main()