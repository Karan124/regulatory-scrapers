#!/usr/bin/env python3
"""
Comprehensive Australian Bureau of Statistics Articles Scraper
Enhanced version with full content extraction, PDF processing, and anti-bot measures
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import time
import random
import logging
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from pathlib import Path
import PyPDF2
from io import BytesIO
from fake_useragent import UserAgent
import urllib3
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@dataclass
class Article:
    """Data class for article information"""
    hash_id: str
    url: str
    headline: str
    published_date: str
    scraped_date: str
    theme: str = ""
    article_type: str = ""
    content_text: str = ""
    pdf_content: str = ""
    image_url: str = ""
    related_links: List[str] = None
    charts_and_tables: str = ""
    
    def __post_init__(self):
        if self.related_links is None:
            self.related_links = []

class ABSArticlesScraper:
    """Enhanced ABS articles scraper with comprehensive content extraction"""
    
    def __init__(self, max_pages: int = 3, first_run: bool = False):
        """
        Initialize the scraper
        
        Args:
            max_pages: Maximum pages to scrape (36 for first run, 3 for daily runs)
            first_run: Whether this is the first run or a scheduled run
        """
        self.base_url = "https://www.abs.gov.au"
        self.articles_url = "https://www.abs.gov.au/articles"
        self.max_pages = 36 if first_run else max_pages
        self.first_run = first_run
        
        # Setup directories
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # File paths
        self.json_file = self.data_dir / "abs_all_articles.json"
        self.csv_file = self.data_dir / "abs_all_articles.csv"
        self.log_file = self.data_dir / "abs_scraper.log"
        
        # Setup logging
        self.setup_logging()
        
        # Load existing articles for deduplication
        self.existing_articles = self.load_existing_articles()
        self.existing_urls = {article.get('url', '') for article in self.existing_articles}
        self.existing_hashes = {article.get('hash_id', '') for article in self.existing_articles}
        
        # Setup session with anti-bot measures
        self.session = self.setup_session()
        
        # Statistics
        self.stats = {
            'pages_processed': 0,
            'articles_found': 0,
            'articles_scraped': 0,
            'articles_skipped': 0,
            'pdfs_processed': 0,
            'errors': 0
        }
        
        self.logger.info(f"ABS Scraper initialized - First run: {first_run}, Max pages: {self.max_pages}")
        self.logger.info(f"Existing articles: {len(self.existing_articles)}")
    
    def setup_logging(self):
        """Setup comprehensive logging"""
        # Clear existing handlers
        logger = logging.getLogger()
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler
        file_handler = logging.FileHandler(self.log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger('ABSScraper')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def setup_session(self):
        """Setup session with comprehensive anti-bot measures"""
        session = requests.Session()
        
        # Use fake user agent
        ua = UserAgent()
        
        # Comprehensive headers to mimic real browser
        session.headers.update({
            'User-Agent': ua.chrome,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Charset': 'utf-8, iso-8859-1;q=0.5',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Pragma': 'no-cache',
            'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Setup retry strategy
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Visit homepage first to establish session and collect cookies
        try:
            self.logger.info("Establishing session by visiting homepage...")
            response = session.get(self.base_url, timeout=30, verify=False)
            self.logger.info(f"Homepage visit status: {response.status_code}")
            
            # Visit articles page to collect more cookies
            time.sleep(random.uniform(2, 4))
            response = session.get(self.articles_url, timeout=30, verify=False)
            self.logger.info(f"Articles page visit status: {response.status_code}")
            
            # Random delay after session establishment
            time.sleep(random.uniform(3, 6))
            
        except Exception as e:
            self.logger.error(f"Error establishing session: {e}")
        
        return session
    
    def generate_hash(self, url: str, headline: str) -> str:
        """Generate unique hash for article"""
        content = f"{url.strip().rstrip('/')}|{headline.strip()}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    
    def load_existing_articles(self) -> List[Dict]:
        """Load existing articles for deduplication"""
        if not self.json_file.exists():
            self.logger.info("No existing articles file found - starting fresh")
            return []
        
        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                articles = json.load(f)
                self.logger.info(f"Loaded {len(articles)} existing articles")
                return articles if isinstance(articles, list) else []
        except Exception as e:
            self.logger.error(f"Error loading existing articles: {e}")
            return []
    
    def safe_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Make safe request with comprehensive error handling and delays"""
        for attempt in range(max_retries):
            try:
                # Progressive delay based on attempt
                if attempt > 0:
                    delay = random.uniform(3 + attempt * 2, 6 + attempt * 3)
                    self.logger.info(f"Retry attempt {attempt + 1} after {delay:.1f}s delay")
                    time.sleep(delay)
                else:
                    # Random delay for first attempt
                    time.sleep(random.uniform(1.5, 3.5))
                
                # Rotate user agent occasionally
                if random.random() < 0.3:
                    ua = UserAgent()
                    self.session.headers['User-Agent'] = ua.chrome
                
                self.logger.debug(f"Requesting: {url}")
                response = self.session.get(url, timeout=30, verify=False)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 403:
                    self.logger.warning(f"403 Forbidden on attempt {attempt + 1} for {url}")
                    if attempt < max_retries - 1:
                        # Longer delay for 403 errors
                        time.sleep(random.uniform(10, 15))
                elif response.status_code == 429:
                    self.logger.warning(f"Rate limited on attempt {attempt + 1} for {url}")
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(15, 25))
                else:
                    self.logger.warning(f"Status {response.status_code} on attempt {attempt + 1} for {url}")
                    
            except requests.Timeout:
                self.logger.error(f"Timeout on attempt {attempt + 1} for {url}")
            except Exception as e:
                self.logger.error(f"Request error on attempt {attempt + 1} for {url}: {e}")
        
        self.stats['errors'] += 1
        return None
    
    def extract_pdf_content(self, pdf_url: str) -> str:
        """Extract and clean text content from PDF"""
        try:
            self.logger.info(f"Extracting PDF content from: {pdf_url}")
            response = self.safe_request(pdf_url)
            
            if not response:
                return ""
            
            # Extract text using PyPDF2
            pdf_file = BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text_content = ""
            for page_num, page in enumerate(pdf_reader.pages):
                try:
                    page_text = page.extract_text()
                    text_content += f"\n--- Page {page_num + 1} ---\n{page_text}"
                except Exception as e:
                    self.logger.warning(f"Error extracting page {page_num + 1}: {e}")
                    continue
            
            # Clean and normalize text
            if text_content:
                # Remove excessive whitespace
                text_content = re.sub(r'\s+', ' ', text_content)
                # Remove unwanted characters but keep essential punctuation
                text_content = re.sub(r'[^\w\s\.,;:!?\-\(\)\[\]{}"\'%$&@#/\\]', '', text_content)
                # Remove page markers if too many
                text_content = re.sub(r'--- Page \d+ ---\s*', '\n\n', text_content)
                text_content = text_content.strip()
                
                self.logger.info(f"Successfully extracted {len(text_content)} characters from PDF")
                self.stats['pdfs_processed'] += 1
                return text_content
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF content from {pdf_url}: {e}")
        
        return ""
    
    def extract_charts_and_tables(self, soup: BeautifulSoup) -> str:
        """Extract information about charts, graphs and tables"""
        chart_table_info = []
        
        # Find tables
        tables = soup.find_all('table')
        for i, table in enumerate(tables, 1):
            caption = table.find('caption')
            caption_text = caption.get_text(strip=True) if caption else f"Table {i}"
            
            # Get table headers
            headers = []
            header_row = table.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
            
            table_info = f"TABLE {i}: {caption_text}"
            if headers:
                table_info += f" | Headers: {', '.join(headers[:5])}"  # First 5 headers
            
            chart_table_info.append(table_info)
        
        # Find chart/graph containers
        chart_selectors = [
            'div[class*="chart"]',
            'div[class*="graph"]',
            'div[class*="visualization"]',
            'img[alt*="chart" i]',
            'img[alt*="graph" i]',
            'figure',
            'div[class*="plotly"]'
        ]
        
        chart_count = 0
        for selector in chart_selectors:
            elements = soup.select(selector)
            for element in elements:
                chart_count += 1
                alt_text = element.get('alt', '') if element.name == 'img' else ''
                title = element.get('title', '')
                figcaption = element.find('figcaption')
                caption = figcaption.get_text(strip=True) if figcaption else ''
                
                chart_info = f"CHART/GRAPH {chart_count}"
                if alt_text:
                    chart_info += f": {alt_text}"
                elif title:
                    chart_info += f": {title}"
                elif caption:
                    chart_info += f": {caption}"
                
                chart_table_info.append(chart_info)
        
        return " | ".join(chart_table_info) if chart_table_info else ""
    
    def extract_article_links(self, soup: BeautifulSoup, page_num: int) -> List[Dict]:
        """Extract article links from listing page"""
        article_links = []
        
        # Look for article links using multiple strategies
        selectors = [
            'a[href*="/articles/"]',
            'h2 a[href*="/articles/"]',
            'h3 a[href*="/articles/"]',
            '.views-row a[href*="/articles/"]',
            '.node-title a[href*="/articles/"]',
            '.field--name-node-title a[href*="/articles/"]'
        ]
        
        found_links = set()
        
        for selector in selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href and '/articles/' in href:
                    full_url = urljoin(self.base_url, href).rstrip('/')
                    
                    # Skip if already found or is the main articles page
                    if full_url in found_links or full_url == self.articles_url:
                        continue
                    
                    # Skip if already exists
                    if full_url in self.existing_urls:
                        self.stats['articles_skipped'] += 1
                        continue
                    
                    headline = link.get_text(strip=True)
                    if headline and len(headline) > 5:
                        article_links.append({
                            'url': full_url,
                            'headline': headline
                        })
                        found_links.add(full_url)
                        self.stats['articles_found'] += 1
        
        self.logger.info(f"Page {page_num}: Found {len(article_links)} new article links")
        return article_links
    
    def extract_article_content(self, article_link: Dict) -> Optional[Article]:
        """Extract comprehensive content from individual article page"""
        url = article_link['url']
        headline = article_link['headline']
        
        self.logger.info(f"Extracting content from: {headline[:60]}...")
        
        response = self.safe_request(url)
        if not response:
            self.logger.error(f"Failed to fetch article content: {url}")
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        try:
            # Generate hash for deduplication
            hash_id = self.generate_hash(url, headline)
            if hash_id in self.existing_hashes:
                self.logger.info(f"Skipping duplicate article (by hash): {headline[:50]}...")
                self.stats['articles_skipped'] += 1
                return None
            
            # Extract better headline from page
            better_headline = headline
            h1_tag = soup.find('h1')
            if h1_tag:
                page_headline = h1_tag.get_text(strip=True)
                if page_headline and len(page_headline) > len(headline):
                    better_headline = page_headline
            
            # Extract published date
            published_date = self.extract_published_date(soup)
            
            # Extract theme/category
            theme = self.extract_theme(soup, url)
            
            # Extract article type
            article_type = self.determine_article_type(url, better_headline, soup)
            
            # Extract main content (comprehensive)
            content_text = self.extract_main_content(soup)
            
            # Extract associated image
            image_url = self.extract_image_url(soup)
            
            # Extract related links (filtered)
            related_links = self.extract_related_links(soup, url)
            
            # Extract charts and tables information
            charts_tables = self.extract_charts_and_tables(soup)
            
            # Extract PDF content if available
            pdf_content = ""
            pdf_links = self.find_pdf_links(soup)
            if pdf_links:
                # Use first PDF as per requirements
                pdf_content = self.extract_pdf_content(pdf_links[0])
            
            # Create article object
            article = Article(
                hash_id=hash_id,
                url=url,
                headline=better_headline,
                published_date=published_date,
                scraped_date=datetime.now(timezone.utc).isoformat(),
                theme=theme,
                article_type=article_type,
                content_text=content_text,
                pdf_content=pdf_content,
                image_url=image_url,
                related_links=related_links,
                charts_and_tables=charts_tables
            )
            
            self.stats['articles_scraped'] += 1
            self.logger.info(f"Successfully scraped: {better_headline[:60]}... [{article_type}]")
            
            return article
            
        except Exception as e:
            self.logger.error(f"Error extracting content from {url}: {e}")
            self.stats['errors'] += 1
            return None
    
    def extract_published_date(self, soup: BeautifulSoup) -> str:
        """Extract published date from article page"""
        # Try multiple methods to find the date
        date_selectors = [
            'meta[name="dcterms.issued"]',
            'meta[property="article:published_time"]',
            'time[datetime]',
            '.field--name-field-abs-release-date time',
            '.date-display-single',
            '.published-date'
        ]
        
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                # Try to get datetime attribute first
                date_value = element.get('datetime') or element.get('content')
                if date_value:
                    return date_value
                
                # Try to get text content
                date_text = element.get_text(strip=True)
                if date_text:
                    return self.parse_date_text(date_text)
        
        return "Unknown"
    
    def parse_date_text(self, date_text: str) -> str:
        """Parse various date formats"""
        # Common ABS date formats
        date_patterns = [
            r'(\d{1,2}\s+\w+\s+\d{4})',  # 24 June 2025
            r'(\d{1,2}/\d{1,2}/\d{4})',  # 24/06/2025
            r'(\d{4}-\d{2}-\d{2})',      # 2025-06-24
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_text)
            if match:
                return match.group(1)
        
        return date_text.strip()
    
    def extract_theme(self, soup: BeautifulSoup, url: str) -> str:
        """Extract article theme/category"""
        # Try breadcrumbs
        breadcrumb = soup.find('nav', class_='breadcrumb') or soup.find('ol', class_='breadcrumb')
        if breadcrumb:
            links = breadcrumb.find_all('a')
            if len(links) > 1:
                return links[-2].get_text(strip=True)
        
        # Try category meta tags
        category_meta = soup.find('meta', {'name': 'category'}) or soup.find('meta', {'property': 'article:section'})
        if category_meta:
            return category_meta.get('content', '')
        
        return ""
    
    def determine_article_type(self, url: str, headline: str, soup: BeautifulSoup) -> str:
        """Determine article type based on URL, headline, and content"""
        url_lower = url.lower()
        headline_lower = headline.lower()
        
        if 'media-release' in url_lower or 'media release' in headline_lower:
            return "Media Release"
        elif 'statistics' in url_lower or 'statistical' in headline_lower:
            return "Statistical Report"
        elif 'survey' in headline_lower or 'census' in headline_lower:
            return "Survey/Census"
        elif 'research' in url_lower or 'research' in headline_lower:
            return "Research"
        elif 'insights' in headline_lower:
            return "Insights"
        elif 'data' in headline_lower:
            return "Data Release"
        
        return "Article"
    
    def extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract comprehensive main content from article"""
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', '.breadcrumb']):
            element.decompose()
        
        # Try multiple content selectors in order of preference
        content_selectors = [
            'main',
            'article',
            '.field--name-body',
            '.content-main',
            '.node__content',
            '.abs-section-content',
            'div[role="main"]'
        ]
        
        content_text = ""
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Get text with proper spacing
                text = content_elem.get_text(separator=' ', strip=True)
                if len(text) > 200:  # Ensure substantial content
                    content_text = text
                    break
        
        # If no content found, try getting all text from body
        if not content_text:
            body = soup.find('body')
            if body:
                content_text = body.get_text(separator=' ', strip=True)
        
        # Clean up the content
        if content_text:
            # Normalize whitespace
            content_text = re.sub(r'\s+', ' ', content_text)
            # Remove common navigation text
            content_text = re.sub(r'Skip to main content|Home|Search ABS|Australian Bureau of Statistics', '', content_text)
            content_text = content_text.strip()
        
        return content_text
    
    def extract_image_url(self, soup: BeautifulSoup) -> str:
        """Extract associated image URL"""
        # Try Open Graph image
        og_image = soup.find('meta', {'property': 'og:image'})
        if og_image and og_image.get('content'):
            return og_image['content']
        
        # Try Twitter card image
        twitter_image = soup.find('meta', {'name': 'twitter:image'})
        if twitter_image and twitter_image.get('content'):
            return twitter_image['content']
        
        # Try to find main article image
        main_content = soup.find('main') or soup.find('article')
        if main_content:
            img = main_content.find('img', src=True)
            if img:
                return urljoin(self.base_url, img['src'])
        
        return ""
    
    def find_pdf_links(self, soup: BeautifulSoup) -> List[str]:
        """Find PDF download links on the page"""
        pdf_links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.lower().endswith('.pdf'):
                full_url = urljoin(self.base_url, href)
                pdf_links.append(full_url)
        
        return pdf_links
    
    def extract_related_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """Extract related article URLs (filtered)"""
        related_links = []
        seen_urls = {current_url}
        
        # Look for links in main content
        main_content = soup.find('main') or soup.find('article') or soup
        
        for link in main_content.find_all('a', href=True):
            href = link['href']
            
            # Skip unwanted file types
            if any(href.lower().endswith(ext) for ext in ['.xlsx', '.csv', '.pdf', '.mp3', '.mp4', '.wav']):
                continue
            
            # Only include HTTP(S) links or ABS internal links
            if href.startswith('http') or href.startswith('/'):
                full_url = urljoin(self.base_url, href) if href.startswith('/') else href
                
                # Only include if not already seen and not current URL
                if full_url not in seen_urls and full_url != current_url:
                    related_links.append(full_url)
                    seen_urls.add(full_url)
                    
                    # Limit to prevent excessive links
                    if len(related_links) >= 10:
                        break
        
        return related_links
    
    def get_next_page_url(self, soup: BeautifulSoup, current_page: int) -> Optional[str]:
        """Get next page URL from pagination"""
        # Look for next page link
        next_selectors = [
            'li.pager__item--next a',
            'a[rel="next"]',
            'a[title*="next" i]',
            '.pager-next a'
        ]
        
        for selector in next_selectors:
            next_link = soup.select_one(selector)
            if next_link and next_link.get('href'):
                href = next_link['href']
                if href.startswith('?'):
                    return f"{self.articles_url}{href}"
                else:
                    return urljoin(self.articles_url, href)
        
        # Try constructing next page URL manually
        next_page = current_page + 1
        return f"{self.articles_url}?page={next_page}"
    
    def scrape_articles(self) -> List[Article]:
        """Main scraping method"""
        all_new_articles = []
        current_page = 0
        
        self.logger.info("="*60)
        self.logger.info("STARTING ABS ARTICLES SCRAPING")
        self.logger.info("="*60)
        
        while current_page < self.max_pages:
            self.logger.info(f"\n--- Processing page {current_page + 1}/{self.max_pages} ---")
            
            # Construct page URL
            if current_page == 0:
                page_url = self.articles_url
            else:
                page_url = f"{self.articles_url}?page={current_page}"
            
            self.logger.info(f"Fetching: {page_url}")
            
            # Get page content
            response = self.safe_request(page_url)
            if not response:
                self.logger.error(f"Failed to fetch page {current_page + 1}")
                break
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract article links
            article_links = self.extract_article_links(soup, current_page + 1)
            
            if not article_links:
                self.logger.warning(f"No new articles found on page {current_page + 1} - stopping")
                break
            
            # Process each article
            for i, article_link in enumerate(article_links, 1):
                self.logger.info(f"\nProcessing article {i}/{len(article_links)}: {article_link['headline'][:50]}...")
                
                article = self.extract_article_content(article_link)
                if article:
                    all_new_articles.append(article)
                    # Update tracking sets
                    self.existing_urls.add(article.url)
                    self.existing_hashes.add(article.hash_id)
                
                # Random delay between articles
                time.sleep(random.uniform(2, 5))
            
            self.stats['pages_processed'] += 1
            current_page += 1
            
            # Longer delay between pages
            time.sleep(random.uniform(5, 10))
        
        # Final statistics
        self.logger.info("\n" + "="*60)
        self.logger.info("SCRAPING COMPLETED")
        self.logger.info("="*60)
        self.logger.info(f"Pages processed: {self.stats['pages_processed']}")
        self.logger.info(f"Articles found: {self.stats['articles_found']}")
        self.logger.info(f"Articles scraped: {self.stats['articles_scraped']}")
        self.logger.info(f"Articles skipped: {self.stats['articles_skipped']}")
        self.logger.info(f"PDFs processed: {self.stats['pdfs_processed']}")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info("="*60)
        
        return all_new_articles
    
    def save_data(self, new_articles: List[Article]):
        """Save articles to JSON and CSV files"""
        if not new_articles:
            self.logger.info("No new articles to save")
            return
        
        # Convert new articles to dict format
        new_articles_data = [asdict(article) for article in new_articles]
        
        # Combine with existing articles
        all_articles = self.existing_articles + new_articles_data
        
        # Remove duplicates based on hash_id
        seen_hashes = set()
        unique_articles = []
        for article in all_articles:
            hash_id = article.get('hash_id')
            if hash_id and hash_id not in seen_hashes:
                seen_hashes.add(hash_id)
                unique_articles.append(article)
        
        all_articles = unique_articles
        
        # Sort by published date (newest first)
        def sort_key(article):
            pub_date = article.get('published_date', 'Unknown')
            if pub_date != 'Unknown':
                try:
                    # Try to parse date for proper sorting
                    return datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                except:
                    pass
            # Fallback to scraped date
            scraped = article.get('scraped_date', '')
            try:
                return datetime.fromisoformat(scraped.replace('Z', '+00:00'))
            except:
                return datetime.min
        
        all_articles.sort(key=sort_key, reverse=True)
        
        # Save to JSON
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved {len(all_articles)} total articles to {self.json_file}")
        except Exception as e:
            self.logger.error(f"Error saving JSON file: {e}")
            return
        
        # Save to CSV (excluding content_html as requested)
        try:
            fieldnames = [
                'hash_id', 'url', 'headline', 'published_date', 'scraped_date',
                'theme', 'article_type', 'content_text', 'pdf_content', 
                'image_url', 'related_links', 'charts_and_tables'
            ]
            
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for article in all_articles:
                    row = {}
                    for field in fieldnames:
                        value = article.get(field, '')
                        
                        # Special handling for related_links (convert list to pipe-separated string)
                        if field == 'related_links' and isinstance(value, list):
                            row[field] = '|'.join(value)
                        else:
                            row[field] = value
                    
                    writer.writerow(row)
            
            self.logger.info(f"Saved {len(all_articles)} total articles to {self.csv_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving CSV file: {e}")
        
        # Print summary
        self.print_summary(new_articles, all_articles)
    
    def print_summary(self, new_articles: List[Article], all_articles: List[Dict]):
        """Print detailed summary of scraping results"""
        self.logger.info("\n" + "="*60)
        self.logger.info("SUMMARY REPORT")
        self.logger.info("="*60)
        
        self.logger.info(f"New articles added: {len(new_articles)}")
        self.logger.info(f"Total articles in database: {len(all_articles)}")
        
        if new_articles:
            # Article types breakdown
            article_types = {}
            for article in new_articles:
                art_type = article.article_type or 'Unknown'
                article_types[art_type] = article_types.get(art_type, 0) + 1
            
            self.logger.info("\nNew articles by type:")
            for art_type, count in sorted(article_types.items(), key=lambda x: x[1], reverse=True):
                self.logger.info(f"  - {art_type}: {count}")
            
            # Sample of new articles
            self.logger.info("\nSample of new articles:")
            for article in new_articles[:5]:
                self.logger.info(f"  - {article.headline[:70]}...")
                self.logger.info(f"    Date: {article.published_date}")
                self.logger.info(f"    Type: {article.article_type}")
                self.logger.info(f"    Theme: {article.theme}")
                if article.pdf_content:
                    self.logger.info(f"    PDF: Yes ({len(article.pdf_content)} chars)")
                if article.charts_and_tables:
                    self.logger.info(f"    Charts/Tables: Yes")
        
        self.logger.info("\n" + "="*60)
    
    def run(self):
        """Main execution method"""
        try:
            start_time = datetime.now()
            self.logger.info(f"ABS Articles Scraper started at {start_time}")
            
            # Scrape articles
            new_articles = self.scrape_articles()
            
            # Save results
            self.save_data(new_articles)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info(f"\n✅ Scraping completed successfully!")
            self.logger.info(f"Total execution time: {duration}")
            
        except KeyboardInterrupt:
            self.logger.info("\n⚠️ Scraping interrupted by user")
            
        except Exception as e:
            self.logger.error(f"\n❌ Unexpected error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
        finally:
            # Close session
            if hasattr(self, 'session') and self.session:
                self.session.close()


def main():
    """Main function with configuration"""
    # Configuration - modify these values as needed
    MAX_PAGES = 3           # Set to 36 for first run, 3 for daily runs
    FIRST_RUN = False       # Set to True for initial comprehensive scrape
    
    # Create and run scraper
    scraper = ABSArticlesScraper(max_pages=MAX_PAGES, first_run=FIRST_RUN)
    scraper.run()


if __name__ == "__main__":
    main()