#!/usr/bin/env python3
"""
AICIS News & Notices Scraper

Scrapes news and notices from the Australian Industrial Chemicals Introduction Scheme (AICIS) website
with anti-bot measures and comprehensive data extraction for LLM processing.
"""

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Website URLs
BASE_URL = "https://www.industrialchemicals.gov.au"
TARGET_URL = f"{BASE_URL}/news-and-notices"

# Output settings
DATA_DIR = "./data"
OUTPUT_FILE = "aicis_news.json"
LOG_FILE = "aicis_scrape.log"

# Deduplication settings
CONSECUTIVE_EXISTING_LIMIT = 5  # Stop after finding 5 consecutive existing articles

# Request settings
REQUEST_DELAY = 1  # Seconds between requests
TIMEOUT = 30       # Request timeout in seconds
MAX_PDF_SIZE_MB = 50  # Maximum PDF size to download

# Content filtering
MIN_CONTENT_LENGTH = 50  # Minimum content length to consider valid

# ============================================================================

import os
import json
import hashlib
import logging
import time
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import PyPDF2
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class AICISNewsScraper:
    """Main scraper class for AICIS news and notices."""
    
    def __init__(self, debug=False):
        """Initialize the scraper."""
        self.base_url = BASE_URL
        self.target_url = TARGET_URL
        self.data_dir = Path(DATA_DIR)
        self.data_file = self.data_dir / OUTPUT_FILE
        self.log_file = self.data_dir / LOG_FILE
        self.debug = debug
        
        # Create data directory
        self.data_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize session and driver
        self.session = None
        self.driver = None
        self.existing_articles = self._load_existing_articles()
        
        # Statistics
        self.stats = {
            'articles_found': 0,
            'new_articles': 0,
            'existing_articles': 0,
            'consecutive_existing': 0,
            'errors': 0,
            'start_time': datetime.now().isoformat()
        }
    
    def _setup_logging(self):
        """Setup logging configuration."""
        log_level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_existing_articles(self) -> Dict[str, Dict]:
        """Load existing articles from JSON file."""
        if self.data_file.exists():
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {article['url']: article for article in data.get('articles', [])}
            except Exception as e:
                self.logger.error(f"Error loading existing articles: {e}")
        return {}
    
    def _save_articles(self, articles: List[Dict]):
        """Save articles to JSON file."""
        # Ensure all datetime objects in stats are converted to strings
        stats_copy = {}
        for key, value in self.stats.items():
            if isinstance(value, datetime):
                stats_copy[key] = value.isoformat()
            else:
                stats_copy[key] = value
        
        output_data = {
            'scrape_metadata': {
                'last_updated': datetime.now().isoformat(),
                'total_articles': len(articles),
                'scraper_version': '1.0',
                'source_url': self.target_url,
                'stats': stats_copy
            },
            'articles': articles
        }
        
        with open(self.data_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)
        
        self.logger.info(f"Saved {len(articles)} articles to {self.data_file}")
    
    def _setup_session(self):
        """Setup requests session with retry strategy and realistic headers."""
        self.session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Realistic headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _setup_driver(self):
        """Setup Chrome driver with stealth options and Linux compatibility"""
        try:
            chrome_options = Options()
            
            # Essential stability options for Linux/WSL
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-features=TranslateUI")
            chrome_options.add_argument("--disable-ipc-flooding-protection")
            
            # Stealth options
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Set realistic window size
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Updated user agent to match your Chrome version
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
            
            # Enable headless for faster execution
            chrome_options.add_argument("--headless=new")
            
            # Memory and performance optimizations
            chrome_options.add_argument("--max_old_space_size=4096")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            
            # Network optimizations
            chrome_options.add_argument("--aggressive-cache-discard")
            chrome_options.add_argument("--disable-background-networking")
            
            # Let Selenium find Chrome automatically via PATH
            self.logger.info("Using system default Chrome binary (auto-detection)")
            
            # Try to find ChromeDriver
            possible_chromedriver_paths = [
                "/usr/bin/chromedriver",
                "/usr/local/bin/chromedriver",
                "/snap/bin/chromedriver"
            ]
            
            chromedriver_path = None
            for path in possible_chromedriver_paths:
                if os.path.exists(path):
                    chromedriver_path = path
                    self.logger.info(f"Found ChromeDriver at: {path}")
                    break
            
            # Initialize driver with improved service configuration
            service_kwargs = {}
            if chromedriver_path:
                service_kwargs['executable_path'] = chromedriver_path
            
            # Add service arguments for better stability
            service_kwargs['service_args'] = [
                '--verbose',
                '--whitelisted-ips=',
                '--disable-dev-shm-usage'
            ]
            
            service = Service(**service_kwargs)
            
            # Initialize driver with explicit service and options
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info(f"Chrome driver initialized successfully")
            if chromedriver_path:
                self.logger.info(f"Using ChromeDriver: {chromedriver_path}")
            else:
                self.logger.info("Using ChromeDriver from PATH")
            
            # Get session cookies by visiting homepage first
            self.logger.info("Initializing session by visiting homepage...")
            self.driver.get(self.base_url)
            time.sleep(3)
            
            # Transfer cookies to requests session
            if self.session:
                for cookie in self.driver.get_cookies():
                    self.session.cookies.set(cookie['name'], cookie['value'])
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            
            # Detailed troubleshooting
            self.logger.error("Troubleshooting information:")
            
            # Check if chromedriver is accessible
            try:
                result = subprocess.run(['chromedriver', '--version'], capture_output=True, text=True, timeout=5)
                self.logger.info(f"ChromeDriver version: {result.stdout.strip()}")
            except Exception as cmd_e:
                self.logger.error(f"Cannot run chromedriver command: {cmd_e}")
            
            # Check Chrome version
            try:
                result = subprocess.run(['google-chrome', '--version'], capture_output=True, text=True, timeout=5)
                self.logger.info(f"Chrome version: {result.stdout.strip()}")
            except Exception as chrome_e:
                self.logger.error(f"Cannot run chrome command: {chrome_e}")
            
            # Don't raise - we can try to continue with requests session only
            self.driver = None
            self.logger.warning("Continuing without driver - using requests session only")
            return False
    
    def _get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Get page content using both driver and session fallback."""
        try:
            # Try with driver first (for anti-bot measures)
            if self.driver:
                self.logger.debug(f"Using Chrome driver for: {url}")
                self.driver.get(url)
                time.sleep(2)
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                if soup.find('div', class_='content') or soup.find('article'):  # Check if we got valid content
                    return soup
                else:
                    self.logger.warning(f"Chrome driver got invalid content for: {url}")
            
            # Fallback to requests session
            if self.session:
                self.logger.debug(f"Using requests session for: {url}")
                self.session.headers['Referer'] = self.base_url
                response = self.session.get(url, timeout=TIMEOUT)
                response.raise_for_status()
                
                self.logger.debug(f"Response status: {response.status_code}, Content length: {len(response.content)}")
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if we got blocked (common anti-bot responses)
                if soup.find('title') and '403' in soup.find('title').get_text():
                    self.logger.warning(f"Got 403 response for: {url}")
                elif soup.find('title') and any(blocked in soup.find('title').get_text().lower() 
                                               for blocked in ['blocked', 'denied', 'forbidden']):
                    self.logger.warning(f"Got blocked response for: {url}")
                
                return soup
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                self.logger.error(f"Got 403 Forbidden for {url} - site may have anti-bot protection")
            else:
                self.logger.error(f"HTTP error getting page content for {url}: {e}")
            self.stats['errors'] += 1
        except Exception as e:
            self.logger.error(f"Error getting page content for {url}: {e}")
            self.stats['errors'] += 1
        
        return None
    
    def _extract_article_links(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract article metadata from the main news page."""
        articles = []
        
        self.logger.debug("Looking for articles on main page...")
        
        # Debug: log page title to confirm we got the right page
        title = soup.find('title')
        if title:
            self.logger.debug(f"Page title: {title.get_text()}")
        
        # Find all article elements - looking for both news and notices
        article_selectors = [
            'article.node--type-news',  # Standard news articles
            '.list__item article',      # Articles within list items
            '.erd-list--teaser-small .list__item',  # Teaser format articles
            '.paragraph-content .list__item',  # From the HTML structure you provided
            'div[role="listitem"] article'  # Role-based selection
        ]
        
        total_found = 0
        
        for selector in article_selectors:
            article_elements = soup.select(selector)
            self.logger.debug(f"Selector '{selector}' found {len(article_elements)} elements")
            total_found += len(article_elements)
            
            for article in article_elements:
                try:
                    # Extract headline and URL
                    headline = ""
                    url = ""
                    
                    # Find the title link
                    title_selectors = [
                        'h3.teaser__title a',
                        '.teaser__title a',
                        'h3 a',
                        '.layout__region--title a',
                        '.block--entity-field-node-title a'  # From HTML structure
                    ]
                    
                    for title_selector in title_selectors:
                        title_elem = article.select_one(title_selector)
                        if title_elem:
                            headline = title_elem.get_text().strip()
                            href = title_elem.get('href')
                            if href:
                                url = urljoin(self.base_url, href)
                            self.logger.debug(f"Found article: {headline[:50]}...")
                            break
                    
                    # Extract published date
                    published_date = None
                    date_selectors = [
                        'time[datetime]',
                        '.teaser__date time',
                        '.field--name-field-published-date time',
                        '.health-field--name-field-published-date time'
                    ]
                    
                    for date_selector in date_selectors:
                        date_elem = article.select_one(date_selector)
                        if date_elem:
                            published_date = date_elem.get('datetime') or date_elem.get_text().strip()
                            break
                    
                    # Extract summary/teaser text
                    summary = ""
                    summary_selectors = [
                        '.teaser__summary',
                        '.field--name-body',
                        '.layout__region--summary',
                        '.health-field--name-body'
                    ]
                    
                    for summary_selector in summary_selectors:
                        summary_elem = article.select_one(summary_selector)
                        if summary_elem:
                            summary = summary_elem.get_text().strip()[:200]  # Truncate summary
                            break
                    
                    # Extract image URL
                    image_url = None
                    img_selectors = [
                        '.teaser__image img',
                        '.field--name-field-thumbnail img',
                        '.health-field--name-field-media-image img',
                        'img[src]'
                    ]
                    
                    for img_selector in img_selectors:
                        img_elem = article.select_one(img_selector)
                        if img_elem and img_elem.get('src'):
                            image_url = urljoin(self.base_url, img_elem['src'])
                            break
                    
                    if url and headline:
                        articles.append({
                            'url': url,
                            'headline': headline,
                            'published_date': published_date,
                            'summary': summary,
                            'image_url': image_url
                        })
                        self.logger.debug(f"Added article: {headline[:30]}...")
                
                except Exception as e:
                    self.logger.warning(f"Error extracting article metadata: {e}")
                    continue
        
        self.logger.info(f"Total elements found across selectors: {total_found}")
        
        # If we didn't find any articles with the specific selectors, try a broader search
        if not articles:
            self.logger.warning("No articles found with specific selectors, trying broader search...")
            
            # Try to find any links that look like news/notices
            all_links = soup.find_all('a', href=True)
            news_patterns = ['/news-and-notices/', '/news/', '/notices/']
            
            for link in all_links:
                href = link.get('href')
                if href and any(pattern in href for pattern in news_patterns):
                    if href.startswith('/'):  # Relative URL
                        full_url = urljoin(self.base_url, href)
                        headline = link.get_text().strip()
                        if headline and len(headline) > 10:  # Filter out navigation links
                            articles.append({
                                'url': full_url,
                                'headline': headline,
                                'published_date': None,
                                'summary': '',
                                'image_url': None
                            })
            
            self.logger.info(f"Broader search found {len(articles)} potential articles")
        
        # Remove duplicates based on URL
        seen_urls = set()
        unique_articles = []
        for article in articles:
            if article['url'] not in seen_urls:
                seen_urls.add(article['url'])
                unique_articles.append(article)
        
        self.logger.info(f"Final unique articles: {len(unique_articles)}")
        
        return unique_articles
    
    def _extract_content_text(self, soup: BeautifulSoup) -> str:
        """Extract clean content text suitable for LLM processing."""
        content_text = []
        
        # Remove unwanted elements completely from the soup first
        unwanted_selectors = [
            'nav', 'footer', '.social-share', '.pager', '.breadcrumb', 
            '.health-toolbar', '.au-breadcrumbs', '.health-band--boxed',
            '.block--system-breadcrumb-block', '.au-direction-link',
            'script', 'style', '.visually-hidden', '.sr-only'
        ]
        
        for selector in unwanted_selectors:
            for elem in soup.select(selector):
                elem.decompose()
        
        # Target the main article content specifically
        main_content_selectors = [
            # Most specific - the actual article body content
            'article .field--name-body .health-field__item',
            '.node--view-mode-full .field--name-body .health-field__item',
            
            # Medium specific - article body areas
            'article .field--name-body',
            '.layout__region--content .field--name-body',
            
            # Paragraph content within articles
            '.paragraph-content .field--name-body',
            
            # Fallback - any content within articles
            'article .content'
        ]
        
        # Track processed content to avoid duplicates
        processed_content = set()
        
        for selector in main_content_selectors:
            content_elements = soup.select(selector)
            self.logger.debug(f"Selector '{selector}' found {len(content_elements)} elements")
            
            for elem in content_elements:
                # Get text with proper spacing
                text = elem.get_text(separator='\n', strip=True)
                
                if not text or len(text) < MIN_CONTENT_LENGTH:
                    continue
                
                # Create content hash to avoid duplicates
                content_hash = hashlib.md5(text.encode()).hexdigest()
                
                if content_hash not in processed_content:
                    content_text.append(text)
                    processed_content.add(content_hash)
                    self.logger.debug(f"Added content block: {text[:50]}...")
        
        # Combine all content
        combined_text = '\n\n'.join(content_text)
        
        # Additional cleanup for known footer/header patterns
        cleanup_patterns = [
            # Registration banner
            r'Registration for 2025–26 is now open\s*for the period.*?31 August 2026\.?',
            # Footer navigation
            r'Contact us\s*About us\s*Careers.*?© Australian Industrial Chemicals Introduction Scheme',
            # Search sections
            r'Additional Searches\s*Choose from 5 options:.*?Search the register',
            # Newsletter signup
            r'Email\s*\(required\)\s*Name of organisation.*?Subscribe',
            # Footer links section
            r'Privacy\s*Accessibility\s*Disclaimer.*?Information Publication Scheme',
            # Duplicate titles (sometimes the title appears multiple times)
            r'^([^\n]+)\n\n\1',  # Remove duplicate titles
        ]
        
        for pattern in cleanup_patterns:
            combined_text = re.sub(pattern, '', combined_text, flags=re.DOTALL | re.MULTILINE)
        
        # Clean up whitespace
        combined_text = re.sub(r'\n\s*\n\s*\n', '\n\n', combined_text)
        combined_text = re.sub(r'[ \t]+', ' ', combined_text)
        
        return combined_text.strip()
    
    def _clean_extracted_text(self, text: str) -> str:
        """Clean extracted text by removing duplicates and unwanted content."""
        if not text:
            return ""
        
        # Split into paragraphs
        paragraphs = text.split('\n\n')
        
        # Remove duplicate paragraphs
        unique_paragraphs = []
        seen_paragraphs = set()
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            # Create a normalized version for comparison (remove extra spaces, case insensitive)
            normalized = ' '.join(para.lower().split())
            
            # Skip if we've seen this paragraph before
            if normalized in seen_paragraphs:
                continue
            
            # Skip common navigation/footer content
            navigation_keywords = [
                'contact us', 'about us', 'careers', 'newsletter', 'privacy policy',
                'terms of use', 'accessibility', 'disclaimer', 'copyright',
                'freedom of information', 'search chemicals', 'search assessments',
                'additional searches', 'choose from 5 options', 'registered businesses',
                'risk management recommendations register', 'australian industrial chemicals'
            ]
            
            if any(keyword in normalized for keyword in navigation_keywords):
                continue
            
            # Skip very short paragraphs (likely navigation)
            if len(para) < 20:
                continue
            
            unique_paragraphs.append(para)
            seen_paragraphs.add(normalized)
        
        # Join unique paragraphs
        cleaned_text = '\n\n'.join(unique_paragraphs)
        
        # Final cleanup
        cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)  # No more than 2 consecutive newlines
        cleaned_text = re.sub(r'[ \t]{2,}', ' ', cleaned_text)   # No excessive spaces
        
        return cleaned_text.strip()
    
    def _extract_related_links(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract related links from content (excluding navigation and ads)."""
        links = []
        
        # Only look for links within the main article content
        content_selectors = [
            '.field--name-body',
            'article .content .field--name-body',
            '.layout__region--content .field--name-body',
            '.paragraph-content .field--name-body'
        ]
        
        # Navigation/footer link patterns to exclude
        exclude_patterns = [
            'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
            'mailto:', '#', 'javascript:', 'tel:',
            '/about-us', '/contact-us', '/careers', '/privacy', '/accessibility',
            '/disclaimer', '/copyright', '/terms-use', '/freedom-of-information',
            '/information-publication-scheme', '/applications-and-forms',
            '/glossary', '/fees', '/newsletter', '/consultations',
            'search-inventory', 'search-assessments', 'search-registered-businesses',
            'risk-management-recommendations-register', '/reporting-and-record-keeping',
            '/help-and-guides', '/updates-and-corrections'
        ]
        
        # Navigation link text patterns to exclude
        exclude_text_patterns = [
            'contact us', 'about us', 'careers', 'newsletter', 'privacy', 'accessibility',
            'disclaimer', 'copyright', 'terms of use', 'freedom of information',
            'search chemicals', 'search assessments', 'search aicis', 'search the register',
            'login', 'subscribe', 'have your say', 'open consultations',
            'applications and forms', 'categorise your introduction',
            'reporting and record keeping', 'updates and corrections'
        ]
        
        for selector in content_selectors:
            content_areas = soup.select(selector)
            for content_area in content_areas:
                for link in content_area.find_all('a', href=True):
                    href = link.get('href')
                    text = link.get_text().strip()
                    
                    # Skip empty links
                    if not text or not href or len(text) < 3:
                        continue
                    
                    # Skip links with excluded patterns
                    if any(pattern in href.lower() for pattern in exclude_patterns):
                        continue
                    
                    # Skip links with excluded text patterns
                    if any(pattern in text.lower() for pattern in exclude_text_patterns):
                        continue
                    
                    # Skip very long link text (likely navigation)
                    if len(text) > 100:
                        continue
                    
                    # Only include links that are likely content-related
                    # Look for document links, assessments, specific chemicals, etc.
                    content_indicators = [
                        '.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx',
                        'assessment', 'evaluation', 'chemical', 'statement',
                        'certificate', 'report', 'list', 'inventory',
                        'cas number', 'regulation', 'guideline'
                    ]
                    
                    is_content_link = any(indicator in href.lower() or indicator in text.lower() 
                                        for indicator in content_indicators)
                    
                    if is_content_link:
                        # Make absolute URL
                        full_url = urljoin(self.base_url, href)
                        
                        links.append({
                            'text': text,
                            'url': full_url,
                            'type': self._classify_link_type(href)
                        })
        
        return links
    
    def _classify_link_type(self, href: str) -> str:
        """Classify link type based on URL."""
        href_lower = href.lower()
        
        if href_lower.endswith('.pdf'):
            return 'pdf'
        elif href_lower.endswith(('.xlsx', '.xls')):
            return 'excel'
        elif href_lower.endswith('.csv'):
            return 'csv'
        elif href_lower.endswith(('.jpg', '.jpeg', '.png', '.gif')):
            return 'image'
        else:
            return 'webpage'
    
    def _extract_tables_and_charts(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract table and chart data from HTML."""
        tables_data = []
        
        # Find all tables within the main content
        content_area = soup.find('div', class_='field--name-body') or soup
        tables = content_area.find_all('table')
        
        for i, table in enumerate(tables):
            try:
                # Convert table to structured data
                table_data = {
                    'table_index': i,
                    'headers': [],
                    'rows': [],
                    'raw_html': str(table)
                }
                
                # Extract table rows
                rows = table.find_all('tr')
                
                for row_idx, row in enumerate(rows):
                    cells = row.find_all(['th', 'td'])
                    row_data = []
                    
                    for cell in cells:
                        cell_text = cell.get_text(strip=True)
                        # Clean up chemical formulas and preserve subscripts
                        cell_text = re.sub(r'<sub>([^<]+)</sub>', r'_\1', cell_text)
                        row_data.append(cell_text)
                    
                    if row_data:
                        if row_idx == 0 and all(cell.name == 'th' for cell in cells):
                            # Header row
                            table_data['headers'] = row_data
                        else:
                            table_data['rows'].append(row_data)
                
                # For tables without explicit headers, use first row as headers if it looks like headers
                if not table_data['headers'] and table_data['rows']:
                    first_row = table_data['rows'][0]
                    if any(keyword in ' '.join(first_row).lower() for keyword in 
                          ['cas', 'chemical', 'name', 'formula', 'date', 'number']):
                        table_data['headers'] = first_row
                        table_data['rows'] = table_data['rows'][1:]
                
                # Convert to list of dictionaries if we have headers
                if table_data['headers'] and table_data['rows']:
                    structured_rows = []
                    for row in table_data['rows']:
                        if len(row) >= len(table_data['headers']):
                            row_dict = {}
                            for j, header in enumerate(table_data['headers']):
                                if j < len(row):
                                    row_dict[header] = row[j]
                            structured_rows.append(row_dict)
                    table_data['structured_data'] = structured_rows
                
                if table_data['rows'] or table_data['headers']:
                    tables_data.append(table_data)
                    
            except Exception as e:
                self.logger.warning(f"Error extracting table {i}: {e}")
        
        return tables_data
    
    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download and extract text from PDF."""
        try:
            # Check file size first
            head_response = self.session.head(pdf_url, timeout=TIMEOUT)
            content_length = head_response.headers.get('content-length')
            if content_length and int(content_length) > MAX_PDF_SIZE_MB * 1024 * 1024:
                self.logger.warning(f"PDF too large ({content_length} bytes), skipping: {pdf_url}")
                return None
                
            response = self.session.get(pdf_url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Save temporarily and extract text
            temp_path = self.data_dir / "temp.pdf"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            # Extract text using PyPDF2
            text_content = []
            with open(temp_path, 'rb') as f:
                pdf_reader = PyPDF2.PdfReader(f)
                for page in pdf_reader.pages:
                    text = page.extract_text()
                    if text.strip():
                        text_content.append(text)
            
            # Clean up temp file
            temp_path.unlink()
            
            # Clean and return text
            full_text = '\n\n'.join(text_content)
            # Remove excessive whitespace
            full_text = re.sub(r'\n\s*\n', '\n\n', full_text)
            full_text = re.sub(r'[ \t]+', ' ', full_text)
            
            return full_text.strip()
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF text from {pdf_url}: {e}")
            return None
    
    def _extract_csv_data(self, csv_url: str) -> Optional[List[Dict]]:
        """Download and extract data from CSV."""
        try:
            response = self.session.get(csv_url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Parse CSV
            df = pd.read_csv(response.content.decode('utf-8'))
            return df.to_dict('records')
            
        except Exception as e:
            self.logger.error(f"Error extracting CSV data from {csv_url}: {e}")
            return None
    
    def _extract_excel_data(self, excel_url: str) -> Optional[Dict]:
        """Download and extract data from Excel file."""
        try:
            response = self.session.get(excel_url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Save temporarily and read Excel
            temp_path = self.data_dir / "temp.xlsx"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            # Read all sheets
            excel_data = {}
            with pd.ExcelFile(temp_path) as xls:
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    excel_data[sheet_name] = df.to_dict('records')
            
            # Clean up temp file
            temp_path.unlink()
            
            return excel_data
            
        except Exception as e:
            self.logger.error(f"Error extracting Excel data from {excel_url}: {e}")
            return None
    
    def _generate_article_hash(self, url: str, headline: str) -> str:
        """Generate hash for deduplication."""
        return hashlib.md5(f"{url}:{headline}".encode()).hexdigest()
    
    def _scrape_article(self, article_meta: Dict) -> Optional[Dict]:
        """Scrape individual article."""
        try:
            article_url = article_meta['url']
            self.logger.info(f"Scraping article: {article_url}")
            
            # Check if already exists
            if article_url in self.existing_articles:
                self.logger.info(f"Article already exists, skipping: {article_url}")
                self.stats['existing_articles'] += 1
                self.stats['consecutive_existing'] += 1
                return None
            
            # Reset consecutive counter on new article
            self.stats['consecutive_existing'] = 0
            
            soup = self._get_page_content(article_url)
            if not soup:
                return None
            
            # Extract content text
            raw_content_text = self._extract_content_text(soup)
            content_text = self._clean_extracted_text(raw_content_text)
            
            # Skip if no meaningful content after cleaning
            if not content_text or len(content_text) < MIN_CONTENT_LENGTH:
                self.logger.warning(f"Insufficient content after cleaning for {article_url}")
                return None
            
            # Extract additional data
            related_links = self._extract_related_links(soup)
            table_data = self._extract_tables_and_charts(soup)
            
            # Extract theme if available (from breadcrumbs or categories)
            theme = None
            theme_selectors = [
                '.au-breadcrumbs a:last-child',
                '.field--name-field-category',
                '.field--name-field-topic'
            ]
            
            for theme_selector in theme_selectors:
                theme_elem = soup.select_one(theme_selector)
                if theme_elem:
                    theme = theme_elem.get_text().strip()
                    break
            
            # Process attachments
            pdf_text = []
            csv_data = []
            excel_data = {}
            
            for link in related_links:
                if link['type'] == 'pdf':
                    pdf_content = self._extract_pdf_text(link['url'])
                    if pdf_content:
                        pdf_text.append({
                            'filename': link['text'],
                            'content': pdf_content
                        })
                elif link['type'] == 'csv':
                    csv_content = self._extract_csv_data(link['url'])
                    if csv_content:
                        csv_data.append({
                            'filename': link['text'],
                            'data': csv_content
                        })
                elif link['type'] == 'excel':
                    excel_content = self._extract_excel_data(link['url'])
                    if excel_content:
                        excel_data[link['text']] = excel_content
                
                # Rate limiting between attachments
                time.sleep(REQUEST_DELAY)
            
            article_data = {
                'url': article_url,
                'headline': article_meta.get('headline', ''),
                'published_date': article_meta.get('published_date'),
                'scraped_date': datetime.now().isoformat(),
                'theme': theme,
                'content_text': content_text,
                'related_links': related_links,
                'associated_image_url': article_meta.get('image_url'),
                'pdf_text': pdf_text,
                'table_and_chart_data': table_data,
                'csv_data': csv_data,
                'excel_data': excel_data,
                'content_hash': self._generate_article_hash(article_url, article_meta.get('headline', ''))
            }
            
            self.stats['new_articles'] += 1
            return article_data
            
        except Exception as e:
            self.logger.error(f"Error scraping article {article_meta.get('url', 'unknown')}: {e}")
            self.stats['errors'] += 1
            return None
    
    def scrape_all_articles(self) -> List[Dict]:
        """Main scraping method."""
        self.logger.info("Starting AICIS news & notices scraper")
        
        # Setup session and driver
        self._setup_session()
        self._setup_driver()
        
        all_articles = list(self.existing_articles.values())
        
        try:
            # Get the main news page
            self.logger.info(f"Scraping main page: {self.target_url}")
            soup = self._get_page_content(self.target_url)
            
            if not soup:
                self.logger.error("Failed to get main page content")
                return all_articles
            
            # Extract article links from main page
            article_metas = self._extract_article_links(soup)
            self.stats['articles_found'] = len(article_metas)
            
            self.logger.info(f"Found {len(article_metas)} articles on main page")
            
            if not article_metas:
                self.logger.warning("No articles found on main page")
                return all_articles
            
            # Scrape each article
            for i, article_meta in enumerate(article_metas):
                # Check consecutive existing limit
                if self.stats['consecutive_existing'] >= CONSECUTIVE_EXISTING_LIMIT:
                    self.logger.info(f"Found {CONSECUTIVE_EXISTING_LIMIT} consecutive existing articles, stopping")
                    break
                
                article_data = self._scrape_article(article_meta)
                if article_data:
                    # Remove existing version if updating
                    all_articles = [a for a in all_articles if a['url'] != article_data['url']]
                    all_articles.append(article_data)
                
                # Rate limiting between articles
                time.sleep(REQUEST_DELAY)
                
                # Progress logging
                if (i + 1) % 10 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(article_metas)} articles")
        
        finally:
            # Cleanup
            if self.driver:
                self.driver.quit()
        
        # Save results
        self._save_articles(all_articles)
        
        # Log final statistics
        self.stats['end_time'] = datetime.now().isoformat()
        start_time = self.stats.get('start_time')
        if isinstance(start_time, str):
            start_dt = datetime.fromisoformat(start_time)
            self.stats['duration'] = (datetime.now() - start_dt).total_seconds()
        
        self.logger.info("Scraping completed!")
        self.logger.info(f"Statistics: {self.stats}")
        
        return all_articles


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='AICIS News & Notices Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Configuration:
  Target URL: {TARGET_URL}
  Consecutive limit: {CONSECUTIVE_EXISTING_LIMIT}
  Request delay: {REQUEST_DELAY}s
  Data directory: {DATA_DIR}
  Output file: {OUTPUT_FILE}

Examples:
  python aicis_news_scraper.py                    # Run scraper
  python aicis_news_scraper.py --debug            # Run with debug logging
  python aicis_news_scraper.py --show-stats       # Show statistics from last run
  python aicis_news_scraper.py --test-connection  # Test connection only
        """
    )
    
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--show-stats', action='store_true',
                       help='Show statistics from last scraping run')
    parser.add_argument('--test-connection', action='store_true',
                       help='Test connection to target URL and exit')
    
    args = parser.parse_args()
    
    # Test connection if requested
    if args.test_connection:
        print("Testing connection to AICIS website...")
        try:
            import requests
            response = requests.get(TARGET_URL, timeout=10)
            print(f"Status code: {response.status_code}")
            print(f"Content length: {len(response.content)} bytes")
            if response.status_code == 200:
                print("✓ Connection successful!")
                
                # Check for content indicators
                soup = BeautifulSoup(response.content, 'html.parser')
                if soup.find('title'):
                    print(f"Page title: {soup.find('title').get_text()}")
                
                articles = soup.select('article')
                print(f"Found {len(articles)} article elements")
                
                links = soup.find_all('a', href=True)
                news_links = [l for l in links if '/news-and-notices/' in l.get('href', '')]
                print(f"Found {len(news_links)} potential news links")
                
            else:
                print(f"⚠ Got status code: {response.status_code}")
        except Exception as e:
            print(f"✗ Connection failed: {e}")
        return
    
    # Show statistics if requested
    if args.show_stats:
        data_file = Path(DATA_DIR) / OUTPUT_FILE
        if data_file.exists():
            with open(data_file, 'r') as f:
                data = json.load(f)
                metadata = data.get('scrape_metadata', {})
                stats = metadata.get('stats', {})
                
                print("Last Scraping Run Statistics:")
                print("=" * 50)
                print(f"Last updated: {metadata.get('last_updated', 'Unknown')}")
                print(f"Total articles: {metadata.get('total_articles', 0)}")
                print(f"Articles found: {stats.get('articles_found', 0)}")
                print(f"New articles: {stats.get('new_articles', 0)}")
                print(f"Existing articles: {stats.get('existing_articles', 0)}")
                print(f"Errors: {stats.get('errors', 0)}")
                print(f"Duration: {stats.get('duration', 0):.1f}s")
        else:
            print("No previous scraping data found.")
        return
    
    # Show startup info
    print(f"AICIS News & Notices Scraper")
    print(f"=" * 40)
    print(f"Target URL: {TARGET_URL}")
    print(f"Output: {DATA_DIR}/{OUTPUT_FILE}")
    print(f"Consecutive limit: {CONSECUTIVE_EXISTING_LIMIT}")
    print(f"Debug mode: {args.debug}")
    print(f"Starting scrape...")
    print()
    
    try:
        scraper = AICISNewsScraper(debug=args.debug)
        articles = scraper.scrape_all_articles()
        
        print(f"\n" + "=" * 50)
        print(f"Scraping completed!")
        print(f"Total articles stored: {len(articles)}")
        print(f"Articles found on page: {scraper.stats['articles_found']}")
        print(f"New articles scraped: {scraper.stats['new_articles']}")
        print(f"Existing articles skipped: {scraper.stats['existing_articles']}")
        print(f"Errors encountered: {scraper.stats['errors']}")
        print(f"Duration: {scraper.stats.get('duration', 0):.1f}s")
        print(f"Data saved to: {scraper.data_file}")
        print(f"Log saved to: {scraper.log_file}")
        
        # Recommendations based on results
        if scraper.stats['articles_found'] == 0:
            print("\n⚠️  No articles found on main page.")
            print("Suggestions:")
            print("- Run with --test-connection to check connectivity")
            print("- Run with --debug for detailed logging")
            print("- Check if the website structure has changed")
        elif scraper.stats['errors'] > scraper.stats['new_articles']:
            print("\n⚠️  High error rate detected.")
            print("- Check log file for details")
            print("- Consider running with lower request rates")
        
    except KeyboardInterrupt:
        print(f"\nScraping interrupted by user")
    except Exception as e:
        print(f"\nError during scraping: {e}")
        print("Run with --debug for more detailed error information")
        if args.debug:
            raise


if __name__ == "__main__":
    main()