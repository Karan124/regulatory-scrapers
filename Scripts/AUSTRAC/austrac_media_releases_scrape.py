#!/usr/bin/env python3
"""
Fixed AUSTRAC Media Releases Scraper
Addresses issues in the original script and updates for current website structure
FIXED CHROME DRIVER SETUP
"""

import os
import json
import time
import hashlib
import logging
import re
import random
from datetime import datetime, timezone
from typing import List, Dict, Optional
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from bs4 import BeautifulSoup
import pandas as pd
import urllib3
import signal
import sys

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------
# Enhanced Logging Setup
# ----------------------------
os.makedirs('data', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(funcName)s:%(lineno)d]: %(message)s',
    handlers=[
        logging.FileHandler('data/austrac_media_scraper.log', mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ----------------------------
# Configuration
# ----------------------------
DATA_DIR = Path("data")
JSON_PATH = DATA_DIR / "austrac_media.json"
CSV_PATH = DATA_DIR / "austrac_media.csv"
CHECKPOINT_PATH = DATA_DIR / "media_scraper_checkpoint.json"

BASE_URL = "https://www.austrac.gov.au"
TARGET_URL = f"{BASE_URL}/news-and-media/media-release"
PAGE_LOAD_TIMEOUT = 30
ARTICLE_TIMEOUT = 20
DELAY_BETWEEN_REQUESTS = 3
MAX_RETRIES = 3
MAX_PAGES = 3  # Reasonable limit

# Global variables for graceful shutdown
shutdown_requested = False

# ----------------------------
# Signal Handler for Graceful Shutdown
# ----------------------------
def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class AUSTRACMediaScraperFixed:
    """
    Fixed AUSTRAC Media Releases scraper with improved error handling and current website structure
    FIXED CHROME DRIVER SETUP
    """
    
    def __init__(self):
        self.base_url = BASE_URL
        self.target_url = TARGET_URL
        self.data_folder = DATA_DIR
        self.data_folder.mkdir(exist_ok=True)
        
        # File paths
        self.json_file = JSON_PATH
        self.csv_file = CSV_PATH
        self.checkpoint_file = CHECKPOINT_PATH
        
        self.driver = None
        self.existing_hashes = set()
        
        # Load existing data for deduplication
        self._load_existing_data()
    
    def _load_existing_data(self):
        """Load existing media releases to prevent duplicates"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    self.existing_hashes = {item.get('hash_id', '') for item in existing_data}
                    logger.info(f"Loaded {len(self.existing_hashes)} existing media release records")
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                self.existing_hashes = set()
        else:
            logger.info("No existing data file found, starting fresh")
    
    def _generate_hash(self, url: str, headline: str, published_date: str) -> str:
        """Generate unique hash for media release"""
        content = f"{url}_{headline}_{published_date}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _setup_driver(self):
        """Simplified Chrome WebDriver setup - let system handle Chrome detection."""
        chrome_options = Options()
        
        # Essential stability options for Linux
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Updated user agent to match current Chrome version
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
        
        # Stealth options
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Performance optimizations
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")  # Speed up loading
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")
        
        # Memory management
        chrome_options.add_argument("--max_old_space_size=4096")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        try:
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
                    logger.info(f"Found ChromeDriver at: {path}")
                    break
            
            # Initialize driver with simplified service configuration
            service_kwargs = {}
            if chromedriver_path:
                service_kwargs['executable_path'] = chromedriver_path
            
            service = Service(**service_kwargs)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Chrome driver initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            return False
    
    def _wait_for_page_load(self, timeout=PAGE_LOAD_TIMEOUT):
        """Wait for page to fully load"""
        try:
            # Wait for basic page structure
            wait = WebDriverWait(self.driver, timeout)
            wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
            
            # Additional wait for dynamic content
            time.sleep(2)
            return True
            
        except TimeoutException:
            logger.warning(f"Page load timeout after {timeout} seconds")
            return False
    
    def _is_driver_alive(self):
        """Check if driver is still responsive"""
        try:
            if self.driver is None:
                return False
            # Try to get current URL as a simple health check
            _ = self.driver.current_url
            return True
        except Exception:
            return False
    
    def _restart_driver(self):
        """Restart the driver if it becomes unresponsive"""
        logger.info("Restarting Chrome driver...")
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"Error closing old driver: {e}")
        
        self._setup_driver()
        logger.info("Chrome driver restarted successfully")
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats from AUSTRAC website"""
        try:
            # Clean the date string
            date_str = date_str.strip()
            
            # Try multiple date formats
            date_formats = [
                "%d %B %Y",      # 24 June 2025
                "%d %b %Y",      # 24 Jun 2025  
                "%B %d, %Y",     # June 24, 2025
                "%b %d, %Y",     # Jun 24, 2025
                "%Y-%m-%d",      # 2025-06-24
                "%d/%m/%Y",      # 24/06/2025
                "%m/%d/%Y"       # 06/24/2025
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing date '{date_str}': {e}")
            return None
    
    def _extract_links_from_content(self, content: str) -> List[str]:
        """Extract all URLs from content text"""
        try:
            # Pattern to match URLs
            url_pattern = r'https?://[^\s<>"\'`|(){}[\]]*[^\s<>"\'`|(){}[\].,;:!?]'
            urls = re.findall(url_pattern, content)
            return list(set(urls))  # Remove duplicates
        except Exception as e:
            logger.warning(f"Error extracting links: {e}")
            return []
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters but keep essential punctuation
        text = re.sub(r'[^\w\s\.\,\;\:\!\?\-\(\)\[\]\{\}\"\'\/\\\@\#\$\%\&\*\+\=\<\>\~\`]', '', text)
        # Clean up spacing around punctuation
        text = re.sub(r'\s+([.!?,:;])', r'\1', text)
        text = re.sub(r'([.!?])\s*([A-Z])', r'\1 \2', text)
        
        return text.strip()
    
    def _extract_category(self, headline: str) -> str:
        """Extract category from media release headline"""
        headline_lower = headline.lower()
        
        # Common categories based on AUSTRAC's typical classifications
        categories = {
            'enforcement': ['penalty', 'infringement', 'compliance', 'enforcement', 'breach', 'violation', 'fine'],
            'regulation': ['regulation', 'requirement', 'obligation', 'rule', 'standard', 'consultation'],
            'guidance': ['guidance', 'update', 'information', 'clarification', 'advice'],
            'partnership': ['partnership', 'alliance', 'cooperation', 'joint', 'collaboration'],
            'technology': ['crypto', 'digital', 'technology', 'fintech', 'blockchain', 'bitcoin'],
            'industry': ['bank', 'casino', 'remitter', 'exchange', 'financial', 'gaming'],
            'reform': ['reform', 'amendment', 'change', 'new law', 'legislation'],
            'intelligence': ['intelligence', 'report', 'analysis', 'data', 'suspicious'],
            'international': ['international', 'global', 'fatf', 'overseas', 'foreign'],
            'education': ['forum', 'education', 'training', 'workshop', 'seminar']
        }
        
        for category, keywords in categories.items():
            if any(keyword in headline_lower for keyword in keywords):
                return category.title()
        
        return 'General'
    
    def _wait_for_articles(self, timeout=15):
        """Wait for articles to load on page with multiple selector strategies"""
        try:
            wait = WebDriverWait(self.driver, timeout)
            
            # Try multiple selectors that might indicate loaded articles
            selectors_to_try = [
                ".views-row",
                ".media-release",
                ".news-item",
                ".content-item",
                "article"
            ]
            
            for selector in selectors_to_try:
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    logger.debug(f"Articles loaded with selector: {selector}")
                    return True
                except TimeoutException:
                    continue
            
            logger.warning("No articles found with any selector")
            return False
            
        except Exception as e:
            logger.error(f"Error waiting for articles: {e}")
            return False
    
    def _extract_articles_from_page(self) -> List[Dict]:
        """Extract media releases from the current page using multiple selector strategies"""
        articles = []
        
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Strategy 1: Look for the specific selectors from original script
            article_elements = soup.select('.views-row')
            
            if not article_elements:
                # Strategy 2: Look for common media release patterns
                article_elements = soup.select('article, .media-release, .news-item, .content-item')
            
            if not article_elements:
                # Strategy 3: Look for any elements with links and dates
                article_elements = soup.select('div:has(a):has(time), li:has(a):has(time)')
            
            if not article_elements:
                # Strategy 4: Look for structured content in lists
                article_elements = soup.select('.view-content > div, .content-list > div, .media-list > div')
            
            logger.info(f"Found {len(article_elements)} potential media release elements")
            
            for element in article_elements:
                try:
                    article_data = self._parse_media_release_element(element)
                    if article_data:
                        articles.append(article_data)
                        logger.info(f"📄 Found media release: {article_data.get('headline', 'Unknown')[:100]}...")
                        
                except Exception as e:
                    logger.warning(f"Error parsing media release element: {e}")
                    continue
            
            # If still no articles, try to extract from text patterns
            if not articles:
                articles = self._extract_from_text_patterns(soup)
            
            return articles
            
        except Exception as e:
            logger.error(f"Error extracting media releases from page: {e}")
            return []
    
    def _parse_media_release_element(self, element) -> Optional[Dict]:
        """Parse individual media release element"""
        try:
            # Extract headline and URL
            headline = None
            url = None
            
            # Look for headline in various ways
            headline_selectors = [
                '.views-field-title a',
                '.field-title a',
                'h1 a, h2 a, h3 a, h4 a, h5 a, h6 a',
                '.title a',
                '.headline a',
                'a'
            ]
            
            for selector in headline_selectors:
                headline_elem = element.select_one(selector)
                if headline_elem:
                    headline = headline_elem.get_text(strip=True)
                    url = headline_elem.get('href')
                    break
            
            if not headline:
                # Try without anchor tag
                for selector in ['.views-field-title', '.field-title', 'h1, h2, h3, h4, h5, h6', '.title', '.headline']:
                    headline_elem = element.select_one(selector)
                    if headline_elem:
                        headline = headline_elem.get_text(strip=True)
                        break
            
            if not headline:
                return None
            
            # Look for URL if not found yet
            if not url:
                link_elem = element.select_one('a[href]')
                if link_elem:
                    url = link_elem.get('href')
            
            if not url:
                return None
            
            # Make URL absolute
            if url.startswith('/'):
                url = self.base_url + url
            elif not url.startswith('http'):
                url = self.base_url + '/' + url
            
            # Extract published date
            published_date = "Unknown"
            date_selectors = [
                '.views-field-field-article-dateline',
                '.field-article-dateline',
                'time',
                '.date',
                '.published',
                '.dateline'
            ]
            
            for selector in date_selectors:
                date_elem = element.select_one(selector)
                if date_elem:
                    published_date = date_elem.get_text(strip=True)
                    # Also try datetime attribute
                    if not published_date and date_elem.get('datetime'):
                        published_date = date_elem.get('datetime')
                    break
            
            # Extract intro/summary text
            intro_text = ""
            intro_selectors = [
                '.views-field-body',
                '.field-body',
                '.summary',
                '.excerpt',
                '.intro',
                '.description'
            ]
            
            for selector in intro_selectors:
                intro_elem = element.select_one(selector)
                if intro_elem:
                    intro_text = intro_elem.get_text(strip=True)
                    break
            
            # Generate hash for deduplication
            hash_id = self._generate_hash(url, headline, published_date)
            
            # Check if already exists
            if hash_id in self.existing_hashes:
                logger.debug(f"🔄 Skipping existing media release: {headline[:50]}...")
                return None
            
            article_data = {
                'hash_id': hash_id,
                'headline': self._clean_text(headline),
                'url': url,
                'published_date': published_date,
                'scraped_date': datetime.now(timezone.utc).isoformat(),
                'intro_text': self._clean_text(intro_text),
                'content': '',
                'related_links': [],
                'category': self._extract_category(headline)
            }
            
            return article_data
            
        except Exception as e:
            logger.error(f"Error parsing media release element: {e}")
            return None
    
    def _extract_from_text_patterns(self, soup) -> List[Dict]:
        """Extract media releases from text patterns when structured elements fail"""
        articles = []
        
        try:
            # Look for date patterns followed by text
            text_content = soup.get_text()
            
            # Pattern: "DD Month YYYY Title..."
            date_pattern = r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})\s+(.+?)(?=\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}|$)'
            
            matches = re.findall(date_pattern, text_content, re.DOTALL | re.IGNORECASE)
            
            for date_str, content in matches:
                try:
                    # Extract headline (first line of content)
                    lines = content.strip().split('\n')
                    headline = lines[0].strip() if lines else content[:100].strip()
                    
                    if len(headline) < 10:  # Skip very short headlines
                        continue
                    
                    # Clean headline
                    headline = re.sub(r'\s+', ' ', headline)
                    headline = headline.split('Read more')[0].strip()  # Remove "Read more" suffix
                    
                    # Generate a basic URL (this might need adjustment based on actual URL structure)
                    url_slug = re.sub(r'[^\w\s-]', '', headline.lower())
                    url_slug = re.sub(r'\s+', '-', url_slug)[:50]
                    url = f"{self.base_url}/media-release/{url_slug}"
                    
                    # Generate hash for deduplication
                    hash_id = self._generate_hash(url, headline, date_str)
                    
                    # Check if already exists
                    if hash_id in self.existing_hashes:
                        continue
                    
                    article_data = {
                        'hash_id': hash_id,
                        'headline': self._clean_text(headline),
                        'url': url,
                        'published_date': date_str.strip(),
                        'scraped_date': datetime.now(timezone.utc).isoformat(),
                        'intro_text': self._clean_text(content[:200] + "..."),
                        'content': '',
                        'related_links': [],
                        'category': self._extract_category(headline)
                    }
                    
                    articles.append(article_data)
                    logger.info(f"📄 Extracted from text: {headline[:100]}...")
                    
                except Exception as e:
                    logger.warning(f"Error parsing text pattern: {e}")
                    continue
            
            return articles
            
        except Exception as e:
            logger.error(f"Error extracting from text patterns: {e}")
            return []
    
    def _extract_media_release_content(self, article: Dict) -> Dict:
        """Extract full content from individual media release page"""
        try:
            logger.info(f"🌐 Extracting content for: {article['headline'][:100]}...")
            
            # Check driver health
            if not self._is_driver_alive():
                self._restart_driver()
            
            # Navigate to article URL
            self.driver.get(article['url'])
            
            if not self._wait_for_page_load():
                logger.warning(f"Page failed to load: {article['url']}")
                return article
            
            time.sleep(random.uniform(1, 2))  # Random delay
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract headline (might be different from listing page)
            headline = article['headline']  # Default to existing
            headline_selectors = ['.au-header-heading', 'h1', '.page-title', '.article-title', '.headline']
            
            for selector in headline_selectors:
                headline_elem = soup.select_one(selector)
                if headline_elem:
                    headline = headline_elem.get_text(strip=True)
                    break
            
            # Extract published date (might be more detailed)
            published_date = article['published_date']  # Default to existing
            datetime_str = None
            
            date_selectors = ['time[datetime]', '.date', '.published-date', '.article-date']
            for selector in date_selectors:
                date_elem = soup.select_one(selector)
                if date_elem:
                    published_date = date_elem.get_text(strip=True)
                    datetime_str = date_elem.get('datetime')
                    break
            
            # Extract main content using multiple strategies
            content = ""
            content_selectors = [
                '.body-copy',
                '.content',
                '.article-content', 
                '.main-content',
                'article',
                '.page-content',
                '.entry-content'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Remove navigation and other non-content elements
                    for unwanted in content_elem.select('nav, .navigation, .breadcrumb, .share, .tags, .metadata'):
                        unwanted.decompose()
                    
                    content = content_elem.get_text(separator='\n', strip=True)
                    break
            
            if not content:
                # Fallback: get all text from body
                body = soup.select_one('body')
                if body:
                    content = body.get_text(separator='\n', strip=True)
            
            # Clean content
            content = self._clean_text(content)
            
            # Extract related links from content
            related_links = []
            if content:
                related_links = self._extract_links_from_content(content)
            
            # Also extract links from the article body HTML
            content_elem = soup.select_one('.body-copy, .content, .article-content')
            if content_elem:
                for link in content_elem.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('http'):
                        related_links.append(href)
                    elif href.startswith('/'):
                        related_links.append(self.base_url + href)
            
            # Remove duplicates
            related_links = list(set(related_links))
            
            # Update article with extracted content
            article['headline'] = headline
            article['published_date'] = published_date
            article['datetime'] = datetime_str
            article['content'] = content
            article['related_links'] = related_links
            
            if content:
                logger.info(f"✅ Content extracted: {len(content)} characters")
            else:
                logger.warning(f"⚠️ No content extracted for: {article['headline'][:50]}...")
            
            return article
            
        except Exception as e:
            logger.error(f"Error extracting content for {article['url']}: {e}")
            return article
    
    def _scrape_page(self, page_num: int = 0) -> List[Dict]:
        """Scrape media releases from a specific page"""
        try:
            if page_num > 0:
                url = f"{self.target_url}?page={page_num}"
            else:
                url = self.target_url
            
            logger.info(f"📄 Scraping page: {url}")
            
            # Check driver health
            if not self._is_driver_alive():
                self._restart_driver()
            
            self.driver.get(url)
            
            if not self._wait_for_page_load():
                logger.error(f"Failed to load page: {url}")
                return []
            
            # Wait for articles to load
            if not self._wait_for_articles():
                logger.warning(f"No articles found on page {page_num + 1}")
                return []
            
            # Random delay to appear more human
            time.sleep(random.uniform(2, 4))
            
            # Extract articles from current page
            articles = self._extract_articles_from_page()
            
            logger.info(f"Found {len(articles)} media releases on page {page_num + 1}")
            return articles
            
        except Exception as e:
            logger.error(f"Error scraping page {page_num + 1}: {e}")
            return []
    
    def _check_pagination(self) -> bool:
        """Check if there are more pages to scrape"""
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Look for pagination indicators
            pagination_selectors = [
                '.pager__item--next',
                '.pagination .next',
                'a[rel="next"]',
                '.next-page'
            ]
            
            for selector in pagination_selectors:
                next_link = soup.select_one(selector)
                if next_link and not next_link.get('disabled'):
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking pagination: {e}")
            return False
    
    def scrape_media_releases(self) -> List[Dict]:
        """Main scraping method"""
        logger.info("="*80)
        logger.info("🚀 Starting AUSTRAC Media Releases scraping")
        logger.info("="*80)
        
        if not self._setup_driver():
            return []
        
        try:
            all_articles = []
            page_num = 0
            consecutive_empty_pages = 0
            max_empty_pages = 3
            
            while consecutive_empty_pages < max_empty_pages and page_num < MAX_PAGES and not shutdown_requested:
                logger.info(f"Processing page {page_num + 1}")
                
                page_articles = self._scrape_page(page_num)
                
                if not page_articles:
                    consecutive_empty_pages += 1
                    logger.info(f"Empty page encountered ({consecutive_empty_pages}/{max_empty_pages})")
                else:
                    consecutive_empty_pages = 0
                    all_articles.extend(page_articles)
                
                # Check if there are more pages
                if not self._check_pagination():
                    logger.info("No more pages to scrape")
                    break
                
                page_num += 1
                
                # Add delay between pages
                time.sleep(random.uniform(2, 4))
            
            if not all_articles:
                logger.info("No new media releases found")
                return []
            
            logger.info(f"Found {len(all_articles)} new media releases")
            logger.info("="*80)
            logger.info("📄 Processing content extraction...")
            logger.info("="*80)
            
            # Extract detailed content for each media release
            enriched_articles = []
            for i, article in enumerate(all_articles):
                if shutdown_requested:
                    break
                
                logger.info(f"Processing media release {i+1}/{len(all_articles)}")
                enriched_article = self._extract_media_release_content(article)
                enriched_articles.append(enriched_article)
                
                # Add delay between requests
                time.sleep(random.uniform(2, 4))
            
            logger.info("="*80)
            logger.info(f"✅ Successfully processed {len(enriched_articles)} media releases")
            logger.info("="*80)
            
            return enriched_articles
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔒 Browser closed")
    
    def _load_existing_articles(self) -> List[Dict]:
        """Load existing articles from JSON file"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading existing articles: {e}")
        return []
    
    def save_articles(self, new_articles: List[Dict]):
        """Save media releases to JSON and CSV files"""
        if not new_articles:
            logger.info("No new media releases to save")
            return
        
        # Load existing data
        existing_articles = self._load_existing_articles()
        
        # Create a set of existing hash_ids for faster lookup
        existing_hash_ids = {article.get('hash_id', '') for article in existing_articles}
        
        # Filter out truly new articles
        actually_new_articles = []
        for article in new_articles:
            if article.get('hash_id', '') not in existing_hash_ids:
                actually_new_articles.append(article)
            else:
                logger.debug(f"Skipping duplicate: {article.get('headline', 'Unknown')}")
        
        if not actually_new_articles:
            logger.info("No genuinely new media releases found - all were duplicates")
            return
        
        # Merge new articles with existing
        all_articles = existing_articles + actually_new_articles
        
        # Sort by scraped_date (most recent first)
        try:
            all_articles.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
        except Exception as e:
            logger.warning(f"Could not sort by date: {e}")
        
        # Save to JSON
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved {len(all_articles)} total media releases to {self.json_file}")
            logger.info(f"➕ Added {len(actually_new_articles)} new media releases")
        except Exception as e:
            logger.error(f"Error saving JSON file: {e}")
        
        # Save to CSV
        if all_articles:
            try:
                fieldnames = [
                    'hash_id', 'headline', 'url', 'published_date', 
                    'scraped_date', 'intro_text', 'content', 'related_links', 'category'
                ]
                
                df = pd.DataFrame(all_articles)
                
                # Ensure all required columns exist
                for col in fieldnames:
                    if col not in df.columns:
                        df[col] = ""
                
                # Convert lists to strings for CSV
                if 'related_links' in df.columns:
                    df['related_links'] = df['related_links'].apply(lambda x: '; '.join(x) if isinstance(x, list) else str(x))
                
                df = df[fieldnames]
                df.to_csv(self.csv_file, index=False, encoding='utf-8')
                logger.info(f"💾 Saved {len(all_articles)} total media releases to {self.csv_file}")
            except Exception as e:
                logger.error(f"Error saving CSV file: {e}")
        
        # Log summary of new additions
        if actually_new_articles:
            logger.info("="*80)
            logger.info("📋 NEW MEDIA RELEASES ADDED:")
            for article in actually_new_articles:
                logger.info(f"   • {article.get('category', 'Unknown')}: {article.get('headline', 'No title')[:80]}...")
            logger.info("="*80)

def main():
    """Main execution function"""
    try:
        scraper = AUSTRACMediaScraperFixed()
        
        logger.info("="*80)
        logger.info("🚀 AUSTRAC MEDIA RELEASES SCRAPER STARTED")
        logger.info("="*80)
        
        new_articles = scraper.scrape_media_releases()
        
        if new_articles:
            logger.info(f"✅ Successfully scraped {len(new_articles)} media releases")
            scraper.save_articles(new_articles)
            
            # Summary statistics
            categories = {}
            content_count = 0
            total_content_chars = 0
            
            for article in new_articles:
                category = article.get('category', 'Unknown')
                categories[category] = categories.get(category, 0) + 1
                
                content = article.get('content', '')
                if content.strip():
                    content_count += 1
                    total_content_chars += len(content)
            
            logger.info("="*80)
            logger.info("📊 SCRAPING SUMMARY:")
            logger.info(f"   Total media releases found: {len(new_articles)}")
            logger.info(f"   Media releases with content: {content_count}")
            if content_count > 0:
                logger.info(f"   Average content length: {total_content_chars // content_count:,} characters")
            logger.info("   Categories:")
            for category, count in sorted(categories.items()):
                logger.info(f"     - {category}: {count}")
            logger.info("="*80)
            
            # Print final summary
            print("="*60)
            print("AUSTRAC MEDIA RELEASES SCRAPING SUMMARY")
            print("="*60)
            print(f"Total media releases processed: {len(new_articles)}")
            print(f"Media releases with content: {content_count}")
            if content_count > 0:
                print(f"Average content length: {total_content_chars // content_count:,} characters")
            print("Categories breakdown:")
            for category, count in sorted(categories.items()):
                print(f"  - {category}: {count}")
            print("="*60)
            
        else:
            logger.info("ℹ️  No new media releases found (all may be existing)")
            print("INFO: No new media releases found - database is up to date")
            
    except KeyboardInterrupt:
        logger.info("🛑 Scraping interrupted by user")
    except Exception as e:
        logger.error(f"💥 Scraping failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()