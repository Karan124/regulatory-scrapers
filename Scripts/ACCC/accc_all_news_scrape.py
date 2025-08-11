import os
import json
import csv
import time
import logging
import sys
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from urllib.parse import urljoin, urlparse
import urllib3
from pathlib import Path

# Suppress urllib3 warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.accc.gov.au"
NEWS_CENTRE_URL = f"{BASE_URL}/news-centre"

# Use script directory for paths
SCRIPT_DIR = Path(__file__).parent
DATA_FOLDER = SCRIPT_DIR / "data"
OUTPUT_JSON = DATA_FOLDER / "accc_all_news.json"
OUTPUT_CSV = DATA_FOLDER / "accc_all_news.csv"
LOG_FILE = SCRIPT_DIR / "accc_scraper.log"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Request settings - reduced for efficiency
REQUEST_TIMEOUT = 20
RETRY_DELAY = 1
MAX_RETRIES = 2
RATE_LIMIT_DELAY = 0.5  # Reduced delay

# FIXED: More conservative early stopping configuration
MAX_PAGES_WITHOUT_NEW = 5  # Increased from 3 to 5
MAX_EXISTING_ARTICLES_BEFORE_STOP = 25  # Increased from 10 to 25

# Ensure data folder exists
DATA_FOLDER.mkdir(exist_ok=True)

class ACCCScraper:
    def __init__(self):
        self.session = None
        self.setup_logging()  # Setup logging FIRST
        self.setup_session()
        self.existing_data = self.load_existing_data()
        self.existing_urls = set(item['url'] for item in self.existing_data)
        self.new_items = []
        self.failed_urls = []
        self.total_scraped = 0
        self.total_new = 0
        self.pages_without_new = 0
        self.consecutive_existing = 0

    def setup_logging(self):
        """Setup logging with orchestrator compatibility"""
        self.logger = logging.getLogger("accc_scraper")
        self.logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        
        # File handler
        fh = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        
        # Console handler - only if not running from orchestrator
        if not os.environ.get('RUNNING_FROM_ORCHESTRATOR'):
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def setup_session(self):
        """Configure session with proper connection pooling and retries"""
        try:
            if self.session:
                self.session.close()
            
            self.session = requests.Session()
            
            # Configure retry strategy
            retry_strategy = Retry(
                total=MAX_RETRIES,
                backoff_factor=RETRY_DELAY,
                status_forcelist=[403, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524],
                allowed_methods=["HEAD", "GET", "OPTIONS"]
            )
            
            # Configure adapter with connection pooling
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=5,
                pool_maxsize=10,
                pool_block=False
            )
            
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
            
            # Set headers
            self.session.headers.update({
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "DNT": "1"
            })
            
            self.logger.info("Session configured successfully")
            
        except Exception as e:
            print(f"Error setting up session: {e}")
            raise

    def load_existing_data(self):
        """Load existing data from JSON file if it exists"""
        if OUTPUT_JSON.exists():
            try:
                with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"Loaded {len(data)} existing items from {OUTPUT_JSON}")
                    return data
            except Exception as e:
                print(f"Warning: Could not load existing data: {e}. Starting fresh.")
                return []
        return []

    def save_data(self, force_save=False):
        """Save all data (existing + new) to JSON and CSV files"""
        try:
            all_data = self.existing_data + self.new_items
            
            if not all_data and not force_save:
                self.logger.warning("No data to save")
                return
            
            # Sort by scraped_date or published_date (newest first)
            try:
                all_data.sort(key=lambda x: x.get('scraped_date', x.get('published_date', '')), reverse=True)
            except:
                pass  # Skip sorting if there are issues
            
            # Save to JSON
            with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, indent=2, ensure_ascii=False)
            
            # Save to CSV
            if all_data:
                df = pd.DataFrame(all_data)
                # Convert list columns to strings for CSV
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x))
                df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
            
            self.logger.info(f"Saved {len(all_data)} items total ({len(self.new_items)} new)")
            print(f"SUCCESS: Saved {len(self.new_items)} new articles to {OUTPUT_JSON}")
            
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            print(f"ERROR: Failed to save data: {e}")
            raise

    def get_page(self, url, max_retries=None):
        """Fetch a page with error handling"""
        if max_retries is None:
            max_retries = MAX_RETRIES
            
        for attempt in range(max_retries + 1):
            try:
                time.sleep(RATE_LIMIT_DELAY)
                
                response = self.session.get(
                    url, 
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                    verify=True
                )
                
                response.raise_for_status()
                
                if not response.text.strip():
                    continue
                
                return response.text
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout fetching {url} (attempt {attempt + 1})")
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"Connection error fetching {url} (attempt {attempt + 1})")
                self.setup_session()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.logger.warning(f"Page not found: {url}")
                    return None
                elif e.response.status_code in [403, 429]:
                    self.logger.warning(f"Rate limited: {url}")
                    time.sleep(RETRY_DELAY * (attempt + 2))
                else:
                    self.logger.warning(f"HTTP error {e.response.status_code}: {url}")
            except Exception as e:
                self.logger.error(f"Unexpected error fetching {url}: {e}")
            
            if attempt < max_retries:
                time.sleep(RETRY_DELAY * (attempt + 1))
        
        self.logger.error(f"Failed to fetch {url} after {max_retries + 1} attempts")
        self.failed_urls.append(url)
        return None

    def parse_news_listing(self, html, page_url):
        """FIXED: Enhanced parsing with better selectors and more comprehensive URL matching"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            articles = []
            
            # FIXED: Expanded selectors to catch more content types
            selectors = [
                'div[data-type="accc-news"]',
                'div[data_type="accc-news"]',
                '.accc-date-card',
                '.news-item',
                '.article-card',
                '.field--name-field-acccgov-body a',  # Content area links
                'article a',  # Article elements
                '.view-content .item',  # Views listings
                '.field--item a'  # Field items
            ]
            
            cards = []
            for selector in selectors:
                found_cards = soup.select(selector)
                if found_cards:
                    cards.extend(found_cards)
                    self.logger.debug(f"Found {len(found_cards)} items with selector: {selector}")
            
            # FIXED: Also search for direct links in the content
            all_links = soup.find_all('a', href=True)
            
            if not cards and not all_links:
                self.logger.warning(f"No news cards or links found on page: {page_url}")
                return []
            
            # Process cards first
            for card in cards:
                # FIXED: Enhanced link selectors
                link_selectors = [
                    'a.accc-date-card__link',
                    'a[href*="/news/"]',
                    'a[href*="/media-release/"]',
                    'a[href*="/speech/"]',
                    'a[href*="/update/"]',
                    'a[href*="/media-updates/"]',  # ADDED: This pattern
                    'a[href*="/about-us/news/"]',  # ADDED: This pattern
                    'h2 a',
                    'h3 a',
                    '.title a',
                    'a'
                ]
                
                link = None
                for link_selector in link_selectors:
                    link = card.select_one(link_selector) if hasattr(card, 'select_one') else card if card.name == 'a' else None
                    if link and 'href' in link.attrs:
                        break
                
                if link and 'href' in link.attrs:
                    href = link['href']
                    article_url = urljoin(BASE_URL, href)
                    
                    if self.is_valid_accc_news_url(article_url):
                        articles.append(article_url)
            
            # FIXED: Process all links on the page for comprehensive coverage
            for link in all_links:
                href = link.get('href', '')
                if href:
                    article_url = urljoin(BASE_URL, href)
                    if self.is_valid_accc_news_url(article_url) and article_url not in articles:
                        articles.append(article_url)
            
            # Remove duplicates while preserving order
            unique_articles = []
            seen = set()
            for url in articles:
                if url not in seen:
                    unique_articles.append(url)
                    seen.add(url)
            
            self.logger.info(f"Found {len(unique_articles)} unique articles on page: {page_url}")
            return unique_articles
            
        except Exception as e:
            self.logger.error(f"Error parsing news listing from {page_url}: {e}")
            return []

    def is_valid_accc_news_url(self, url):
        """FIXED: More comprehensive URL validation for ACCC news content"""
        try:
            parsed = urlparse(url)
            
            # Must be ACCC domain
            if parsed.netloc not in ['www.accc.gov.au', 'accc.gov.au']:
                return False
            
            path = parsed.path.lower()
            
            # FIXED: Expanded patterns to include all news types
            news_patterns = [
                '/news/',
                '/media-release/',
                '/speech/',
                '/update/',
                '/media-updates/',  # ADDED
                '/about-us/news/',  # ADDED
                '/about-us/publications/',  # ADDED
                '/media/',  # ADDED
            ]
            
            # Check if URL matches any news pattern
            for pattern in news_patterns:
                if pattern in path:
                    return True
            
            # FIXED: Additional checks for content that might be news-related
            news_keywords = [
                'media-release',
                'news',
                'speech',
                'update',
                'announcement',
                'report',
                'determination',
                'authorisation',
                'investigation'
            ]
            
            for keyword in news_keywords:
                if keyword in path:
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error validating URL {url}: {e}")
            return False

    def scrape_with_early_stopping(self):
        """FIXED: More conservative early stopping that reduces risk of missing content"""
        self.logger.info("Starting optimized ACCC news scraper with conservative early stopping")
        
        try:
            page = 0
            
            while page < 50:  # Safety limit
                page_url = NEWS_CENTRE_URL if page == 0 else f"{NEWS_CENTRE_URL}?page={page}"
                self.logger.info(f"Processing page {page + 1}: {page_url}")
                
                html = self.get_page(page_url)
                if not html:
                    self.logger.error(f"Failed to fetch page {page + 1}")
                    break
                
                article_urls = self.parse_news_listing(html, page_url)
                if not article_urls:
                    self.logger.info(f"No articles found on page {page + 1}, stopping")
                    break
                
                new_articles_this_page = 0
                existing_articles_this_page = 0
                
                for url in article_urls:
                    if url in self.existing_urls:
                        existing_articles_this_page += 1
                        self.consecutive_existing += 1
                        self.logger.debug(f"Skipping existing article: {url}")
                        
                        # FIXED: More conservative early stopping
                        if self.consecutive_existing >= MAX_EXISTING_ARTICLES_BEFORE_STOP:
                            self.logger.info(f"Found {self.consecutive_existing} consecutive existing articles, stopping")
                            return
                        continue
                    
                    # Reset consecutive counter when we find a new article
                    self.consecutive_existing = 0
                    
                    self.logger.info(f"Scraping new article: {url}")
                    article_data = self.scrape_article_page(url)
                    if article_data:
                        self.new_items.append(article_data)
                        self.existing_urls.add(url)
                        self.total_new += 1
                        new_articles_this_page += 1
                    
                    self.total_scraped += 1
                
                # FIXED: More conservative page-level early stopping
                if new_articles_this_page == 0:
                    self.pages_without_new += 1
                    self.logger.info(f"No new articles on page {page + 1} ({self.pages_without_new} consecutive pages without new articles)")
                    
                    if self.pages_without_new >= MAX_PAGES_WITHOUT_NEW:
                        self.logger.info(f"Stopping after {self.pages_without_new} pages without new articles")
                        break
                else:
                    self.pages_without_new = 0  # Reset counter
                    self.logger.info(f"Found {new_articles_this_page} new articles on page {page + 1}")
                
                # Save progress every few pages
                if (page + 1) % 5 == 0:
                    self.save_data()
                
                page += 1
                
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
            self.save_data()
        except Exception as e:
            self.logger.error(f"Error in scrape_with_early_stopping: {e}")
            self.save_data()
            raise

    def scrape_article_page(self, url):
        """FIXED: Enhanced article scraping with better content extraction"""
        try:
            html = self.get_page(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find main article content
            article = soup.select_one('article.accc-full-view') or soup.select_one('article') or soup.find('body')
            if not article:
                return None
            
            # Extract basic data
            data = {
                'url': url,
                'scraped_date': datetime.now().isoformat(),
                'title': self.get_text_by_selectors(article, soup, ['h1', '.title', '.article-title', '.page-title']),
                'published_date': self.get_date(article, soup),
                'article_type': self.get_article_type(url, article, soup),
                'summary': self.get_text_by_selectors(article, soup, ['.field--name-field-summary', '.summary', '.excerpt', '.lead']),
                'content': self.get_content(article, soup),
                'topics': self.get_topics(article, soup),
                'related_links': []  # Simplified - not extracting links for speed
            }
            
            # Validate - must have title OR content
            if not data['title'] and not data['content']:
                self.logger.warning(f"No title or content found for {url}")
                return None
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error scraping article {url}: {e}")
            return None

    def get_article_type(self, url, article, soup):
        """FIXED: Better article type detection based on URL patterns"""
        # First try to find explicit type markers
        type_text = self.get_text_by_selectors(article, soup, [
            '.accc-date-card__ribbon .field--name-bundle-fieldnode', 
            '.article-type',
            '.content-type',
            '.news-type'
        ])
        
        if type_text:
            return type_text
        
        # FIXED: Infer from URL patterns
        url_lower = url.lower()
        if '/media-release/' in url_lower:
            return 'Media release'
        elif '/speech/' in url_lower:
            return 'Speech'
        elif '/media-updates/' in url_lower:
            return 'Media update'
        elif '/update/' in url_lower:
            return 'Update'
        elif '/news/' in url_lower:
            return 'News'
        elif '/determination/' in url_lower:
            return 'Determination'
        elif '/authorisation/' in url_lower:
            return 'Authorisation'
        
        return ""

    def get_text_by_selectors(self, article, soup, selectors):
        """Get text using multiple selectors"""
        for selector in selectors:
            element = article.select_one(selector) or soup.select_one(selector)
            if element:
                text = element.get_text().strip()
                if text and len(text) > 3:
                    return text
        return ""

    def get_date(self, article, soup):
        """Extract published date"""
        selectors = [
            '.field--name-field-accc-news-published-date time',
            'time[datetime]',
            '.published-date',
            '.date',
            '.field--name-field-date'
        ]
        
        for selector in selectors:
            element = article.select_one(selector) or soup.select_one(selector)
            if element:
                if element.get('datetime'):
                    return element['datetime']
                date_text = element.get_text().strip()
                if date_text:
                    return date_text
        return ""

    def get_content(self, article, soup):
        """FIXED: Enhanced content extraction"""
        selectors = [
            '.field--name-field-acccgov-body',
            '.article-body',
            '.content-body',
            '.field--name-body',
            '.main-content'
        ]
        
        for selector in selectors:
            content_div = article.select_one(selector) or soup.select_one(selector)
            if content_div:
                # Remove unwanted elements
                for unwanted in content_div.select('script, style, nav, aside, .field--name-field-related-links'):
                    unwanted.decompose()
                
                content = content_div.get_text(separator='\n').strip()
                if content and len(content) > 50:  # Minimum content length
                    return '\n'.join(line.strip() for line in content.split('\n') if line.strip())
        
        # Fallback: get all paragraphs
        paragraphs = article.select('p')
        if paragraphs:
            content = '\n\n'.join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
            if content and len(content) > 50:
                return content
        
        return ""

    def get_topics(self, article, soup):
        """Extract topics"""
        topics = []
        selectors = [
            '.field--name-field-acccgov-topic .terms-badge', 
            '.topics .badge',
            '.field--name-field-topic a',
            '.tags a'
        ]
        
        for selector in selectors:
            elements = article.select(selector) or soup.select(selector)
            for element in elements:
                topic = element.get_text().strip()
                if topic and topic not in topics:
                    topics.append(topic)
        
        return topics

    def print_summary(self):
        """Print summary"""
        print("="*50)
        print("ACCC SCRAPING SUMMARY")
        print("="*50)
        print(f"Total articles processed: {self.total_scraped}")
        print(f"New articles scraped: {self.total_new}")
        print(f"Existing articles in database: {len(self.existing_data)}")
        print(f"Failed URLs: {len(self.failed_urls)}")
        print(f"Total articles now in database: {len(self.existing_data) + len(self.new_items)}")
        
        if self.failed_urls:
            print("\nFailed URLs:")
            for url in self.failed_urls[:10]:  # Show first 10
                print(f"  - {url}")
            if len(self.failed_urls) > 10:
                print(f"  ... and {len(self.failed_urls) - 10} more")
        
        if self.total_new == 0:
            print("INFO: No new articles found - database is up to date")
        
        self.logger.info(f"Scraping completed: {self.total_new} new articles found")

    def run(self):
        """Run the optimized scraper"""
        start_time = time.time()
        
        try:
            print(f"Starting ACCC scraper - {len(self.existing_data)} existing articles in database")
            self.scrape_with_early_stopping()
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
            print(f"ERROR: Scraping failed: {e}")
            sys.exit(1)
        finally:
            try:
                self.save_data(force_save=True)
                self.print_summary()
                
                elapsed_time = time.time() - start_time
                print(f"Completed in {elapsed_time:.1f} seconds")
                
                if self.session:
                    self.session.close()
                    
            except Exception as e:
                print(f"ERROR in cleanup: {e}")
                sys.exit(1)

if __name__ == "__main__":
    # Set environment variable if running from orchestrator
    if len(sys.argv) > 1 and sys.argv[1] == "--orchestrator":
        os.environ['RUNNING_FROM_ORCHESTRATOR'] = 'true'
    
    try:
        scraper = ACCCScraper()
        scraper.run()
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)