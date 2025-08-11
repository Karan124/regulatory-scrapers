import os
import json
import csv
import time
import logging
import sys
import re
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from urllib.parse import urljoin, urlparse, parse_qs
import urllib3
import random
import cloudscraper
from fake_useragent import UserAgent
import io

try:
    import pdfplumber
except ImportError:
    print("WARNING: pdfplumber not found. PDF extraction will be disabled.")
    print("Please install it using: pip install pdfplumber")
    pdfplumber = None

# Suppress urllib3 warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.acma.gov.au"

# Multiple content sources
CONTENT_SOURCES = {
    'media-releases': f"{BASE_URL}/media-releases",
   # 'news-articles': f"{BASE_URL}/news-articles", 
    'articles': f"{BASE_URL}/articles",
    'statements': f"{BASE_URL}/statements",
    'speeches': f"{BASE_URL}/speeches"
}

# Use script directory for paths
SCRIPT_DIR = Path(__file__).parent
DATA_FOLDER = SCRIPT_DIR / "data"
OUTPUT_JSON = DATA_FOLDER / "acma_all_content.json"
OUTPUT_CSV = DATA_FOLDER / "acma_all_content.csv"
LOG_FILE = SCRIPT_DIR / "acma_scraper.log"

# User-Agents
try:
    ua = UserAgent()
    USER_AGENTS = [ua.random for _ in range(10)]
except:
    USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"]

# Request settings
REQUEST_TIMEOUT = 45
MAX_RETRIES = 5
RATE_LIMIT_DELAY = (3, 7)

# Configuration
MAX_PAGES_PER_YEAR = 7
MAX_CONSECUTIVE_EXISTING = 20
DAILY_MODE_ARTICLE_LIMIT = 300

# Ensure data folder exists
DATA_FOLDER.mkdir(exist_ok=True)

class ACMAScraperEnhanced:
    def __init__(self, daily_mode=True):
        self.daily_mode = daily_mode
        self.scraper = None
        self.session = None
        self.setup_logging()
        self.setup_enhanced_session()
        self.existing_data = self.load_existing_data()
        self.existing_urls = set(item['url'] for item in self.existing_data)
        self.new_items = []
        self.failed_urls = []
        self.total_new = 0
        self.consecutive_existing = 0
        self.articles_processed_today = 0

    def setup_logging(self):
        """Setup logging"""
        self.logger = logging.getLogger("acma_scraper")
        self.logger.setLevel(logging.INFO)
        if self.logger.hasHandlers(): 
            self.logger.handlers.clear()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def setup_enhanced_session(self):
        """Setup enhanced session with cloudscraper"""
        try:
            self.scraper = cloudscraper.create_scraper()
            self.session = requests.Session()
            retry_strategy = Retry(
                total=MAX_RETRIES, 
                backoff_factor=1, 
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.rotate_session_identity()
            self.logger.info("Enhanced session configured")
        except Exception as e:
            self.logger.error(f"Error setting up session: {e}")
            raise

    def rotate_session_identity(self):
        """Rotate user agent"""
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        if self.session: 
            self.session.headers.update(headers)
        if self.scraper: 
            self.scraper.headers.update(headers)

    def get_page_enhanced(self, url):
        """Get page with rate limiting and error handling"""
        time.sleep(random.uniform(*RATE_LIMIT_DELAY))
        try:
            response = self.scraper.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            self.failed_urls.append(url)
            return None

    def load_existing_data(self):
        """Load existing data from JSON file"""
        if OUTPUT_JSON.exists():
            try:
                with open(OUTPUT_JSON, 'r', encoding='utf-8') as f: 
                    data = json.load(f)
                    self.logger.info(f"Loaded {len(data)} existing items")
                    return data
            except json.JSONDecodeError: 
                self.logger.warning("JSON decode error, starting fresh")
                return []
        return []

    def save_data(self):
        """Save data to JSON and CSV files"""
        if not self.new_items: 
            return
        
        all_data = self.existing_data + self.new_items
        seen_urls = set()
        unique_data = [d for d in all_data if d['url'] not in seen_urls and not seen_urls.add(d['url'])]
        unique_data.sort(key=lambda x: x.get('published_date', ''), reverse=True)
        
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f: 
            json.dump(unique_data, f, indent=2, ensure_ascii=False)
        
        if unique_data:
            df = pd.DataFrame(unique_data)
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x))
            df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        
        self.logger.info(f"Saved {len(self.new_items)} new items. Total: {len(unique_data)}")
        self.existing_data = unique_data
        self.new_items = []

    def get_recent_years_only(self):
        """Get years to scrape - dynamically adjusts for year rollovers"""
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # If it's early in the year (Jan-Mar), also check previous year more thoroughly
        if current_month <= 3:
            years_to_check = [str(year) for year in range(current_year, current_year - 4, -1)]
            self.logger.info(f"Early year detected, checking extra year: {years_to_check}")
        else:
            years_to_check = [str(year) for year in range(current_year, current_year - 3, -1)]
        
        self.logger.info(f"Dynamic year range: {years_to_check}")
        return years_to_check

    def get_pagination_info(self, html):
        """Parse pagination to determine total pages"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            pager_selectors = ['nav.pager', '.pager', '.pagination', 'nav[aria-label*="pagination"]']
            pager = None
            
            for selector in pager_selectors:
                pager = soup.select_one(selector)
                if pager:
                    break
            
            if not pager: 
                self.logger.debug("No pagination found, assuming single page")
                return 1

            # Try to find the "last" page link
            last_selectors = [
                'li.pager__item--last a',
                '.pager-last a',
                'a[aria-label*="last"]',
                'a[title*="last"]'
            ]
            
            for selector in last_selectors:
                last_link = pager.select_one(selector)
                if last_link and last_link.get('href'):
                    href = last_link.get('href')
                    query_params = parse_qs(urlparse(href).query)
                    if 'page' in query_params:
                        try:
                            total_pages = int(query_params['page'][0]) + 1
                            self.logger.debug(f"Found total pages from last link: {total_pages}")
                            return total_pages
                        except (ValueError, IndexError):
                            continue
            
            # Fallback: find the highest page number shown
            page_numbers = [0]
            for link in pager.select('a[href*="page="]'):
                try:
                    query_params = parse_qs(urlparse(link.get('href')).query)
                    if 'page' in query_params: 
                        page_num = int(query_params['page'][0])
                        page_numbers.append(page_num)
                except (ValueError, IndexError): 
                    continue
            
            if page_numbers:
                total_pages = max(page_numbers) + 1
                self.logger.debug(f"Found total pages from max page number: {total_pages}")
                return total_pages
            
            return 1
            
        except Exception as e:
            self.logger.error(f"Could not determine pagination info: {e}")
            return 1

    def is_valid_acma_content_url(self, url):
        """Enhanced URL validation - EXCLUDES listing/navigation pages"""
        try:
            parsed = urlparse(url)
            
            # Must be ACMA domain
            if parsed.netloc not in ['www.acma.gov.au', 'acma.gov.au']:
                return False
            
            path = parsed.path.lower()
            
            # Exclude listing/navigation pages (exact matches)
            excluded_patterns = [
                r'/media-releases$',
                r'/media-releases/$',
                r'/news-articles$',
                r'/news-articles/$',
                r'/articles$',
                r'/articles/$',
                r'/statements$',
                r'/statements/$',
                r'/speeches$',
                r'/speeches/$',
                r'/news-speeches-and-publications$',
                r'/news-speeches-and-publications/$',
                r'/publications$',
                r'/publications/$',
                r'/events$',
                r'/events/$'
            ]
            
            # Check if this is a listing/navigation page to exclude
            for pattern in excluded_patterns:
                if re.search(pattern, path):
                    self.logger.debug(f"Excluding listing page: {url}")
                    return False
            
            # Enhanced content patterns - must contain actual content identifiers
            content_patterns = [
                r'/media-releases/.+',
                r'/media-release/.+',
                r'/news-articles/.+',
                r'/news-article/.+',
                r'/articles/.+',
                r'/article/.+',
                r'/statements/.+',
                r'/statement/.+',
                r'/speeches/.+',
                r'/speech/.+',
                r'/publications/.+',
                r'/publication/.+'
            ]
            
            # Check if URL matches any actual content pattern
            for pattern in content_patterns:
                if re.search(pattern, path):
                    return True
            
            # Additional checks for date-based URLs (common in articles)
            if re.search(r'/20\d{2}-\d{2}/.+', path):  # Must have content after date
                return True
            
            # Check for content that might be news-related but in different sections
            if any(keyword in path for keyword in ['news', 'media', 'release', 'announcement', 'update']):
                # Additional validation to avoid false positives
                if any(exclude in path for exclude in ['/about', '/contact', '/help', '/privacy']):
                    return False
                # Must not be a listing page
                if not re.search(r'/(news|media|release|announcement|update)$', path):
                    return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"Error validating URL {url}: {e}")
            return False

    def is_listing_or_navigation_page(self, soup, url):
        """Detect if this is a listing or navigation page that should be skipped"""
        try:
            # Check for pagination elements (indicates listing page)
            if soup.select_one('nav.pager, .pager, .pagination'):
                return True
            
            # Check for filter elements (indicates listing page)
            if soup.select_one('.filters, .filter-form, select[name="year"]'):
                return True
            
            # Check for multiple article previews/teasers (indicates listing page)
            article_previews = soup.select('.node-teaser, .article-teaser, .news-teaser, .media-teaser')
            if len(article_previews) > 3:  # Multiple previews indicate listing page
                return True
            
            # Check URL patterns that indicate listing pages
            url_lower = url.lower()
            listing_patterns = [
                r'/media-releases$',
                r'/news-articles$',
                r'/articles$',
                r'/statements$',
                r'/speeches$',
                r'/publications$',
                r'/news-speeches-and-publications$'
            ]
            
            for pattern in listing_patterns:
                if re.search(pattern, url_lower):
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error detecting listing page for {url}: {e}")
            return False

    def is_navigation_content(self, content):
        """Detect if content is primarily navigation/menu content"""
        if not content or len(content.strip()) < 50:
            return True
        
        content_lower = content.lower()
        
        # Check for navigation indicators
        navigation_indicators = [
            'what can we help you with?',
            'view our media releases',
            'articles, announcements and news',
            'sign up for media releases',
            'contact us with media enquiries',
            'filter by year',
            'category',
            '- any -',
            'stay up to date with our latest news'
        ]
        
        # If content contains multiple navigation indicators, it's likely a navigation page
        indicator_count = sum(1 for indicator in navigation_indicators if indicator in content_lower)
        if indicator_count >= 3:
            return True
        
        # Check for repeated navigation elements
        if content_lower.count('learn more') > 3:
            return True
        
        # Check content length vs navigation content ratio
        total_length = len(content)
        navigation_content_length = sum(len(indicator) for indicator in navigation_indicators if indicator in content_lower)
        
        if navigation_content_length > total_length * 0.3:  # >30% navigation content
            return True
        
        return False

    def discover_all_acma_content_urls(self, source_name, base_url, year):
        """Comprehensive URL discovery for ACMA content"""
        self.logger.info(f"=== Comprehensive URL discovery for {source_name} year {year} ===")
        
        discovered_urls = set()
        
        # Step 1: Scrape the listing pages
        first_page_url = f"{base_url}?year={year}"
        html = self.get_page_enhanced(first_page_url)
        if not html:
            self.logger.warning(f"Failed to get first page for {source_name} year {year}")
            return []

        total_pages = self.get_pagination_info(html)
        pages_to_process = min(total_pages, MAX_PAGES_PER_YEAR)
        
        # Process all pages
        for page in range(pages_to_process):
            page_url = f"{base_url}?year={year}&page={page}" if page > 0 else first_page_url
            
            if page > 0:
                html = self.get_page_enhanced(page_url)
                if not html:
                    continue
            
            # Extract ALL possible ACMA content URLs from the page
            page_urls = self.extract_all_acma_urls_from_page(html, page_url)
            discovered_urls.update(page_urls)
        
        # Step 2: Additional discovery for media-releases
        if source_name == 'media-releases':
            additional_urls = self.discover_additional_content_urls(year)
            discovered_urls.update(additional_urls)
        
        self.logger.info(f"Discovered {len(discovered_urls)} unique URLs from {source_name} for {year}")
        return list(discovered_urls)

    def extract_all_acma_urls_from_page(self, html, page_url):
        """Extract ALL ACMA content URLs from a page using multiple strategies"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found_urls = set()
            
            # Strategy 1: Find all links and filter for ACMA content
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href:
                    full_url = urljoin(BASE_URL, href)
                    if self.is_valid_acma_content_url(full_url):
                        found_urls.add(full_url)
                        self.logger.debug(f"Found content URL: {full_url}")
            
            # Strategy 2: Look for specific patterns in the HTML
            for element in soup.find_all(['div', 'article', 'section']):
                # Check data attributes
                for attr_name, attr_value in element.attrs.items():
                    if isinstance(attr_value, str) and ('articles/' in attr_value or 'media-release' in attr_value):
                        potential_url = urljoin(BASE_URL, attr_value)
                        if self.is_valid_acma_content_url(potential_url):
                            found_urls.add(potential_url)
                            self.logger.debug(f"Found URL in attribute {attr_name}: {potential_url}")
            
            # Strategy 3: Look for URLs in JavaScript
            for script in soup.find_all('script'):
                script_content = script.string or ""
                # Look for URL patterns in JavaScript
                url_matches = re.findall(r'["\']([^"\']*(?:articles|media-release)[^"\']*)["\']', script_content)
                for match in url_matches:
                    if match.startswith('/'):
                        potential_url = urljoin(BASE_URL, match)
                        if self.is_valid_acma_content_url(potential_url):
                            found_urls.add(potential_url)
                            self.logger.debug(f"Found URL in script: {potential_url}")
            
            self.logger.info(f"Extracted {len(found_urls)} URLs from page {page_url}")
            return found_urls
            
        except Exception as e:
            self.logger.error(f"Error extracting URLs from {page_url}: {e}")
            return set()

    def discover_additional_content_urls(self, year):
        """Additional discovery method to catch content that might be missed"""
        additional_urls = set()
        
        try:
            # Check the main ACMA homepage and news sections for recent content
            additional_pages_to_check = [
                f"{BASE_URL}/",
                f"{BASE_URL}/news-speeches-and-publications",
                f"{BASE_URL}/articles",
                f"{BASE_URL}/news-articles"
            ]
            
            for page_url in additional_pages_to_check:
                html = self.get_page_enhanced(page_url)
                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Look for any links to current year content
                    all_links = soup.find_all('a', href=True)
                    for link in all_links:
                        href = link.get('href')
                        if href and year in href:
                            full_url = urljoin(BASE_URL, href)
                            if self.is_valid_acma_content_url(full_url):
                                additional_urls.add(full_url)
                                self.logger.debug(f"Found additional URL: {full_url}")
                
        except Exception as e:
            self.logger.error(f"Error in additional content discovery: {e}")
        
        return additional_urls

    def scrape_content_source(self, source_name, base_url):
        """Enhanced content source scraping"""
        self.logger.info(f"=== Scraping {source_name} ===")
        
        total_new = 0
        for year in self.get_recent_years_only():
            if self.articles_processed_today >= DAILY_MODE_ARTICLE_LIMIT:
                break
            
            # Use comprehensive URL discovery
            discovered_urls = self.discover_all_acma_content_urls(source_name, base_url, year)
            
            # Process discovered URLs
            for url in discovered_urls:
                if self.articles_processed_today >= DAILY_MODE_ARTICLE_LIMIT:
                    break
                    
                self.articles_processed_today += 1
                
                if url in self.existing_urls:
                    self.consecutive_existing += 1
                    continue
                
                self.consecutive_existing = 0
                
                # Create basic article data for scraping
                article_data = {
                    'url': url,
                    'title': '',
                    'date_text': '',
                    'source_type': source_name
                }
                
                full_article_data = self.scrape_article_page(url, article_data, source_name)
                if full_article_data:
                    self.new_items.append(full_article_data)
                    self.existing_urls.add(url)
                    total_new += 1
                    self.logger.info(f"Successfully scraped: {url}")
                
                # Stop if too many consecutive existing articles
                if self.consecutive_existing >= MAX_CONSECUTIVE_EXISTING:
                    self.logger.info(f"Reached {self.consecutive_existing} consecutive existing articles")
                    break
            
            # Save after each year
            if self.new_items:
                self.save_data()
        
        self.logger.info(f"Found {total_new} new items from {source_name}")
        return total_new

    def scrape_article_page(self, url, article_data, source_type):
        """Enhanced article scraping with listing page detection"""
        try:
            html = self.get_page_enhanced(url)
            if not html: 
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Detect and skip listing/navigation pages
            if self.is_listing_or_navigation_page(soup, url):
                self.logger.debug(f"Skipping listing/navigation page: {url}")
                return None
            
            # Enhanced title extraction
            title_selectors = ['h1', '.page-title', '.article-title', '.title', 'h1.heading', '.hero-title']
            title = ""
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            
            if not title:
                title = article_data.get('title', '')
            
            # Skip if title indicates this is a listing page
            listing_title_indicators = [
                'media releases',
                'news articles', 
                'statements',
                'speeches',
                'publications',
                'news, speeches and publications'
            ]
            
            if any(indicator in title.lower() for indicator in listing_title_indicators):
                self.logger.debug(f"Skipping page with listing title: {title}")
                return None
            
            # Enhanced content extraction
            content = self.extract_content(soup)
            
            # Validate content quality - skip if it's navigation content
            if self.is_navigation_content(content):
                self.logger.debug(f"Skipping page with navigation content: {url}")
                return None
            
            # Extract PDF content
            pdf_content = ""
            content_area = soup.select_one('div.prose, article, main') or soup
            for link in content_area.select('a[href$=".pdf"]'):
                pdf_url = urljoin(BASE_URL, link['href'])
                extracted_text = self._extract_text_from_pdf(pdf_url)
                if extracted_text:
                    pdf_content += f"\n\n--- PDF Content from {os.path.basename(pdf_url)} ---\n\n{extracted_text}"
            
            # Enhanced date extraction
            published_date = self.extract_published_date(soup, article_data.get('date_text', ''), url)
            
            result = {
                'url': url, 
                'scraped_date': datetime.now().isoformat(),
                'title': title,
                'published_date': published_date,
                'content': content + pdf_content,
                'source_type': source_type,
                'content_type': self.determine_content_type(url, soup)
            }
            
            # Final validation: Must have meaningful content
            if not title or len(content.strip()) < 100:
                self.logger.warning(f"Insufficient content quality for {url}")
                return None
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error scraping article page {url}: {e}")
            return None

    def determine_content_type(self, url, soup):
        """Determine content type from URL and page structure"""
        url_lower = url.lower()
        
        if '/media-release' in url_lower:
            return 'Media Release'
        elif '/articles/' in url_lower:
            return 'Article'
        elif '/news-article' in url_lower:
            return 'News Article'
        elif '/statement' in url_lower:
            return 'Statement'
        elif '/speech' in url_lower:
            return 'Speech'
        else:
            # Try to determine from page content
            breadcrumb = soup.select_one('.breadcrumb, nav[aria-label="breadcrumb"]')
            if breadcrumb:
                breadcrumb_text = breadcrumb.get_text().lower()
                if 'media release' in breadcrumb_text:
                    return 'Media Release'
                elif 'article' in breadcrumb_text:
                    return 'Article'
                elif 'statement' in breadcrumb_text:
                    return 'Statement'
                elif 'speech' in breadcrumb_text:
                    return 'Speech'
            
            return 'Content'

    def extract_published_date(self, soup, fallback_date_text="", url=""):
        """Enhanced date extraction with URL-based fallback"""
        # Try multiple date selectors
        date_selectors = [
            'time[datetime]',
            '.published-date',
            '.date',
            '.news-date',
            '.field--name-field-date',
            '.field--name-created',
            'meta[property="article:published_time"]'
        ]
        
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'time' and element.get('datetime'):
                    return element['datetime']
                elif element.name == 'meta' and element.get('content'):
                    return element['content']
                else:
                    date_text = element.get_text(strip=True)
                    if date_text:
                        parsed_date = self.parse_date(date_text)
                        if parsed_date:
                            return parsed_date
        
        # Extract date from URL pattern (important for /articles/YYYY-MM/ URLs)
        url_date_match = re.search(r'/(\d{4}-\d{2})/', url)
        if url_date_match:
            try:
                year_month = url_date_match.group(1)
                return f"{year_month}-01"  # Default to first of month
            except:
                pass
        
        # Fallback to provided date text
        if fallback_date_text:
            return self.parse_date(fallback_date_text)
        
        return ""

    def parse_date(self, date_text):
        """Enhanced date parsing"""
        if not date_text:
            return ""
            
        # Clean the date text
        date_text = re.sub(r'\s+', ' ', date_text.strip())
        
        # Try multiple date formats
        formats = [
            '%d %B %Y',     # 15 July 2025
            '%B %d, %Y',    # July 15, 2025
            '%d/%m/%Y',     # 15/07/2025
            '%Y-%m-%d',     # 2025-07-15
            '%d %b %Y',     # 15 Jul 2025
            '%b %d, %Y',    # Jul 15, 2025
            '%Y-%m-%dT%H:%M:%S',  # ISO format
            '%Y-%m-%dT%H:%M:%SZ', # ISO format with Z
        ]
        
        for fmt in formats:
            try: 
                parsed = datetime.strptime(date_text, fmt)
                return parsed.isoformat().split('T')[0]
            except (ValueError, TypeError): 
                continue
        
        # Try to extract just year-month-day from longer strings
        date_match = re.search(r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', date_text, re.IGNORECASE)
        if date_match:
            try:
                day, month_name, year = date_match.groups()
                parsed = datetime.strptime(f"{day} {month_name} {year}", '%d %B %Y')
                return parsed.isoformat().split('T')[0]
            except:
                pass
        
        return ""

    def _extract_text_from_pdf(self, pdf_url):
        """Extract text from PDF files"""
        if not pdfplumber: 
            return ""
        try:
            response = self.session.get(pdf_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            if 'application/pdf' not in response.headers.get('Content-Type', ''): 
                return ""
            with io.BytesIO(response.content) as f, pdfplumber.open(f) as pdf:
                return "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
        except Exception as e:
            self.logger.error(f"Failed to extract PDF text from {pdf_url}: {e}")
            return ""

    def extract_content(self, soup):
        """Enhanced content extraction with navigation content filtering"""
        content_selectors = [
            'div.prose',
            '.field--name-field-html .field__item',
            '.field--name-body .field__item',
            'main .content',
            'article .content',
            '.article-content',
            '.page-content',
            '.field--name-field-body',
            'main',
            'article'
        ]
        
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Remove unwanted elements more aggressively
                for unwanted in content_elem.select('script, style, nav, aside, .pager, .breadcrumb, .social-share, .filters, .filter-form, .pagination, .search-form'):
                    unwanted.decompose()
                
                content = content_elem.get_text(separator='\n').strip()
                if content and len(content) > 50:
                    # Additional filter for navigation content
                    if not self.is_navigation_content(content):
                        return content
        
        # Final fallback: get all paragraphs but filter navigation content
        paragraphs = soup.select('p')
        if paragraphs:
            content = '\n\n'.join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
            if content and len(content) > 50 and not self.is_navigation_content(content):
                return content
        
        return ""

    def run(self):
        """Run comprehensive ACMA scraper"""
        start_time = time.time()
        self.logger.info("Starting comprehensive ACMA scraper...")
        
        try:
            total_new_across_all_sources = 0
            
            # Scrape all content sources
            for source_name, source_url in CONTENT_SOURCES.items():
                if self.articles_processed_today >= DAILY_MODE_ARTICLE_LIMIT:
                    self.logger.info("Daily limit reached, stopping")
                    break
                    
                source_new = self.scrape_content_source(source_name, source_url)
                total_new_across_all_sources += source_new
                
                # Reset consecutive counter between sources
                self.consecutive_existing = 0
            
            self.total_new = total_new_across_all_sources
            self.logger.info(f"Total new articles found across all sources: {total_new_across_all_sources}")
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}", exc_info=True)
        finally:
            # Final save
            if self.new_items:
                self.save_data()
            self.logger.info(f"Scraping completed in {time.time() - start_time:.2f} seconds.")

def main():
    try:
        scraper = ACMAScraperEnhanced()
        scraper.run()
        print("\nScraping completed successfully!")
        print(f"New articles found: {scraper.total_new}")
        print(f"Total articles in database: {len(scraper.existing_data)}")
        sys.exit(0)
    except Exception as e:
        logging.getLogger("acma_scraper").critical(f"FATAL ERROR: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()