#!/usr/bin/env python3
"""
AER News Scraper
Scrapes news articles from Australian Energy Regulator website

Requirements:
pip install requests beautifulsoup4 fake-useragent PyPDF2 lxml selenium
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import json
import csv
import os
import time
import logging
from datetime import datetime
import re
from urllib.parse import urljoin, urlparse
import random
from fake_useragent import UserAgent
import PyPDF2
import io
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class AERScraper:
    def __init__(self, max_pages=3):
        """Initialize the scraper with configuration"""
        self.BASE_URL = "https://www.aer.gov.au"
        self.NEWS_URL = "https://www.aer.gov.au/news/articles"
        self.MAX_PAGES = max_pages
        self.DATA_DIR = "data"
        self.JSON_FILE = os.path.join(self.DATA_DIR, "aer_all_news.json")
        self.CSV_FILE = os.path.join(self.DATA_DIR, "aer_all_news.csv")
        self.LOG_FILE = os.path.join(self.DATA_DIR, "aer_scraper.log")
        
        # Create data directory if it doesn't exist
        os.makedirs(self.DATA_DIR, exist_ok=True)
        
        # Setup logging
        self.setup_logging()
        
        # Initialize session with stealth capabilities
        self.session = self.create_stealth_session()
        
        # Initialize Selenium driver for JavaScript-heavy pages (optional)
        self.driver = None
        self._selenium_available = self._check_selenium_availability()
        
        if not self._selenium_available:
            self.logger.info("Selenium not available - will use requests-only mode")
        
        # Load existing data for deduplication
        self.existing_articles = self.load_existing_data()
        
        self.logger.info(f"Initialized AER Scraper - Max pages: {self.MAX_PAGES}")

    def _check_selenium_availability(self):
        """Check if Selenium can be used"""
        try:
            import shutil
            
            # Check for Chrome/Chromium
            chrome_paths = [
                '/usr/bin/google-chrome',
                '/usr/bin/chrome', 
                '/usr/bin/chromium-browser',
                '/usr/bin/chromium',
                '/snap/bin/chromium'
            ]
            
            chrome_found = any(shutil.which(path) for path in chrome_paths)
            
            if not chrome_found:
                self.logger.info("No Chrome/Chromium browser found. To install:")
                self.logger.info("Ubuntu/Debian: sudo apt-get install chromium-browser")
                self.logger.info("Or: sudo apt-get install google-chrome-stable")
                return False
            
            return True
            
        except Exception as e:
            self.logger.debug(f"Selenium availability check failed: {e}")
            return False

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def create_stealth_session(self):
        """Create a session with enhanced stealth capabilities"""
        session = requests.Session()
        
        # Setup retry strategy
        retry_strategy = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=2
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Setup comprehensive headers to mimic real browser
        ua = UserAgent()
        session.headers.update({
            'User-Agent': ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,en-AU;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"'
        })
        
        return session

    def init_selenium_driver(self):
        """Initialize Selenium driver with stealth options"""
        if self.driver is None:
            try:
                # Try multiple approaches to get ChromeDriver working
                options = Options()
                options.add_argument('--headless')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_argument('--disable-web-security')
                options.add_argument('--allow-running-insecure-content')
                options.add_argument('--disable-features=VizDisplayCompositor')
                options.add_argument('--disable-gpu')
                options.add_argument('--remote-debugging-port=9222')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                
                # Add user agent to options
                try:
                    ua = UserAgent()
                    options.add_argument(f'--user-agent={ua.random}')
                except:
                    # Fallback if UserAgent fails
                    options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
                
                # Try different approaches
                approaches = [
                    # Approach 1: Try system chrome
                    lambda: webdriver.Chrome(options=options),
                    # Approach 2: Try with explicit service
                    lambda: self._try_chrome_with_service(options),
                    # Approach 3: Try chromium
                    lambda: self._try_chromium(options)
                ]
                
                for i, approach in enumerate(approaches):
                    try:
                        self.driver = approach()
                        if self.driver:
                            # Execute script to hide webdriver property
                            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                            self.logger.info(f"Selenium driver initialized successfully using approach {i+1}")
                            return
                    except Exception as approach_error:
                        self.logger.debug(f"Approach {i+1} failed: {approach_error}")
                        continue
                
                # If all approaches fail, log and continue without Selenium
                self.logger.warning("All Selenium initialization approaches failed - continuing without Selenium")
                self.driver = None
                
            except Exception as e:
                self.logger.warning(f"Selenium initialization failed: {e} - continuing without Selenium")
                self.driver = None

    def _try_chrome_with_service(self, options):
        """Try Chrome with explicit service"""
        from selenium.webdriver.chrome.service import Service
        import shutil
        
        # Try to find chrome/chromium binary
        chrome_paths = [
            '/usr/bin/google-chrome',
            '/usr/bin/chrome',
            '/usr/bin/chromium-browser',
            '/usr/bin/chromium',
            '/snap/bin/chromium'
        ]
        
        for chrome_path in chrome_paths:
            if shutil.which(chrome_path):
                options.binary_location = chrome_path
                service = Service()
                return webdriver.Chrome(service=service, options=options)
        
        raise Exception("No Chrome binary found")

    def _try_chromium(self, options):
        """Try with Chromium"""
        import shutil
        
        chromium_path = shutil.which('chromium-browser') or shutil.which('chromium')
        if chromium_path:
            options.binary_location = chromium_path
            return webdriver.Chrome(options=options)
        
        raise Exception("Chromium not found")

    def close_selenium_driver(self):
        """Close Selenium driver"""
        if self.driver:
            self.driver.quit()
            self.driver = None

    def load_existing_data(self):
        """Load existing articles for deduplication"""
        existing_articles = {}
        
        if os.path.exists(self.JSON_FILE):
            try:
                with open(self.JSON_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for article in data:
                        # Use URL as unique identifier
                        existing_articles[article.get('url', '')] = article
                self.logger.info(f"Loaded {len(existing_articles)} existing articles")
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
        
        return existing_articles

    def establish_session(self):
        """Establish session by visiting main page and collecting cookies"""
        try:
            self.logger.info("Establishing session by visiting main page...")
            
            # First, visit the main page to collect cookies
            response = self.session.get(self.BASE_URL)
            if response.status_code == 200:
                self.logger.info("Main page visited successfully")
                time.sleep(random.uniform(2, 4))
                
                # Visit the news section to establish browsing pattern
                news_response = self.session.get(self.NEWS_URL)
                if news_response.status_code == 200:
                    self.logger.info("News page visited successfully - session established")
                    time.sleep(random.uniform(2, 4))
                    return True
                else:
                    self.logger.warning(f"News page returned status {news_response.status_code}")
            else:
                self.logger.warning(f"Main page returned status {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"Error establishing session: {e}")
            
        return False

    def get_page_content(self, url, use_selenium=False):
        """Get page content with retry mechanism and enhanced stealth"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Add random delay to avoid being detected
                time.sleep(random.uniform(2, 5))
                
                # Update headers with fresh user agent for this request
                ua = UserAgent()
                self.session.headers.update({
                    'User-Agent': ua.random,
                    'Referer': self.BASE_URL if attempt == 0 else self.NEWS_URL
                })
                
                if use_selenium and self.driver is None:
                    self.init_selenium_driver()
                
                if use_selenium and self.driver:
                    self.logger.info(f"Using Selenium for {url}")
                    self.driver.get(url)
                    time.sleep(random.uniform(3, 6))
                    content = self.driver.page_source
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # Check if we got redirected
                    current_url = self.driver.current_url
                    if current_url != url:
                        self.logger.warning(f"Selenium redirected from {url} to {current_url}")
                    
                    return soup
                else:
                    response = self.session.get(url, timeout=30)
                    
                    # Check for redirects
                    if response.url != url:
                        self.logger.warning(f"Redirected from {url} to {response.url}")
                    
                    if response.status_code == 403:
                        self.logger.warning(f"403 Forbidden for {url} (attempt {attempt + 1})")
                        if attempt < max_retries - 1:
                            # Try with Selenium on next attempt
                            self.logger.info("Will retry with Selenium...")
                            time.sleep(random.uniform(10, 15))
                            return self.get_page_content(url, use_selenium=True)
                    elif response.status_code == 404:
                        self.logger.warning(f"404 Not Found for {url} - page may not exist")
                        return None
                    elif response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Check if this looks like an error page or empty results
                        page_title = soup.title.string if soup.title else ""
                        if "error" in page_title.lower() or "not found" in page_title.lower():
                            self.logger.warning(f"Error page detected: {page_title}")
                            return None
                            
                        return soup
                    else:
                        self.logger.warning(f"Status {response.status_code} for {url}")
                        
            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(10, 20))
        
        self.logger.error(f"Failed to get content for {url} after {max_retries} attempts")
        return None

    def extract_pdf_text(self, pdf_url):
        """Extract text from PDF file"""
        try:
            self.logger.info(f"Extracting text from PDF: {pdf_url}")
            
            response = self.session.get(pdf_url, timeout=60)
            if response.status_code != 200:
                self.logger.error(f"Failed to download PDF: {response.status_code}")
                return ""
            
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                page_text = page.extract_text()
                text += page_text + "\n"
            
            # Clean up text - remove excessive whitespace and unwanted characters
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'[^\w\s\.\,\;\:\!\?\-\(\)]', '', text)
            text = text.strip()
            
            self.logger.info(f"Successfully extracted {len(text)} characters from PDF")
            return text
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF text from {pdf_url}: {e}")
            return ""

    def extract_links_from_content(self, soup, base_url):
        """Extract all relevant links from article content"""
        links = []
        
        try:
            # Find all links in the content area - simple approach
            content_areas = soup.find_all(['div', 'section', 'article'])
            
            for area in content_areas:
                class_list = area.get('class', [])
                class_str = ' '.join(class_list) if class_list else ''
                # Look for content, body, or main in class names
                if any(keyword in class_str.lower() for keyword in ['content', 'body', 'main']):
                    for link in area.find_all('a', href=True):
                        href = link.get('href')
                        if href:
                            full_url = urljoin(base_url, href)
                            # Filter out navigation and unrelated links
                            if not any(skip in href.lower() for skip in 
                                     ['javascript:', 'mailto:', '#', 'tel:']):
                                links.append({
                                    'url': full_url,
                                    'text': link.get_text(strip=True),
                                    'title': link.get('title', '')
                                })
            
            # Remove duplicates
            seen_urls = set()
            unique_links = []
            for link in links:
                if link['url'] not in seen_urls:
                    seen_urls.add(link['url'])
                    unique_links.append(link)
            
            return unique_links[:10]  # Limit to first 10 relevant links
            
        except Exception as e:
            self.logger.error(f"Error extracting links: {e}")
            return []

    def get_article_links_from_page(self, page_num=0):
        """Get article links from a specific page"""
        try:
            if page_num == 0:
                url = self.NEWS_URL
            else:
                url = f"{self.NEWS_URL}?page={page_num}"
            
            self.logger.info(f"Fetching article links from page {page_num}: {url}")
            
            soup = self.get_page_content(url)
            if not soup:
                self.logger.warning(f"Failed to get page content for page {page_num}, trying with Selenium...")
                soup = self.get_page_content(url, use_selenium=True)
                if not soup:
                    self.logger.error(f"Failed to get page {page_num} even with Selenium")
                    return []
            
            # Check if we got a malformed page (no body tag indicates serious issues)
            body = soup.find('body')
            if not body:
                self.logger.warning(f"No body tag found on page {page_num}, likely rate limited or blocked")
                
                # Strategy 1: Try refreshing session with longer delay
                self.logger.info("Strategy 1: Refreshing session and waiting...")
                self.establish_session()
                time.sleep(random.uniform(15, 25))  # Longer delay
                
                # Update headers to look more human
                ua = UserAgent()
                self.session.headers.update({
                    'User-Agent': ua.random,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                })
                
                # Retry with fresh session
                soup = self.get_page_content(url)
                if soup and soup.find('body'):
                    self.logger.info("Session refresh with delay resolved the issue")
                else:
                    # Strategy 2: Try Selenium only if available
                    if self.driver is not None or self.init_selenium_driver():
                        self.logger.info("Strategy 2: Trying Selenium...")
                        try:
                            soup = self.get_page_content(url, use_selenium=True)
                            if soup and soup.find('body'):
                                self.logger.info("Selenium resolved the issue")
                            else:
                                self.logger.warning(f"Selenium didn't help for page {page_num}")
                                return []
                        except Exception as selenium_error:
                            self.logger.warning(f"Selenium failed: {selenium_error}")
                            return []
                    else:
                        self.logger.warning(f"No Selenium available and session refresh failed for page {page_num}")
                        return []
            
            # Debug: Save problematic pages for analysis
            if page_num >= 6:  # Save pages that might be problematic
                debug_file = os.path.join(self.DATA_DIR, f"debug_page_{page_num}.html")
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(str(soup.prettify()))
                self.logger.info(f"Saved debug HTML for page {page_num} to {debug_file}")
            
            # Check if this is a valid page by looking for the results header
            view_header = soup.find('div', class_='view-header')
            if view_header:
                header_text = view_header.get_text(strip=True)
                self.logger.info(f"Page {page_num} header: {header_text}")
                
                # Extract pagination info like "3490 result(s), displaying 49 to 60"
                if 'result(s)' in header_text and 'displaying' in header_text:
                    self.logger.info(f"Valid results page found: {header_text}")
                else:
                    self.logger.warning(f"Unexpected header format: {header_text}")
            else:
                self.logger.warning(f"No view-header found on page {page_num}")
                
                # Check if we might have been redirected to a different page
                page_title = soup.title.string if soup.title else ""
                self.logger.info(f"Page title: {page_title}")
                
                # Look for error indicators
                error_indicators = soup.find_all(string=re.compile(r'error|not found|invalid|access denied', re.I))
                if error_indicators:
                    self.logger.warning(f"Error indicators found: {[err.strip() for err in error_indicators[:3]]}")
                
                # Check the actual content length
                page_content = str(soup)
                self.logger.info(f"Page content length: {len(page_content)} characters")
                
                # Look for common error patterns
                if len(page_content) < 1000:  # Very short page, likely an error
                    self.logger.warning("Very short page content - likely an error page")
                    # Log first 500 characters for debugging
                    self.logger.warning(f"Page content preview: {page_content[:500]}")
                
                # Check if page contains any meaningful content
                body = soup.find('body')
                if body:
                    body_text = body.get_text(strip=True)
                    self.logger.info(f"Body text length: {len(body_text)} characters")
                    if len(body_text) < 100:
                        self.logger.warning(f"Very little body text: {body_text[:200]}")
                else:
                    self.logger.warning("No body tag found")
                    
                # Check for specific AER content indicators
                aer_indicators = soup.find_all(['div', 'header', 'nav'])
                aer_count = 0
                for elem in aer_indicators:
                    class_list = elem.get('class', [])
                    class_str = ' '.join(class_list) if class_list else ''
                    if any(keyword in class_str.lower() for keyword in ['aer', 'header', 'nav']):
                        aer_count += 1
                
                self.logger.info(f"Found {aer_count} AER/header/nav elements")
            
            article_links = []
            
            # Strategy 1: Find articles using the correct card structure
            # Look for divs that contain both 'node' and 'node--type-article' in their class
            all_divs = soup.find_all('div')
            article_cards = []
            for div in all_divs:
                class_list = div.get('class', [])
                class_str = ' '.join(class_list) if class_list else ''
                if 'node' in class_str and 'node--type-article' in class_str:
                    article_cards.append(div)
            
            self.logger.info(f"Found {len(article_cards)} article cards on page {page_num}")
            
            for i, card in enumerate(article_cards):
                # Look for the link in card title
                title_div = card.find('h3', class_='card__title')
                if title_div:
                    link = title_div.find('a', href=True)
                    if link:
                        href = link['href']
                        self.logger.debug(f"Card {i}: Found link {href}")
                        
                        # Ensure it's a proper article link
                        if href.startswith('/news/articles/') and href.count('/') >= 4:
                            full_url = urljoin(self.BASE_URL, href)
                            article_links.append(full_url)
                            self.logger.debug(f"Card {i}: Added article link {full_url}")
            
            # Strategy 2: Look for stretched-link class specifically
            if not article_links:
                self.logger.info("Strategy 2: Looking for stretched-link elements")
                stretched_links = soup.find_all('a', class_='stretched-link')
                
                for i, link in enumerate(stretched_links):
                    href = link.get('href', '')
                    if href.startswith('/news/articles/') and href.count('/') >= 4:
                        full_url = urljoin(self.BASE_URL, href)
                        article_links.append(full_url)
                        self.logger.debug(f"Stretched link {i}: Added {full_url}")
            
            # Strategy 3: Fallback - H3 with card__title class
            if not article_links:
                self.logger.info("Strategy 3: Looking for H3 card titles")
                h3_titles = soup.find_all('h3', class_='card__title')
                
                for i, h3 in enumerate(h3_titles):
                    link = h3.find('a', href=True)
                    if link:
                        href = link['href']
                        if href.startswith('/news/articles/') and href.count('/') >= 4:
                            full_url = urljoin(self.BASE_URL, href)
                            article_links.append(full_url)
                            self.logger.debug(f"H3 title {i}: Added {full_url}")
            
            # Remove duplicates
            unique_links = list(set(article_links))
            
            # Enhanced debugging if no articles found
            if not unique_links:
                self.logger.warning(f"No articles found on page {page_num}")
                
                # Check if we've reached the actual end by examining pagination
                pagination_nav = soup.find('nav')
                pagination_found = False
                last_page_num = None
                current_page_from_pagination = None
                
                if pagination_nav:
                    # Look for pagination links
                    page_links = pagination_nav.find_all('a')
                    page_numbers = []
                    
                    # Find current page from pagination
                    current_page_link = pagination_nav.find('a', {'aria-current': 'page'})
                    if current_page_link:
                        current_text = current_page_link.get_text(strip=True)
                        try:
                            current_page_from_pagination = int(current_text)
                            self.logger.info(f"Current page according to pagination: {current_page_from_pagination}")
                            
                            # Check if we're getting the wrong page (0-based vs 1-based indexing issue)
                            expected_page_display = page_num + 1  # Convert 0-based to 1-based
                            if current_page_from_pagination != expected_page_display:
                                self.logger.warning(f"Page mismatch! Expected page {expected_page_display}, but pagination shows page {current_page_from_pagination}")
                                # This might be normal - the website might use different indexing
                                # Let's continue and see if we find articles anyway
                        except ValueError:
                            pass
                    
                    for link in page_links:
                        href = link.get('href', '')
                        if 'page=' in href:
                            try:
                                page_val = int(href.split('page=')[1])
                                page_numbers.append(page_val)
                            except (ValueError, IndexError):
                                pass
                        
                        # Check for "last page" link
                        if link.get('title') == 'Go to last page':
                            try:
                                last_page_num = int(href.split('page=')[1])
                                pagination_found = True
                            except (ValueError, IndexError):
                                pass
                    
                    if page_numbers:
                        max_visible_page = max(page_numbers)
                        self.logger.info(f"Pagination found. Current max visible page: {max_visible_page}")
                        
                        if last_page_num:
                            self.logger.info(f"Last page is: {last_page_num}")
                            
                            # Don't stop based on pagination mismatch - the page might still have content
                            # Only stop if we're definitely beyond the last page
                            if page_num > last_page_num:
                                self.logger.info("Beyond the actual last page, stopping")
                                return []
                        
                        # If current page is beyond visible pagination, we might be at the end
                        if page_num > max_visible_page:
                            self.logger.info(f"Page {page_num} is beyond max visible page {max_visible_page}")
                
                # Check for the expected structure to debug detection issues
                layout_items = soup.find_all('div', class_='views-layout__item')
                card_titles = soup.find_all('h3', class_='card__title')
                stretched_links = soup.find_all('a', class_='stretched-link')
                
                self.logger.warning(f"Page structure: {len(layout_items)} layout items, "
                                  f"{len(card_titles)} card titles, "
                                  f"{len(stretched_links)} stretched links")
                
                # Sample some links to see what we're missing
                all_links = soup.find_all('a', href=True)
                news_links = []
                for link in all_links:
                    if '/news/articles/' in link['href']:
                        news_links.append(link['href'])
                
                self.logger.warning(f"Found {len(news_links)} total news links: {news_links[:10]}")
                
                # If we have a view-header but no articles, might be an empty results page
                if view_header and not news_links:
                    self.logger.info("Empty results page detected - likely reached the end")
                    return []
            
            self.logger.info(f"Found {len(unique_links)} unique article links on page {page_num}")
            
            # Log first few URLs found for verification
            if unique_links:
                for i, link in enumerate(unique_links[:3]):
                    self.logger.info(f"Sample article {i+1}: {link}")
            
            return unique_links
            
        except Exception as e:
            self.logger.error(f"Error getting article links from page {page_num}: {e}")
            return []

    def parse_article_page(self, url):
        """Parse individual article page"""
        try:
            self.logger.info(f"Parsing article: {url}")
            
            soup = self.get_page_content(url)
            if not soup:
                return None
            
            # Extract title from H1
            title = ""
            title_elem = soup.find('h1')
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Extract content from the body field
            content = ""
            # Look for field--name-field-body in class names
            all_divs = soup.find_all('div')
            for div in all_divs:
                class_list = div.get('class', [])
                class_str = ' '.join(class_list) if class_list else ''
                if 'field--name-field-body' in class_str:
                    content = div.get_text(strip=True)
                    break
            
            # Fallback content selectors
            if not content:
                content_candidates = [
                    soup.find('div', class_='block-body'),
                    soup.find('main'),
                ]
                
                # Also check for any div with 'content' in class name
                for div in all_divs:
                    class_list = div.get('class', [])
                    class_str = ' '.join(class_list) if class_list else ''
                    if 'content' in class_str.lower():
                        content_candidates.append(div)
                        break
                
                for elem in content_candidates:
                    if elem:
                        content = elem.get_text(strip=True)
                        if content:  # Only use if non-empty
                            break
            
            # Extract article type
            article_type = "News Release"  # Default
            for div in all_divs:
                class_list = div.get('class', [])
                class_str = ' '.join(class_list) if class_list else ''
                if 'field--name-field-article-type' in class_str:
                    type_item = div.find('div', class_='field__item')
                    if type_item:
                        type_text = type_item.get_text(strip=True).lower()
                        if 'communication' in type_text:
                            article_type = "Communications"
                        elif 'speech' in type_text:
                            article_type = "Speeches"
                        elif 'news release' in type_text:
                            article_type = "News Release"
                        else:
                            article_type = type_item.get_text(strip=True)
                    break
            
            # Extract category from segments field
            category = "General"  # Default
            segments = []
            for div in all_divs:
                class_list = div.get('class', [])
                class_str = ' '.join(class_list) if class_list else ''
                if 'field--name-field-segments' in class_str:
                    segment_items = div.find_all('div', class_='field__item')
                    if segment_items:
                        segments = [item.get_text(strip=True) for item in segment_items]
                        # Use the first segment as category
                        if segments:
                            first_segment = segments[0].lower()
                            if 'distribution' in first_segment:
                                category = "Distribution"
                            elif 'retail' in first_segment:
                                category = "Retail"
                            elif 'transmission' in first_segment:
                                category = "Transmission"
                            elif 'wholesale' in first_segment:
                                category = "Wholesale"
                            else:
                                category = segments[0]
                    break
            
            # Extract sectors
            sectors = []
            for div in all_divs:
                class_list = div.get('class', [])
                class_str = ' '.join(class_list) if class_list else ''
                if 'field--name-field-sectors' in class_str:
                    sector_items = div.find_all('div', class_='field__item')
                    sectors = [item.get_text(strip=True) for item in sector_items]
                    break
            
            # If no category from segments, try sectors
            if category == "General" and sectors:
                category = sectors[0]
            
            # Extract date
            published_date = ""
            for div in all_divs:
                class_list = div.get('class', [])
                class_str = ' '.join(class_list) if class_list else ''
                if 'field--name-field-date' in class_str:
                    time_elem = div.find('time')
                    if time_elem:
                        published_date = time_elem.get('datetime', '')
                        if not published_date:
                            published_date = time_elem.get_text(strip=True)
                    break
            
            # Fallback date extraction
            if not published_date:
                page_text = soup.get_text()
                # Simple date patterns
                patterns = [
                    r'\d{1,2}\s+\w+\s+\d{4}',
                    r'\d{4}-\d{2}-\d{2}',
                    r'\d{1,2}/\d{1,2}/\d{4}'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, page_text)
                    if match:
                        published_date = match.group(0)
                        break
            
            # Extract image
            image_url = ""
            images = soup.find_all('img')
            for img in images:
                img_class = ' '.join(img.get('class', []))
                if any(keyword in img_class.lower() for keyword in ['hero', 'featured', 'main']):
                    if img.get('src'):
                        image_url = urljoin(self.BASE_URL, img['src'])
                        break
            
            # If no featured image found, use first image
            if not image_url and images:
                first_img = images[0]
                if first_img.get('src'):
                    image_url = urljoin(self.BASE_URL, first_img['src'])
            
            # Extract related links
            related_links = self.extract_links_from_content(soup, self.BASE_URL)
            
            # Check for PDF links and extract content
            pdf_content = ""
            all_links = soup.find_all('a', href=True)
            pdf_links = []
            for link in all_links:
                href = link.get('href', '')
                if href.lower().endswith('.pdf'):
                    pdf_links.append(link)
            
            if pdf_links:
                # Use the first PDF link as specified
                first_pdf = pdf_links[0]
                pdf_url = urljoin(self.BASE_URL, first_pdf['href'])
                pdf_content = self.extract_pdf_text(pdf_url)
                
                # If main content is minimal but PDF has content, use PDF content
                if len(content) < 200 and len(pdf_content) > 200:
                    content = pdf_content
            
            article_data = {
                'url': url,
                'title': title,
                'content': content,
                'pdf_content': pdf_content,
                'article_type': article_type,
                'category': category,
                'sectors': sectors,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'image_url': image_url,
                'related_links': related_links
            }
            
            return article_data
            
        except Exception as e:
            self.logger.error(f"Error parsing article {url}: {e}")
            return None

    def scrape_all_articles(self):
        """Main method to scrape all articles"""
        try:
            # Establish session first
            if not self.establish_session():
                self.logger.error("Failed to establish session")
                return
            
            all_articles = []
            new_articles_count = 0
            consecutive_empty_pages = 0
            max_consecutive_empty = 3  # Stop after 3 consecutive empty pages
            session_refresh_interval = 20  # Refresh session every 20 pages
            
            # Get article links from all pages
            for page_num in range(self.MAX_PAGES):
                self.logger.info(f"Processing page {page_num + 1}/{self.MAX_PAGES}")
                
                # Refresh session periodically to avoid timeouts
                if page_num > 0 and page_num % session_refresh_interval == 0:
                    self.logger.info("Refreshing session to avoid timeouts...")
                    self.establish_session()
                
                article_links = self.get_article_links_from_page(page_num)
                
                if not article_links:
                    consecutive_empty_pages += 1
                    self.logger.warning(f"No articles found on page {page_num} ({consecutive_empty_pages} consecutive empty pages)")
                    
                    if consecutive_empty_pages >= max_consecutive_empty:
                        self.logger.info(f"Stopping after {consecutive_empty_pages} consecutive empty pages")
                        break
                    else:
                        # Continue to next page - might be a temporary issue
                        self.logger.info(f"Continuing to next page (allowing up to {max_consecutive_empty} empty pages)")
                        
                        # Add extra delay after empty pages to avoid overwhelming server
                        time.sleep(random.uniform(5, 10))
                        continue
                else:
                    # Reset counter when we find articles
                    consecutive_empty_pages = 0
                    self.logger.info(f"Found {len(article_links)} articles on page {page_num}")
                
                # Process each article
                for link in article_links:
                    # Check if article already exists (deduplication)
                    if link in self.existing_articles:
                        self.logger.info(f"Article already exists, skipping: {link}")
                        continue
                    
                    # Parse the article
                    article_data = self.parse_article_page(link)
                    
                    if article_data:
                        all_articles.append(article_data)
                        new_articles_count += 1
                        self.logger.info(f"Successfully scraped: {article_data['title'][:50]}...")
                        
                        # Add delay between articles
                        time.sleep(random.uniform(2, 5))
                    else:
                        self.logger.warning(f"Failed to parse article: {link}")
                    
                    # Add delay to avoid overwhelming the server
                    time.sleep(random.uniform(1, 3))
                
                # Add longer delay between pages
                time.sleep(random.uniform(3, 7))
            
            # Combine with existing articles
            combined_articles = list(self.existing_articles.values()) + all_articles
            
            # Save results
            self.save_results(combined_articles)
            
            self.logger.info(f"Scraping completed. New articles: {new_articles_count}, Total articles: {len(combined_articles)}")
            
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Error in main scraping process: {e}")
        finally:
            self.close_selenium_driver()

    def save_results(self, articles):
        """Save results to JSON and CSV files"""
        try:
            # Save JSON
            with open(self.JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(articles, f, indent=2, ensure_ascii=False)
            
            # Save CSV - handle dynamic fieldnames
            if articles:
                # Get all possible fieldnames from all articles
                all_fieldnames = set()
                for article in articles:
                    all_fieldnames.update(article.keys())
                
                # Define our expected fieldnames in order
                expected_fieldnames = [
                    'url', 'title', 'content', 'pdf_content', 'article_type', 
                    'category', 'sectors', 'published_date', 'scraped_date', 'image_url', 'related_links'
                ]
                
                # Add any extra fieldnames that exist in the data
                extra_fieldnames = sorted(all_fieldnames - set(expected_fieldnames))
                fieldnames = expected_fieldnames + extra_fieldnames
                
                with open(self.CSV_FILE, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for article in articles:
                        # Create CSV row with all possible fields
                        csv_article = {}
                        for field in fieldnames:
                            value = article.get(field, '')
                            
                            # Convert complex types to strings for CSV
                            if isinstance(value, (list, dict)):
                                csv_article[field] = json.dumps(value, ensure_ascii=False)
                            else:
                                csv_article[field] = value
                        
                        writer.writerow(csv_article)
            
            self.logger.info(f"Results saved to {self.JSON_FILE} and {self.CSV_FILE}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            # Save what we can in JSON format at least
            try:
                with open(self.JSON_FILE, 'w', encoding='utf-8') as f:
                    json.dump(articles, f, indent=2, ensure_ascii=False)
                self.logger.info("At least JSON file was saved successfully")
            except Exception as json_error:
                self.logger.error(f"Failed to save even JSON file: {json_error}")

def main():
    """Main function"""
    # The website appears to have pages 0-7 working (96 articles / 12 per page = 8 pages)
    # Let's be more conservative and focus on getting those pages reliably
    MAX_PAGES = 3  # Reduced to focus on pages that likely exist
    
    scraper = AERScraper(max_pages=MAX_PAGES)
    scraper.scrape_all_articles()

if __name__ == "__main__":
    main()