#!/usr/bin/env python3
"""
AHPRA News Scraper - FIXED VERSION
Comprehensive scraper for AHPRA news articles with PDF extraction and deduplication
"""

import requests
import json
import os
import time
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Set, Optional
import re
from pathlib import Path

# Third-party imports
try:
    from bs4 import BeautifulSoup
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.service import Service
    from selenium_stealth import stealth
    import PyPDF2
    import requests_cache
    from fake_useragent import UserAgent
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install beautifulsoup4 selenium selenium-stealth PyPDF2 requests-cache fake-useragent")
    exit(1)

class AHPRANewsScraper:
    # ============================================
    # CONFIGURATION - Change this as needed
    # ============================================
    MAX_RECENT_ARTICLES = 10  # For subsequent runs, check this many recent articles for new ones
    TEST_MODE = False  # Set to True for testing (scrapes only 3 articles), False for full scrape
    TEST_ARTICLES_COUNT = 3  # Number of articles to scrape in test mode
    
    def __init__(self, data_folder: str = "data", max_recent_articles: int = None, test_mode: bool = None):
        """Initialize the AHPRA news scraper"""
        self.base_url = "https://www.ahpra.gov.au"
        self.news_url = "https://www.ahpra.gov.au/News.aspx"
        self.data_folder = Path(data_folder)
        self.data_folder.mkdir(exist_ok=True)
        
        # File paths
        self.json_file = self.data_folder / "ahpra_news.json"
        self.csv_file = self.data_folder / "ahpra_news.csv"
        self.log_file = self.data_folder / "ahpra_scraper.log"
        
        # Article processing configuration
        self.is_first_run = not self.json_file.exists()
        if max_recent_articles is not None:
            self.max_recent_articles = max_recent_articles
        else:
            self.max_recent_articles = self.MAX_RECENT_ARTICLES
        
        # Test mode configuration
        if test_mode is not None:
            self.test_mode = test_mode
        else:
            self.test_mode = self.TEST_MODE
        
        # Setup logging
        self.setup_logging()
        
        # Initialize session with caching
        self.session = requests_cache.CachedSession(
            cache_name=str(self.data_folder / 'ahpra_cache'),
            expire_after=3600  # 1 hour cache
        )
        
        # User agent rotation
        self.ua = UserAgent()
        
        # Selenium driver
        self.driver = None
        
        # Track processed articles for deduplication
        self.existing_articles: Set[str] = set()
        self.load_existing_articles()
        
        # PDF cache to avoid duplicate downloads
        self.pdf_cache: Dict[str, str] = {}
        
        # Debug sample counter for full mode
        self._debug_sample_count = 0
        
        mode_desc = "TEST MODE" if self.test_mode else "FULL MODE"
        articles_desc = f"{self.TEST_ARTICLES_COUNT} articles (test)" if self.test_mode else (f"{self.max_recent_articles} recent articles" if not self.is_first_run else "ALL articles")
        
        self.logger.info(f"AHPRA News Scraper initialized - {mode_desc} - First run: {self.is_first_run}, Will process: {articles_desc}")

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def setup_selenium(self):
        """Setup Selenium driver with stealth mode"""
        try:
            chrome_options = Options()
            
            # Essential stability options for Linux
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Updated user agent to match current Chrome version
            chrome_options.add_argument(f'--user-agent={self.ua.random}')
            
            # Stealth options
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Performance optimizations
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            
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
            
            # Initialize driver with simplified service configuration
            service_kwargs = {}
            if chromedriver_path:
                service_kwargs['executable_path'] = chromedriver_path
            
            service = Service(**service_kwargs)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            # Remove automation indicators
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info("Chrome driver initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Selenium: {e}")
            return False

    def get_browser_headers(self) -> Dict[str, str]:
        """Generate realistic browser headers"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }

    def load_existing_articles(self):
        """Load existing articles from JSON file for deduplication"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    for article in existing_data:
                        # Create unique identifier from URL and title
                        identifier = f"{article.get('url', '')}_{article.get('title', '')}"
                        self.existing_articles.add(identifier)
                self.logger.info(f"Loaded {len(self.existing_articles)} existing articles")
            except Exception as e:
                self.logger.error(f"Error loading existing articles: {e}")

    def establish_session(self):
        """Establish session by visiting main page first"""
        try:
            headers = self.get_browser_headers()
            
            # Visit main page first
            self.logger.info("Establishing session...")
            response = self.session.get(self.base_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Small delay to mimic human behavior
            time.sleep(2)
            
            # Visit news page
            response = self.session.get(self.news_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            self.logger.info("Session established successfully")
            return response
            
        except Exception as e:
            self.logger.error(f"Failed to establish session: {e}")
            return None

    def expand_all_years(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Use Selenium to expand all year sections"""
        try:
            if not self.driver:
                if not self.setup_selenium():
                    return soup
            
            self.driver.get(self.news_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "article-year"))
            )
            
            # Find all collapsed year sections
            year_triggers = self.driver.find_elements(By.CSS_SELECTOR, ".article-year h3 a.trigger")
            
            for trigger in year_triggers:
                try:
                    # Check if section is collapsed
                    parent_h3 = trigger.find_element(By.XPATH, "..")
                    if "collapsed" in parent_h3.get_attribute("class"):
                        self.driver.execute_script("arguments[0].click();", trigger)
                        time.sleep(1)  # Wait for expansion
                except Exception as e:
                    self.logger.warning(f"Could not expand year section: {e}")
            
            # Wait a bit for all content to load
            time.sleep(3)
            
            # Get the updated page source
            updated_html = self.driver.page_source
            return BeautifulSoup(updated_html, 'html.parser')
            
        except Exception as e:
            self.logger.error(f"Error expanding years with Selenium: {e}")
            return soup

    def extract_article_links(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract all article links from the news page"""
        articles = []
        
        # Find all article containers
        article_containers = soup.find_all('div', class_='article-list')
        
        for container in article_containers:
            try:
                # Extract date
                date_elem = container.find('div', class_='release')
                date_str = date_elem.text.strip() if date_elem else ""
                
                # Extract title and link
                summary_elem = container.find('div', class_='summary')
                if summary_elem:
                    title_elem = summary_elem.find('h4')
                    title = title_elem.text.strip() if title_elem else ""
                    
                    # Find "Read More" link
                    link_elem = summary_elem.find('a', href=True)
                    if link_elem and link_elem.get('href'):
                        url = urljoin(self.base_url, link_elem['href'])
                        
                        # Extract preview text
                        preview_elem = summary_elem.find('p')
                        preview = ""
                        if preview_elem:
                            # Remove the "Read More" link text
                            preview_text = preview_elem.get_text(strip=True)
                            preview = re.sub(r'Read More$', '', preview_text).strip()
                        
                        articles.append({
                            'title': title,
                            'url': url,
                            'date': date_str,
                            'preview': preview
                        })
                        
            except Exception as e:
                self.logger.warning(f"Error extracting article info: {e}")
        
        self.logger.info(f"Found {len(articles)} articles")
        return articles

    def download_pdf(self, pdf_url: str) -> str:
        """Download and extract text from PDF"""
        try:
            # Check cache first
            if pdf_url in self.pdf_cache:
                return self.pdf_cache[pdf_url]
            
            headers = self.get_browser_headers()
            response = self.session.get(pdf_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Save PDF temporarily
            pdf_path = self.data_folder / "temp.pdf"
            with open(pdf_path, 'wb') as f:
                f.write(response.content)
            
            # Extract text
            text = ""
            with open(pdf_path, 'rb') as f:
                pdf_reader = PyPDF2.PdfReader(f)
                for page in pdf_reader.pages:
                    text += page.extract_text()
            
            # Clean up text
            text = re.sub(r'\s+', ' ', text).strip()
            text = re.sub(r'[^\w\s\-.,;:!?()"\']', '', text)
            
            # Cache the result
            self.pdf_cache[pdf_url] = text
            
            # Remove temporary file
            pdf_path.unlink()
            
            self.logger.info(f"Extracted {len(text)} characters from PDF")
            return text
            
        except Exception as e:
            self.logger.error(f"Error downloading PDF {pdf_url}: {e}")
            return ""

    def extract_article_content(self, article_url: str) -> Dict:
        """Extract content from individual article page - FIXED VERSION"""
        try:
            headers = self.get_browser_headers()
            response = self.session.get(article_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # DEBUG: Save raw HTML for troubleshooting (only in test mode)
            if self.test_mode:
                debug_file = self.data_folder / f"debug_{article_url.split('/')[-1]}.html"
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(str(soup.prettify()))
                self.logger.info(f"Debug: Saved raw HTML to {debug_file}")
            elif hasattr(self, '_debug_sample_count') and self._debug_sample_count < 2:
                # In full mode, only save HTML for first 2 articles as samples
                debug_file = self.data_folder / f"debug_sample_{article_url.split('/')[-1]}.html"
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(str(soup.prettify()))
                self.logger.info(f"Debug sample: Saved raw HTML to {debug_file}")
                self._debug_sample_count = getattr(self, '_debug_sample_count', 0) + 1
            
            # Extract title - try multiple selectors
            title = ""
            title_selectors = [
                'h1.heading',
                'h1',
                '.heading',
                'title'
            ]
            
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    if title and title != "Australian Health Practitioner Regulation Agency":
                        break
            
            self.logger.info(f"Extracted title: '{title}'")
            
            # Extract date - improved date extraction with specific AHPRA patterns
            date_str = ""
            date_patterns = [
                r'\b(\d{1,2})\s+(\w{3})\s+(\d{4})\b',  # "07 Jul 2025"
                r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b',    # "07/07/2025"
                r'\b(\d{4})-(\d{1,2})-(\d{1,2})\b',    # "2025-07-07"
            ]
            
            # Look for date in various places - prioritize main content areas
            date_candidates = []
            
            # First check in main content areas where the actual content is
            main_content_selectors = ['.main', '#content', '.container']
            
            for selector in main_content_selectors:
                main_elem = soup.select_one(selector)
                if main_elem:
                    # Look for dates in paragraph tags and strong tags
                    for elem in main_elem.find_all(['p', 'strong'], limit=5):
                        elem_text = elem.get_text(strip=True)
                        if len(elem_text) < 50:  # Short text more likely to contain date
                            for pattern in date_patterns:
                                matches = re.findall(pattern, elem_text)
                                if matches:
                                    date_candidates.extend(matches)
                    
                    if date_candidates:
                        break
            
            # Process date candidates
            if date_candidates:
                # Take the first valid date and format it properly
                first_match = date_candidates[0]
                if len(first_match) == 3:
                    # Handle different date formats
                    if '/' in str(first_match):
                        # Format: day/month/year -> day month year
                        day, month, year = first_match
                        # Convert month number to abbreviated name
                        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        if month.isdigit() and 1 <= int(month) <= 12:
                            month_name = month_names[int(month)]
                            date_str = f"{day} {month_name} {year}"
                        else:
                            date_str = f"{day}/{month}/{year}"
                    else:
                        date_str = " ".join(first_match)
                    
            self.logger.info(f"Extracted date: '{date_str}'")
            
            # Extract main content - FIXED VERSION based on analyzer results
            content_parts = []
            
            # Based on the analyzer, we need to look in .main, not #page-body
            # But we need to be more specific to avoid navigation elements
            main_content_selectors = ['.main', '#content', '.container']
            
            for selector in main_content_selectors:
                main_elem = soup.select_one(selector)
                if main_elem:
                    self.logger.info(f"Extracting content from: {selector}")
                    
                    # First, try to find the actual article content area
                    # Look for the div with id="page-body" within the main element
                    article_content = main_elem.find('div', id='page-body')
                    if not article_content:
                        # If no page-body, look for other content indicators
                        article_content = main_elem.find('div', class_='col-md-9') or main_elem.find('div', class_='col-sm-12')
                    
                    if not article_content:
                        # Fallback to main element but exclude navigation
                        article_content = main_elem
                    
                    # Get all paragraphs, headings, and list items from the article content
                    for elem in article_content.find_all(['p', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'td']):
                        # Skip if element is inside navigation areas
                        if elem.find_parent(['nav', 'ul.section-navigation', 'div.nav-wrap']):
                            continue
                        
                        text = elem.get_text(strip=True)
                        
                        # Skip if it's empty or too short
                        if not text or len(text) < 20:
                            continue
                            
                        # Skip if it's navigation or metadata - enhanced list
                        if any(skip_phrase in text.lower() for skip_phrase in [
                            'home news', 'breadcrumb', 'navigation', 'skip to content',
                            'page reviewed', 'back to top', 'share', 'print', 'email',
                            'follow us', 'contact us', 'accessibility', 'privacy',
                            'recommendations from the coroner', 'consultations', 'collapse',
                            'expand', 'april 2017', 'march 2016', 'november 2015', 'march 2012',
                            'past consultations', 'web service announcements', 'employer connect',
                            'australian health practitioner regulation agency'
                        ]):
                            continue
                        
                        # Skip if it contains navigation-like patterns
                        if re.search(r'collapse|expand|april \d{4}|march \d{4}|november \d{4}', text.lower()):
                            continue
                        
                        # Skip if it's the date we already extracted
                        if date_str and date_str in text:
                            continue
                            
                        # Skip if it's the title we already extracted
                        if title and title.lower() in text.lower():
                            continue
                        
                        # Skip breadcrumb-like content
                        if 'home' in text.lower() and len(text) < 100:
                            continue
                        
                        # Skip if it looks like a navigation menu item
                        if len(text) < 100 and any(nav_word in text.lower() for nav_word in ['news', 'consultations', 'recommendations']):
                            continue
                        
                        # Add to content if it seems like actual content
                        if len(text) > 30:  # Substantial text
                            content_parts.append(text)
                    
                    # If we found content, break
                    if content_parts:
                        break
            
            # Remove duplicates while preserving order
            seen = set()
            unique_content_parts = []
            for part in content_parts:
                if part not in seen:
                    seen.add(part)
                    unique_content_parts.append(part)
            
            content_parts = unique_content_parts
            
            # Join all content
            content_text = "\n\n".join(content_parts)
            
            # Clean up the content
            content_text = re.sub(r'\n{3,}', '\n\n', content_text)
            content_text = re.sub(r'\s+', ' ', content_text)
            content_text = content_text.strip()
            
            # Extract related links - look in the main content area
            related_links = []
            main_elem = soup.select_one('.main') or soup.select_one('#content') or soup.select_one('.container')
            
            if main_elem:
                for link in main_elem.find_all('a', href=True):
                    href = link.get('href')
                    if href and not href.startswith('#'):
                        # Skip mailto links
                        if href.startswith('mailto:'):
                            continue
                        
                        full_url = urljoin(self.base_url, href)
                        
                        # Filter out unwanted file types
                        if not any(ext in full_url.lower() for ext in ['.xlsx', '.csv', '.mp3', '.mp4', '.wav']):
                            # Only include AHPRA links or relevant external links
                            if (self.base_url in full_url or href.startswith('/') or 
                                any(domain in full_url for domain in ['gov.au', 'health.gov.au'])):
                                related_links.append(full_url)
            
            # Remove duplicates
            related_links = list(set(related_links))
            
            # Extract PDF content
            pdf_content = []
            pdf_links = []
            
            if main_elem:
                for link in main_elem.find_all('a', href=True):
                    href = link.get('href')
                    if href and href.lower().endswith('.pdf'):
                        pdf_url = urljoin(self.base_url, href)
                        if pdf_url not in pdf_links:  # Avoid duplicates
                            pdf_links.append(pdf_url)
                            pdf_text = self.download_pdf(pdf_url)
                            if pdf_text:
                                pdf_content.append({
                                    'url': pdf_url,
                                    'text': pdf_text
                                })
            
            # Extract image if available
            image_url = ""
            if main_elem:
                # Look for news-specific images first
                news_img = main_elem.find('div', id='news-image-left')
                if news_img:
                    img_elem = news_img.find('img')
                    if img_elem and img_elem.get('src'):
                        image_url = urljoin(self.base_url, img_elem['src'])
                
                # If no news image found, look for other content images
                if not image_url:
                    for img_elem in main_elem.find_all('img'):
                        if img_elem.get('src'):
                            src = img_elem.get('src')
                            # Skip logo images
                            if 'logo' not in src.lower():
                                image_url = urljoin(self.base_url, src)
                                break
            
            self.logger.info(f"Extracted {len(content_text)} characters of content from {len(content_parts)} parts")
            
            # DEBUG: Log content preview
            if self.test_mode:
                self.logger.info(f"Content preview: {content_text[:200]}...")
                self.logger.info(f"Found {len(content_parts)} content parts")
                for i, part in enumerate(content_parts[:3]):
                    self.logger.info(f"Part {i+1}: {part[:100]}...")
            
            return {
                'title': title,
                'url': article_url,
                'date': date_str,
                'content': content_text,
                'related_links': related_links,
                'pdf_content': pdf_content,
                'image_url': image_url,
                'scraped_date': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting content from {article_url}: {e}")
            return {}

    def is_new_article(self, article_data: Dict) -> bool:
        """Check if article is new (not already processed)"""
        identifier = f"{article_data.get('url', '')}_{article_data.get('title', '')}"
        return identifier not in self.existing_articles

    def save_data(self, articles: List[Dict]):
        """Save scraped data to JSON and CSV files"""
        try:
            # Load existing data
            existing_data = []
            if self.json_file.exists():
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            
            # Merge new articles
            all_articles = existing_data + articles
            
            # Save JSON
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, ensure_ascii=False, indent=2)
            
            # Save CSV
            import csv
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                if all_articles:
                    writer = csv.DictWriter(f, fieldnames=all_articles[0].keys())
                    writer.writeheader()
                    for article in all_articles:
                        # Convert lists and dicts to strings for CSV
                        row = {}
                        for key, value in article.items():
                            if isinstance(value, (list, dict)):
                                row[key] = json.dumps(value, ensure_ascii=False)
                            else:
                                row[key] = value
                        writer.writerow(row)
            
            self.logger.info(f"Saved {len(all_articles)} total articles ({len(articles)} new)")
            
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")

    def run(self):
        """Main scraping function"""
        try:
            self.logger.info("Starting AHPRA news scraping...")
            
            # Establish session
            response = self.establish_session()
            if not response:
                return
            
            # Parse initial page
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Expand all years using Selenium
            soup = self.expand_all_years(soup)
            
            # Extract article links
            article_links = self.extract_article_links(soup)
            
            # Apply processing logic based on run type
            if self.test_mode:
                # Test mode: process only a few articles
                articles_to_process = article_links[:self.TEST_ARTICLES_COUNT]
                self.logger.info(f"TEST MODE: Processing {len(articles_to_process)} articles for testing (from {len(article_links)} total)")
            elif self.is_first_run:
                # First run: process all articles
                articles_to_process = article_links
                self.logger.info(f"First run: Processing all {len(articles_to_process)} articles")
            else:
                # Subsequent runs: only check recent articles for new ones
                articles_to_process = article_links[:self.max_recent_articles]
                self.logger.info(f"Subsequent run: Checking {len(articles_to_process)} most recent articles (from {len(article_links)} total)")
            
            # Process each article
            new_articles = []
            processed_count = 0
            skipped_count = 0
            
            for i, article_info in enumerate(articles_to_process):
                self.logger.info(f"Processing article {i+1}/{len(articles_to_process)}: {article_info['title']}")
                
                # Extract full content
                article_data = self.extract_article_content(article_info['url'])
                
                if article_data and self.is_new_article(article_data):
                    new_articles.append(article_data)
                    processed_count += 1
                    
                    # Add to existing articles set
                    identifier = f"{article_data.get('url', '')}_{article_data.get('title', '')}"
                    self.existing_articles.add(identifier)
                else:
                    skipped_count += 1
                    self.logger.info(f"Skipped article (already exists or failed): {article_info['title']}")
                
                # Rate limiting
                time.sleep(2)
            
            # Save data
            if new_articles:
                self.save_data(new_articles)
                self.logger.info(f"Successfully added {len(new_articles)} new articles")
            else:
                self.logger.info("No new articles found")
            
            # Test mode summary
            if self.test_mode:
                self.logger.info("=" * 60)
                self.logger.info("TEST MODE COMPLETED SUCCESSFULLY!")
                self.logger.info("=" * 60)
                self.logger.info(f"Processed {processed_count} articles")
                self.logger.info(f"Skipped {skipped_count} articles")
                self.logger.info(f"Found {len(new_articles)} new articles")
                self.logger.info("To run full scrape, set TEST_MODE = False in the class")
                self.logger.info("=" * 60)
            
            self.logger.info("Scraping completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in main scraping function: {e}")
        
        finally:
            # Cleanup
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    # TEST MODE CONFIGURATION
    # Set TEST_MODE = True at the top of the class to test with only 3 articles
    # Set TEST_MODE = False to run full scrape
    
    # Option 1: Use class defaults (recommended)
    scraper = AHPRANewsScraper()
    
    # Option 2: Override test mode for this run
    # scraper = AHPRANewsScraper(test_mode=True)   # Force test mode
    # scraper = AHPRANewsScraper(test_mode=False)  # Force full mode
    
    # Option 3: Override recent articles limit
    # scraper = AHPRANewsScraper(max_recent_articles=5)   # Check 5 recent articles
    # scraper = AHPRANewsScraper(max_recent_articles=20)  # Check 20 recent articles
    
    scraper.run()