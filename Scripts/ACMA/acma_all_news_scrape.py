#!/usr/bin/env python3
"""
ACMA News Articles Scraper
Scrapes news articles from ACMA website with anti-bot measures, PDF extraction, and deduplication.
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import time
import random
import logging
from urllib.parse import urljoin, urlparse
from datetime import datetime
import re
from typing import Dict, List, Optional, Set
import hashlib
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import PyPDF2
import io
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
# import undetected_chromedriver as uc  # Commented out due to Python 3.12 compatibility

class ACMANewsScraper:
    def __init__(self):
        self.base_url = "https://www.acma.gov.au"
        self.news_url = "https://www.acma.gov.au/news-articles"
        self.session = requests.Session()
        self.ua = UserAgent()
        self.scraped_articles = set()
        self.existing_articles = {}
        self.data_folder = "data"
        self.json_file = os.path.join(self.data_folder, "acma_news.json")
        self.csv_file = os.path.join(self.data_folder, "acma_news.csv")
        self.log_file = os.path.join(self.data_folder, "scraper.log")
        
        # Configuration for pagination limits
        self.MAX_PAGES_DAILY = 3
        self.MAX_PAGES_FULL = 50  # Reasonable limit for full scrape
        
        # Create data folder if it doesn't exist
        os.makedirs(self.data_folder, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup session with retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Load existing articles for deduplication
        self.load_existing_articles()
        
        # Setup Chrome driver for JavaScript-heavy pages
        self.setup_driver()
    
    def setup_driver(self):
        """Setup Chrome driver for stealth scraping"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-javascript")  # We don't need JS for basic scraping
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument(f"--user-agent={self.ua.random}")
            
            # Try to use regular webdriver first
            try:
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager
                
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except ImportError:
                # Fallback to system Chrome
                self.driver = webdriver.Chrome(options=chrome_options)
            
            # Execute anti-detection script
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": self.ua.random
            })
            
        except Exception as e:
            self.logger.warning(f"Failed to setup Chrome driver: {e}. Will use requests only.")
            self.driver = None
    
    def get_stealth_headers(self) -> Dict[str, str]:
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
    
    def establish_session(self):
        """Establish session by visiting main page first"""
        try:
            self.logger.info("Establishing session...")
            headers = self.get_stealth_headers()
            
            # Visit main page first to get cookies
            response = self.session.get(self.base_url, headers=headers, timeout=30)
            
            if response.status_code == 403:
                self.logger.warning("Got 403 on main page, trying alternative approach...")
                # Try different headers
                headers.update({
                    'Accept': '*/*',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Origin': self.base_url
                })
                response = self.session.get(self.base_url, headers=headers, timeout=30)
            
            response.raise_for_status()
            self.logger.info(f"Main page response: {response.status_code}")
            
            # Random delay
            time.sleep(random.uniform(3, 6))
            
            # Visit about page or similar to look more natural
            about_url = f"{self.base_url}/about"
            self.session.get(about_url, headers=headers, timeout=30)
            time.sleep(random.uniform(2, 4))
            
            # Now visit news page
            response = self.session.get(self.news_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            self.logger.info("Session established successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to establish session: {e}")
            return False
    
    def load_existing_articles(self):
        """Load existing articles for deduplication"""
        try:
            if os.path.exists(self.json_file):
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for article in data:
                        # Create unique identifier using URL and title
                        article_id = self.generate_article_id(article.get('url', ''), article.get('title', ''))
                        self.existing_articles[article_id] = article
                        self.scraped_articles.add(article_id)
                self.logger.info(f"Loaded {len(self.existing_articles)} existing articles")
            else:
                self.logger.info("No existing articles found")
        except Exception as e:
            self.logger.error(f"Error loading existing articles: {e}")
    
    def generate_article_id(self, url: str, title: str) -> str:
        """Generate unique identifier for article"""
        return hashlib.md5(f"{url}_{title}".encode()).hexdigest()
    
    def get_page_content(self, url: str, use_selenium: bool = False) -> Optional[BeautifulSoup]:
        """Get page content with anti-bot measures"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if use_selenium and self.driver:
                    self.driver.get(url)
                    time.sleep(random.uniform(3, 6))
                    html = self.driver.page_source
                    return BeautifulSoup(html, 'html.parser')
                else:
                    headers = self.get_stealth_headers()
                    # Add referrer header for better stealth
                    if attempt > 0:
                        headers['Referer'] = self.base_url
                    
                    response = self.session.get(url, headers=headers, timeout=30)
                    
                    if response.status_code == 403:
                        self.logger.warning(f"403 error for {url}, attempt {attempt + 1}")
                        if attempt < max_retries - 1:
                            time.sleep(random.uniform(5, 10))  # Longer delay for 403
                            continue
                    
                    response.raise_for_status()
                    return BeautifulSoup(response.content, 'html.parser')
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request error for {url}, attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(3, 7))
                    continue
            except Exception as e:
                self.logger.error(f"Unexpected error getting page content for {url}: {e}")
                break
        
        return None
    
    def extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF"""
        try:
            headers = self.get_stealth_headers()
            response = self.session.get(pdf_url, headers=headers, timeout=60)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            
            # Clean text
            text = re.sub(r'\s+', ' ', text)  # Remove extra whitespace
            text = re.sub(r'[^\w\s.,;:!?()-]', '', text)  # Remove unwanted characters
            text = text.strip()
            
            self.logger.info(f"Extracted {len(text)} characters from PDF: {pdf_url}")
            return text
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF text from {pdf_url}: {e}")
            return ""
    
    def extract_table_data(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract table data from HTML"""
        tables = []
        for table in soup.find_all('table'):
            table_data = []
            headers = []
            
            # Extract headers
            header_row = table.find('tr')
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
            
            # Extract data rows
            for row in table.find_all('tr')[1:]:  # Skip header row
                row_data = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                if row_data:
                    if headers:
                        table_data.append(dict(zip(headers, row_data)))
                    else:
                        table_data.append(row_data)
            
            if table_data:
                tables.append(table_data)
        
        return tables
    
    def get_all_news_articles(self, max_pages_override: int = None) -> List[Dict]:
        """Get news articles with intelligent pagination for daily runs"""
        all_articles = []
        page = 0
        consecutive_existing_articles = 0
        max_consecutive_existing = 10  # Stop if we see 10 consecutive existing articles
        
        # Determine if this is likely a daily run and set page limits
        is_daily_run = len(self.existing_articles) > 0
        if max_pages_override:
            max_pages = max_pages_override
        elif is_daily_run:
            max_pages = self.MAX_PAGES_DAILY
        else:
            max_pages = self.MAX_PAGES_FULL
        
        self.logger.info(f"Scraping mode: {'Daily' if is_daily_run else 'Full'}, Max pages: {max_pages}")
        
        while True:
            self.logger.info(f"Scraping page {page + 1}")
            
            # Check page limits
            if page >= max_pages:
                self.logger.info(f"Reached maximum pages ({max_pages})")
                break
            
            # Construct URL for pagination
            if page == 0:
                url = self.news_url
            else:
                url = f"{self.news_url}?page={page}"
            
            soup = self.get_page_content(url)
            if not soup:
                break
            
            # Find articles on current page
            articles = soup.find_all('article', class_='card card-type-wide')
            
            if not articles:
                self.logger.info(f"No articles found on page {page + 1}")
                break
            
            new_articles_on_page = 0
            
            for article in articles:
                try:
                    # Extract basic info
                    title_elem = article.find('h3', class_='card-title')
                    date_elem = article.find('time')
                    link_elem = article.find('a', class_='card-link')
                    body_elem = article.find('p', class_='card-body')
                    img_elem = article.find('img')
                    
                    if not title_elem or not link_elem:
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    article_url = urljoin(self.base_url, link_elem.get('href'))
                    
                    # Check if already scraped
                    article_id = self.generate_article_id(article_url, title)
                    if article_id in self.scraped_articles:
                        self.logger.debug(f"Article already scraped: {title}")
                        consecutive_existing_articles += 1
                        
                        # For daily runs, if we see many consecutive existing articles, 
                        # it's likely we've reached old content
                        if is_daily_run and consecutive_existing_articles >= max_consecutive_existing:
                            self.logger.info(f"Found {max_consecutive_existing} consecutive existing articles, stopping daily run")
                            return all_articles
                        continue
                    
                    # Reset counter when we find new article
                    consecutive_existing_articles = 0
                    new_articles_on_page += 1
                    
                    # Extract other basic info
                    published_date = date_elem.get('datetime') if date_elem else ""
                    summary = body_elem.get_text(strip=True) if body_elem else ""
                    image_url = img_elem.get('src') if img_elem else ""
                    if image_url:
                        image_url = urljoin(self.base_url, image_url)
                    
                    # Get full article content
                    article_data = self.scrape_article_content(article_url, title, published_date, summary, image_url)
                    if article_data:
                        all_articles.append(article_data)
                        self.scraped_articles.add(article_id)
                    
                    # Random delay between articles
                    time.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    self.logger.error(f"Error processing article: {e}")
                    continue
            
            self.logger.info(f"Page {page + 1}: Found {new_articles_on_page} new articles")
            
            # For daily runs, if no new articles on this page, consider stopping
            if is_daily_run and new_articles_on_page == 0:
                self.logger.info("No new articles on this page for daily run, stopping")
                break
            
            # Check for next page
            pagination = soup.find('nav', class_='pager')
            if pagination:
                next_page = pagination.find('li', class_='pager__item pager__item--next')
                if next_page:
                    page += 1
                    time.sleep(random.uniform(2, 5))  # Delay between pages
                else:
                    break
            else:
                break
        
        return all_articles
    
    def scrape_article_content(self, url: str, title: str, published_date: str, summary: str, image_url: str) -> Optional[Dict]:
        """Scrape full content of a single article"""
        try:
            self.logger.info(f"Scraping article: {title}")
            
            soup = self.get_page_content(url)
            if not soup:
                return None
            
            # Extract main content - use the working selector from test results
            content = ""
            content_found_with = "none"
            
            # Primary approach: Use the article element that works (from test results)
            article_elem = soup.select_one('article[data-history-node-id]')
            if article_elem:
                self.logger.info("Found article element, extracting content...")
                
                # Clone to avoid modifying original
                article_clone = article_elem.__copy__()
                
                # Remove metadata and navigation elements first
                for unwanted_class in ['article__meta', 'field--name-field-hero', 'page-title', 'breadcrumb']:
                    for elem in article_clone.select(f'.{unwanted_class}'):
                        elem.decompose()
                
                # Remove structural elements
                for unwanted in article_clone(["script", "style", "nav", "footer", "header"]):
                    unwanted.decompose()
                
                # Get all text content
                full_text = article_clone.get_text(separator=' ', strip=True)
                
                # Filter out the notice and extract clean content
                sentences = full_text.split('. ')
                clean_sentences = []
                
                for sentence in sentences:
                    sentence = sentence.strip()
                    
                    # Skip unwanted patterns
                    skip_patterns = [
                        "Links to Communications Alliance publications are temporarily down",
                        "while they move to a new website",
                        "search for this information at",
                        "Industry Publications - Communications Alliance",
                        "27 June 2025",  # Remove date metadata if it appears alone
                        "News articles",  # Navigation text
                    ]
                    
                    # Check if sentence should be skipped
                    should_skip = False
                    for pattern in skip_patterns:
                        if pattern.lower() in sentence.lower():
                            should_skip = True
                            break
                    
                    # Skip very short sentences that are likely metadata
                    if len(sentence) < 15:
                        should_skip = True
                    
                    if not should_skip:
                        clean_sentences.append(sentence)
                
                if clean_sentences:
                    content = '. '.join(clean_sentences)
                    # Ensure it ends with a period
                    if not content.endswith('.'):
                        content += '.'
                    content_found_with = "article element (filtered)"
                    self.logger.info(f"Extracted clean content: {len(content)} chars from {len(clean_sentences)} sentences")
            
            # Fallback 1: Try to find field-html but with better filtering
            if len(content) < 200:
                self.logger.info("Trying field-html fallback with filtering")
                
                html_field = soup.select_one('div.field--name-field-html')
                if html_field:
                    # Look for paragraph elements specifically
                    paragraphs = html_field.find_all('p')
                    if paragraphs:
                        clean_paragraphs = []
                        for p in paragraphs:
                            p_text = p.get_text(strip=True)
                            
                            # Skip the notice paragraphs
                            if "Links to Communications Alliance publications" in p_text:
                                continue
                            if "temporarily down while they move" in p_text:
                                continue
                            if len(p_text) < 20:  # Skip very short paragraphs
                                continue
                            
                            clean_paragraphs.append(p_text)
                        
                        if clean_paragraphs:
                            content = ' '.join(clean_paragraphs)
                            content_found_with = "field-html paragraphs (filtered)"
                            self.logger.info(f"Extracted from paragraphs: {len(content)} chars")
            
            # Final fallback: Use the direct text approach but clean it
            if len(content) < 200:
                self.logger.warning("Using emergency text extraction")
                
                # Try to find any substantial text content
                all_text_elements = soup.find_all(['p', 'div'], string=True)
                content_parts = []
                
                for elem in all_text_elements:
                    text = elem.get_text(strip=True) if hasattr(elem, 'get_text') else str(elem).strip()
                    
                    # Skip the notice text and short texts
                    if len(text) > 50 and "Links to Communications Alliance" not in text:
                        content_parts.append(text)
                
                if content_parts:
                    content = ' '.join(content_parts[:10])  # Take first 10 substantial text parts
                    content_found_with = "emergency text extraction"
                    self.logger.info(f"Emergency extraction: {len(content)} chars")
            
            # Clean the final content
            if content:
                # Remove extra whitespace
                content = re.sub(r'\s+', ' ', content).strip()
                
                # Remove title if it appears at the start
                if content.startswith(title):
                    content = content[len(title):].strip()
                
                # Remove any remaining notice fragments
                content = re.sub(r'Links to Communications Alliance publications.*?Communications Alliance\.?', '', content, flags=re.IGNORECASE)
                content = re.sub(r'\s+', ' ', content).strip()
            
            # Log final results
            self.logger.info(f"Final content: {len(content)} chars using {content_found_with}")
            
            if len(content) < 200:
                self.logger.warning(f"Still short content: '{content[:200]}...'")
                return None  # Don't save articles with insufficient content
            
            # Extract theme/category
            theme = ""
            breadcrumb = soup.select_one('nav.breadcrumb')
            if breadcrumb:
                theme = breadcrumb.get_text(strip=True)
            
            # Extract related links from the content area only
            related_links = []
            content_area = soup.select_one('div.field--name-field-html') or article_elem
            if content_area:
                for link in content_area.select('a[href]'):
                    href = link.get('href')
                    link_text = link.get_text(strip=True)
                    if href and link_text and len(link_text) > 3 and not href.startswith('#'):
                        full_url = urljoin(self.base_url, href)
                        if full_url != url:
                            related_links.append({
                                'url': full_url,
                                'text': link_text
                            })
            
            # Filter related links
            filtered_links = []
            for link in related_links:
                url_lower = link['url'].lower()
                text_lower = link['text'].lower()
                
                # Skip unwanted file types and terms
                if any(ext in url_lower for ext in ['.xlsx', '.csv', '.xls', '.mp3', '.wav', '.mp4', '.avi']):
                    continue
                
                skip_terms = ['home', 'contact', 'about', 'search', 'menu', 'navigation', 'communications alliance']
                if any(term in text_lower for term in skip_terms):
                    continue
                
                # Include relevant links
                if ('acma.gov.au' in url_lower or 'gov.au' in url_lower or 
                    'donotcall.gov.au' in url_lower or 'scamwatch.gov.au' in url_lower):
                    filtered_links.append(link)
                    if len(filtered_links) >= 10:
                        break
            
            # Extract tables and PDFs
            tables = []
            pdf_content = ""
            pdf_links = []
            
            if content_area:
                tables = self.extract_table_data(content_area)
                
                for link in content_area.select('a[href$=".pdf"]'):
                    pdf_url = urljoin(self.base_url, link.get('href'))
                    pdf_links.append(pdf_url)
                    self.logger.info(f"Extracting PDF: {pdf_url}")
                    pdf_text = self.extract_pdf_text(pdf_url)
                    if pdf_text:
                        pdf_content += f"\n\n=== PDF: {link.get_text(strip=True) or 'Document'} ===\n{pdf_text}"
            
            # Combine all content
            full_content = content
            if pdf_content:
                full_content += pdf_content
            
            # Final cleaning
            full_content = re.sub(r'\s+', ' ', full_content).strip()
            
            article_data = {
                'title': title,
                'url': url,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'summary': summary,
                'content': full_content,
                'theme': theme,
                'image_url': image_url,
                'related_links': filtered_links,
                'pdf_links': pdf_links,
                'tables': tables,
                'content_length': len(full_content),
                'main_content_length': len(content),
                'pdf_content_length': len(pdf_content),
                'extraction_method': content_found_with
            }
            
            return article_data
            
        except Exception as e:
            self.logger.error(f"Error scraping article content for {url}: {e}")
            return None
    
    def save_results(self, articles: List[Dict]):
        """Save results to JSON and CSV files"""
        try:
            # Combine with existing articles
            all_articles = list(self.existing_articles.values()) + articles
            
            # Save JSON
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            
            # Save CSV
            import pandas as pd
            df = pd.DataFrame(all_articles)
            # Remove complex nested data for CSV
            csv_columns = ['title', 'url', 'published_date', 'scraped_date', 'summary', 'content', 'theme', 'image_url', 'content_length', 'main_content_length', 'pdf_content_length']
            # Only include columns that exist in the dataframe
            available_columns = [col for col in csv_columns if col in df.columns]
            df_csv = df[available_columns]
            df_csv.to_csv(self.csv_file, index=False, encoding='utf-8')
            
            self.logger.info(f"Saved {len(all_articles)} articles to {self.json_file} and {self.csv_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
    
    def test_content_extraction(self, test_urls: List[str] = None):
        """Test content extraction on specific URLs for debugging"""
        if not test_urls:
            test_urls = [
                "https://www.acma.gov.au/articles/2025-06/acma-decision-revised-commercial-television-industry-code-practice",
                # "https://www.acma.gov.au/articles/2022-01/research-reveals-australians-want-more-control-over-how-their-information-used"  # This URL gives 404
            ]
        
        self.logger.info("=== TESTING CONTENT EXTRACTION ===")
        
        for url in test_urls:
            self.logger.info(f"\nTesting URL: {url}")
            soup = self.get_page_content(url)
            if not soup:
                self.logger.error(f"Failed to get page content for {url}")
                continue
            
            # Test each selector
            selectors_to_test = [
                'div.field--name-field-html div.field__item',
                'div.field--name-field-html',
                'div.field--name-field-content',
                'article[data-history-node-id]'
            ]
            
            for selector in selectors_to_test:
                elements = soup.select(selector)
                if elements:
                    content = elements[0].get_text(separator=' ', strip=True)
                    self.logger.info(f"  {selector}: {len(content)} chars")
                    if len(content) > 100:
                        self.logger.info(f"    Preview: {content[:200]}...")
                else:
                    self.logger.info(f"  {selector}: NOT FOUND")
            
            # Test the actual extraction method
            self.logger.info(f"\n  Testing actual extraction method:")
            result = self.scrape_article_content(url, "Test Title", "2025-06-27", "Test summary", "")
            if result:
                self.logger.info(f"  ✅ SUCCESS: Extracted {result['content_length']} chars using {result['extraction_method']}")
                self.logger.info(f"  Content preview: {result['content'][:300]}...")
            else:
                self.logger.error(f"  ❌ FAILED: Could not extract content")
            
            print("-" * 80)
            
    def cleanup(self):
        """Cleanup resources"""
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()
    
    def run(self, force_full_scrape: bool = False):
        """Main scraping process"""
        try:
            if force_full_scrape:
                self.logger.info(f"Starting ACMA news scraper (FULL SCRAPE - Max {self.MAX_PAGES_FULL} pages)...")
            else:
                self.logger.info(f"Starting ACMA news scraper (INCREMENTAL - Max {self.MAX_PAGES_DAILY} pages)...")
            
            # Establish session
            if not self.establish_session():
                self.logger.error("Failed to establish session")
                return
            
            # Get all news articles
            if force_full_scrape:
                # For full scrape, clear existing data and scrape everything
                self.existing_articles = {}
                self.scraped_articles = set()
                new_articles = self.get_all_news_articles(max_pages_override=self.MAX_PAGES_FULL)
            else:
                new_articles = self.get_all_news_articles()  # Smart incremental scrape
            
            if new_articles:
                self.logger.info(f"Found {len(new_articles)} new articles")
                self.save_results(new_articles)
            else:
                self.logger.info("No new articles found")
            
            self.logger.info("Scraping completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in main scraping process: {e}")
        finally:
            self.cleanup()

if __name__ == "__main__":
    import sys
    
    # Install required packages
    required_packages = [
        'requests', 'beautifulsoup4', 'selenium', 'webdriver-manager',
        'PyPDF2', 'fake-useragent', 'pandas', 'lxml'
    ]
    
    print("Required packages:", ', '.join(required_packages))
    print("Install with: pip install " + ' '.join(required_packages))
    print("\nNote: Make sure you have Chrome browser installed on your system")
    print("Alternative: pip install setuptools if you want to try undetected-chromedriver")
    
    # Check for command line arguments
    force_full_scrape = "--full" in sys.argv or "--force-full" in sys.argv
    debug_mode = "--debug" in sys.argv
    test_mode = "--test" in sys.argv
    
    if debug_mode:
        print("\n🔍 DEBUG MODE ENABLED - Will save HTML files for inspection")
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run scraper
    scraper = ACMANewsScraper()
    
    if test_mode:
        print("\n🧪 RUNNING CONTENT EXTRACTION TEST")
        scraper.establish_session()
        scraper.test_content_extraction()
    elif force_full_scrape:
        print("\n🔄 Running FULL SCRAPE (will scrape all articles)")
        scraper.run(force_full_scrape=force_full_scrape)
    else:
        print("\n⚡ Running INCREMENTAL SCRAPE (only new articles)")
        print("   Use --full flag for complete scrape")
        print("   Use --debug flag for detailed debugging")
        print("   Use --test flag to test content extraction")
        scraper.run(force_full_scrape=force_full_scrape)