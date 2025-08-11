#!/usr/bin/env python3
"""
AEMO Media Release Scraper
Comprehensive scraper for AEMO's media releases with pagination, PDF extraction, and anti-bot protection.
"""

import os
import json
import hashlib
import time
import logging
import requests
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import re
from pathlib import Path

# Import required packages
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Try to import undetected_chromedriver, fallback if not available
try:
    import undetected_chromedriver as uc
    UC_AVAILABLE = True
except ImportError as e:
    print(f"Warning: undetected_chromedriver not available: {e}")
    print("Falling back to regular Selenium Chrome driver")
    UC_AVAILABLE = False
    uc = None

import PyPDF2
import pdfplumber
from bs4 import BeautifulSoup

# Configuration
MAX_PAGE = 3  # Set to None for initial run (scrape all), or set to 3 for daily runs
BASE_URL = "https://aemo.com.au"
MEDIA_URL = "https://aemo.com.au/newsroom/media-release"
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "aemo_media_releases.json"
PROCESSED_FILE = DATA_DIR / "processed_media.json"
LOG_FILE = DATA_DIR / "aemo_media_scraper.log"

# Create directories
DATA_DIR.mkdir(exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress PDF processing warnings for cleaner logs
pdf_loggers = ['pdfplumber', 'PyPDF2', 'pdfminer']
for pdf_logger_name in pdf_loggers:
    pdf_logger = logging.getLogger(pdf_logger_name)
    pdf_logger.setLevel(logging.ERROR)  # Only show errors, not warnings

class AEMOMediaScraper:
    def __init__(self):
        self.session = requests.Session()
        self.driver = None
        self.processed_articles = self.load_processed_articles()
        self.scraped_articles = []
        self.downloaded_pdfs = set()  # Now stores PDF IDs instead of filenames
        
        # Anti-bot headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session.headers.update(self.headers)

    def setup_driver(self):
        """Setup Chrome driver with stealth options and Linux compatibility"""
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
        logger.info("Using system default Chrome binary (auto-detection)")
        
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
            
            logger.info(f"Chrome driver initialized successfully")
            if chromedriver_path:
                logger.info(f"Using ChromeDriver: {chromedriver_path}")
            else:
                logger.info("Using ChromeDriver from PATH")
            
            # Visit homepage first to establish session
            self.driver.get(BASE_URL)
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            
            # Detailed troubleshooting
            logger.error("Troubleshooting information:")
            
            # Check if chromedriver is accessible
            try:
                result = subprocess.run(['chromedriver', '--version'], capture_output=True, text=True, timeout=5)
                logger.info(f"ChromeDriver version: {result.stdout.strip()}")
            except Exception as cmd_e:
                logger.error(f"Cannot run chromedriver command: {cmd_e}")
            
            # Check Chrome version
            try:
                result = subprocess.run(['google-chrome', '--version'], capture_output=True, text=True, timeout=5)
                logger.info(f"Chrome version: {result.stdout.strip()}")
            except Exception as chrome_e:
                logger.error(f"Cannot run chrome command: {chrome_e}")
            
            raise

    def load_processed_articles(self) -> Set[str]:
        """Load previously processed article identifiers"""
        if PROCESSED_FILE.exists():
            try:
                with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('processed_urls', []))
            except Exception as e:
                logger.warning(f"Could not load processed file: {e}")
        return set()

    def save_processed_articles(self):
        """Save processed article identifiers"""
        try:
            data = {'processed_urls': list(self.processed_articles)}
            with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved {len(self.processed_articles)} processed article IDs")
        except Exception as e:
            logger.error(f"Failed to save processed articles: {e}")

    def get_article_identifier(self, url: str, title: str) -> str:
        """Generate unique identifier for article"""
        content = f"{url}|{title}".encode('utf-8')
        return hashlib.md5(content).hexdigest()

    def get_page_content(self, url: str, use_driver: bool = True) -> Optional[BeautifulSoup]:
        """Get page content with anti-bot protection"""
        try:
            if use_driver and self.driver:
                self.driver.get(url)
                time.sleep(2)
                content = self.driver.page_source
            else:
                response = self.session.get(url, timeout=30)
                if response.status_code == 403:
                    logger.warning(f"403 error for {url}, trying with driver")
                    return self.get_page_content(url, use_driver=True)
                response.raise_for_status()
                content = response.text
            
            return BeautifulSoup(content, 'html.parser')
            
        except Exception as e:
            logger.error(f"Failed to get content from {url}: {e}")
            return None

    def extract_article_links(self, page_num: int) -> List[Dict[str, str]]:
        """Extract article links from a media release listing page"""
        url = f"{MEDIA_URL}#e={(page_num-1)*10}"
        logger.info(f"Extracting links from page {page_num}: {url}")
        
        soup = self.get_page_content(url)
        if not soup:
            return []
        
        articles = []
        article_items = soup.find_all('li')
        
        for item in article_items:
            link = item.find('a', class_='search-result-list-item')
            if not link:
                continue
                
            href = link.get('href')
            if not href or not href.startswith('/newsroom/media-release/'):
                continue
                
            full_url = urljoin(BASE_URL, href)
            
            # Extract basic info
            title_elem = link.find('h3', class_='field-title')
            date_elem = link.find('div', class_='is-date')
            abstract_elem = link.find('div', class_='field-abstract')
            
            title = title_elem.get_text(strip=True) if title_elem else ""
            pub_date = date_elem.get_text(strip=True) if date_elem else ""
            abstract = abstract_elem.get_text(strip=True) if abstract_elem else ""
            
            article_id = self.get_article_identifier(full_url, title)
            
            if article_id in self.processed_articles:
                logger.info(f"Skipping already processed media release: {title}")
                continue
            
            articles.append({
                'url': full_url,
                'title': title,
                'published_date': pub_date,
                'abstract': abstract,
                'article_id': article_id
            })
        
        logger.info(f"Found {len(articles)} new media releases on page {page_num}")
        return articles

    def clean_text(self, text: str) -> str:
        """Clean text for LLM processing"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\-.,;:!?()\[\]{}"\'/]', ' ', text)
        # Remove multiple spaces
        text = re.sub(r' +', ' ', text)
        
        return text.strip()

    def extract_pdf_text(self, pdf_url: str) -> str:
        """Download and extract text from PDF without saving file"""
        try:
            # Create a simple identifier for deduplication based on URL
            pdf_id = hashlib.md5(pdf_url.encode()).hexdigest()
            
            # Skip if already processed this URL
            if pdf_id in self.downloaded_pdfs:
                logger.info(f"PDF already processed: {pdf_url}")
                return ""
            
            # Download PDF into memory
            logger.info(f"Downloading PDF: {pdf_url}")
            
            response = self.session.get(pdf_url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check if response is actually a PDF
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and not pdf_url.lower().endswith('.pdf'):
                logger.warning(f"URL doesn't seem to be a PDF: {pdf_url} (content-type: {content_type})")
                return ""
            
            # Read PDF content into memory
            pdf_content = response.content
            
            if not pdf_content:
                logger.error(f"PDF download failed or content is empty: {pdf_url}")
                return ""
            
            self.downloaded_pdfs.add(pdf_id)
            logger.info(f"Successfully downloaded PDF: {pdf_url} ({len(pdf_content)} bytes)")
            
            # Extract text directly from memory
            return self.read_pdf_from_memory(pdf_content, pdf_url)
            
        except Exception as e:
            logger.error(f"Failed to extract PDF text from {pdf_url}: {e}")
            return ""

    def read_pdf_from_memory(self, pdf_content: bytes, pdf_url: str) -> str:
        """Read text from PDF content in memory"""
        try:
            text_content = []
            
            # Try pdfplumber first (better for tables)
            try:
                import io
                pdf_file = io.BytesIO(pdf_content)
                
                with pdfplumber.open(pdf_file) as pdf:
                    for page_num, page in enumerate(pdf.pages):
                        try:
                            text = page.extract_text()
                            if text:
                                text_content.append(text)
                                
                            # Extract tables
                            tables = page.extract_tables()
                            for table in tables:
                                if table:
                                    table_text = []
                                    for row in table:
                                        if row:
                                            row_text = " | ".join([str(cell) if cell else "" for cell in row])
                                            table_text.append(row_text)
                                    if table_text:
                                        text_content.append("\n".join(table_text))
                        except Exception as page_error:
                            logger.warning(f"Error processing page {page_num} of {pdf_url}: {page_error}")
                            continue
                                
            except Exception as pdfplumber_error:
                logger.warning(f"pdfplumber failed for {pdf_url}: {pdfplumber_error}")
                
                # Fallback to PyPDF2
                try:
                    import io
                    pdf_file = io.BytesIO(pdf_content)
                    reader = PyPDF2.PdfReader(pdf_file)
                    
                    for page_num, page in enumerate(reader.pages):
                        try:
                            text = page.extract_text()
                            if text:
                                text_content.append(text)
                        except Exception as page_error:
                            logger.warning(f"Error processing page {page_num} with PyPDF2: {page_error}")
                            continue
                            
                except Exception as pypdf_error:
                    logger.error(f"PyPDF2 also failed for {pdf_url}: {pypdf_error}")
            
            full_text = "\n".join(text_content)
            extracted_text = self.clean_text(full_text)
            
            if extracted_text:
                logger.info(f"Successfully extracted {len(extracted_text)} characters from PDF: {pdf_url}")
            else:
                logger.warning(f"No text extracted from PDF: {pdf_url}")
                
            return extracted_text
            
        except Exception as e:
            logger.error(f"Failed to read PDF content from memory: {e}")
            return ""

    def extract_article_content(self, article_info: Dict[str, str]) -> Optional[Dict[str, any]]:
        """Extract full content from an article page"""
        url = article_info['url']
        logger.info(f"Extracting content from: {url}")
        
        soup = self.get_page_content(url)
        if not soup:
            return None
        
        # Find the main article content area - try multiple selectors
        content_div = None
        
        # Try to find the main content container
        main_content_selectors = [
            'main #content',  # Main content area
            'div.component.rich-text',  # Rich text component in main content
            'main div.component.rich-text',  # Rich text within main
            'div#content div.component.rich-text',  # Rich text within content div
            'main',  # Fallback to main tag
            'article'  # Fallback to article tag
        ]
        
        for selector in main_content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                logger.info(f"Found content using selector: {selector}")
                break
        
        if not content_div:
            logger.warning(f"No content found for {url}")
            return None
        
        # Extract main article text content (skip navigation, header, footer)
        content_text = []
        
        # Look specifically for paragraphs and list items in the main content
        text_elements = content_div.find_all(['p', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        
        for element in text_elements:
            # Skip if this element is within navigation, header, or footer
            if element.find_parent(['nav', 'header', 'footer']):
                continue
            
            # Skip if element has navigation-related classes
            element_classes = element.get('class', [])
            nav_classes = ['breadcrumb', 'navigation', 'menu', 'nav', 'header', 'footer']
            if any(nav_class in ' '.join(element_classes) for nav_class in nav_classes):
                continue
            
            text = element.get_text(strip=True)
            if text and len(text) > 15:  # Filter out very short text
                content_text.append(text)
        
        # Extract embedded links from main content only
        related_links = []
        pdf_links = []
        
        for link in content_div.find_all('a', href=True):
            # Skip links in navigation areas
            if link.find_parent(['nav', 'header', 'footer']):
                continue
            
            href = link.get('href')
            link_text = link.get_text(strip=True)
            
            if href and link_text:
                full_link = urljoin(BASE_URL, href)
                
                # Check if it's a PDF (check both href and full URL)
                is_pdf = (href.lower().endswith('.pdf') or 
                         '.pdf' in href.lower() or 
                         full_link.lower().endswith('.pdf'))
                
                if is_pdf:
                    pdf_links.append(link)
                    logger.info(f"Found PDF link: {full_link}")
                    # Also add PDF to related links
                    related_links.append({
                        'url': full_link,
                        'text': link_text,
                        'type': 'PDF'
                    })
                # Skip unwanted file types but include other relevant links
                elif not any(ext in href.lower() for ext in ['.xlsx', '.csv', '.mp3', '.mp4', '.wav']):
                    # Only include links that seem relevant (not just navigation)
                    link_classes = link.get('class', [])
                    if not any(nav_class in ' '.join(link_classes) for nav_class in ['nav', 'menu', 'breadcrumb']):
                        related_links.append({
                            'url': full_link,
                            'text': link_text,
                            'type': 'link'
                        })
        
        # Extract PDF content
        pdf_texts = []
        logger.info(f"Found {len(pdf_links)} PDF links to process")
        
        for pdf_link in pdf_links:
            href = pdf_link.get('href')
            pdf_url = urljoin(BASE_URL, href)
            logger.info(f"Processing PDF: {pdf_url}")
            pdf_text = self.extract_pdf_text(pdf_url)
            if pdf_text:
                pdf_texts.append(pdf_text)
                logger.info(f"Successfully extracted text from PDF: {pdf_url}")
            else:
                logger.warning(f"Failed to extract text from PDF: {pdf_url}")
        
        # Extract main article image (not navigation icons)
        image_url = None
        main_imgs = content_div.find_all('img')
        for img in main_imgs:
            # Skip small icons and navigation images
            src = img.get('src', '')
            alt = img.get('alt', '').lower()
            
            # Skip icons and small images
            if any(icon_term in src.lower() for icon_term in ['icon', 'logo', 'button']) or \
               any(icon_term in alt for icon_term in ['icon', 'logo', 'button']):
                continue
            
            # Skip images with small dimensions
            width = img.get('width')
            height = img.get('height')
            if width and height:
                try:
                    if int(width) < 100 or int(height) < 100:
                        continue
                except ValueError:
                    pass
            
            image_url = urljoin(BASE_URL, src)
            break
        
        # Extract theme/topic from tags
        theme = None
        tag_elem = soup.find('span', class_='tag-links')
        if tag_elem:
            theme_link = tag_elem.find('a')
            if theme_link:
                theme = theme_link.get_text(strip=True)
        
        # Clean and join content
        clean_content = self.clean_text('\n\n'.join(content_text))
        
        article_data = {
            'url': url,
            'title': article_info['title'],
            'published_date': article_info['published_date'],
            'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'theme': theme,
            'abstract': article_info.get('abstract', ''),
            'content': clean_content,
            'related_links': related_links,
            'image_url': image_url,
            'pdf_text': '\n\n--- PDF SEPARATOR ---\n\n'.join(pdf_texts) if pdf_texts else None,
            'article_id': article_info['article_id']
        }
        
        logger.info(f"Extracted {len(content_text)} paragraphs, {len(related_links)} links, {len(pdf_texts)} PDFs")
        
        return article_data

    def get_total_pages(self) -> int:
        """Get total number of pages from pagination"""
        soup = self.get_page_content(MEDIA_URL)
        if not soup:
            return 1
        
        page_selector = soup.find('ul', class_='page-selector-list')
        if not page_selector:
            return 1
        
        # Find last page number
        page_links = page_selector.find_all('a', {'data-itemnumber': True})
        if page_links:
            try:
                return max(int(link.get('data-itemnumber', 1)) for link in page_links)
            except (ValueError, TypeError):
                pass
        
        return 25  # Default based on typical pagination

    def scrape_all_articles(self):
        """Main scraping function"""
        logger.info("Starting AEMO media release scraping")
        
        try:
            self.setup_driver()
            
            total_pages = self.get_total_pages()
            max_pages_to_scrape = min(total_pages, MAX_PAGE) if MAX_PAGE else total_pages
            
            logger.info(f"Total pages: {total_pages}, Scraping: {max_pages_to_scrape}")
            
            all_articles = []
            
            # Extract article links from all pages
            for page_num in range(1, max_pages_to_scrape + 1):
                try:
                    articles = self.extract_article_links(page_num)
                    all_articles.extend(articles)
                    time.sleep(2)  # Rate limiting
                except Exception as e:
                    logger.error(f"Failed to extract from page {page_num}: {e}")
                    continue
            
            logger.info(f"Found {len(all_articles)} new media releases to process")
            
            # Extract content from each article
            for i, article_info in enumerate(all_articles):
                try:
                    logger.info(f"Processing media release {i+1}/{len(all_articles)}: {article_info['title']}")
                    
                    article_data = self.extract_article_content(article_info)
                    if article_data:
                        self.scraped_articles.append(article_data)
                        self.processed_articles.add(article_info['article_id'])
                    
                    time.sleep(3)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Failed to process media release {article_info['url']}: {e}")
                    continue
            
            # Save results
            self.save_results()
            self.save_processed_articles()
            
            logger.info(f"Scraping completed. Processed {len(self.scraped_articles)} media releases")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()

    def save_results(self):
        """Save scraped articles to JSON file"""
        try:
            existing_data = []
            if OUTPUT_FILE.exists():
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            
            # Append new articles (deduplication already handled)
            existing_data.extend(self.scraped_articles)
            
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(self.scraped_articles)} new media releases to {OUTPUT_FILE}")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

def main():
    """Main function"""
    scraper = AEMOMediaScraper()
    
    try:
        scraper.scrape_all_articles()
        print(f"✅ Media release scraping completed successfully!")
        print(f"📄 Results saved to: {OUTPUT_FILE}")
        print(f"📋 Log file: {LOG_FILE}")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        print("❌ Scraping interrupted")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        print(f"❌ Scraping failed: {e}")

if __name__ == "__main__":
    main()