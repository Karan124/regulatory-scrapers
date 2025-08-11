#!/usr/bin/env python3
"""
AEMO Publications Scraper
Comprehensive scraper for AEMO's major publications with PDF extraction and anti-bot protection.
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
MAX_PAGE = 1  # Set to None for initial run (scrape all), or set to 3 for daily runs
BASE_URL = "https://aemo.com.au"
PUBLICATIONS_URL = "https://aemo.com.au/library/major-publications"
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "aemo_publications.json"
PROCESSED_FILE = DATA_DIR / "processed_publications.json"
LOG_FILE = DATA_DIR / "aemo_publications_scraper.log"

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

class AEMOPublicationsScraper:
    def __init__(self):
        self.session = requests.Session()
        self.driver = None
        self.processed_publications = self.load_processed_publications()
        self.scraped_publications = []
        self.downloaded_pdfs = set()  # Stores PDF IDs
        
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
        
        # Updated user agent
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
            
            service_kwargs['service_args'] = [
                '--verbose',
                '--whitelisted-ips=',
                '--disable-dev-shm-usage'
            ]
            
            service = Service(**service_kwargs)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Chrome driver initialized successfully")
            
            # Visit homepage first to establish session
            self.driver.get(BASE_URL)
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise

    def load_processed_publications(self) -> Set[str]:
        """Load previously processed publication identifiers"""
        if PROCESSED_FILE.exists():
            try:
                with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('processed_urls', []))
            except Exception as e:
                logger.warning(f"Could not load processed file: {e}")
        return set()

    def save_processed_publications(self):
        """Save processed publication identifiers"""
        try:
            data = {'processed_urls': list(self.processed_publications)}
            with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved {len(self.processed_publications)} processed publication IDs")
        except Exception as e:
            logger.error(f"Failed to save processed publications: {e}")

    def get_publication_identifier(self, url: str, title: str) -> str:
        """Generate unique identifier for publication"""
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

    def extract_publication_links(self, page_num: int) -> List[Dict[str, str]]:
        """Extract publication links from a publications listing page"""
        url = f"{PUBLICATIONS_URL}#e={(page_num-1)*10}"
        logger.info(f"Extracting publication links from page {page_num}: {url}")
        
        soup = self.get_page_content(url)
        if not soup:
            return []
        
        publications = []
        publication_items = soup.find_all('li')
        
        for item in publication_items:
            link = item.find('a', class_='search-result-list-item')
            if not link:
                continue
                
            href = link.get('href')
            if not href:
                continue
            
            # Check if it's a PDF document
            if not href.lower().endswith('.pdf') and '.pdf' not in href.lower():
                continue
                
            full_url = urljoin(BASE_URL, href)
            
            # Extract basic info
            title_elem = link.find('h3')
            date_elem = link.find('span', class_='is-date') or link.find('span', class_='field-publisheddate')
            abstract_elem = link.find('div', class_='field-abstract')
            size_elem = item.find('div', string=re.compile(r'Size'))
            
            title = title_elem.get_text(strip=True) if title_elem else ""
            pub_date = date_elem.get_text(strip=True) if date_elem else ""
            abstract = abstract_elem.get_text(strip=True) if abstract_elem else ""
            file_size = ""
            
            # Extract file size if available
            if size_elem:
                size_parent = size_elem.find_parent()
                if size_parent:
                    size_text = size_parent.get_text(strip=True)
                    size_match = re.search(r'Size\s*([0-9.,]+\s*[KMGT]?B)', size_text)
                    if size_match:
                        file_size = size_match.group(1)
            
            publication_id = self.get_publication_identifier(full_url, title)
            
            if publication_id in self.processed_publications:
                logger.info(f"Skipping already processed publication: {title}")
                continue
            
            publications.append({
                'url': full_url,
                'title': title,
                'published_date': pub_date,
                'abstract': abstract,
                'file_size': file_size,
                'publication_id': publication_id
            })
        
        logger.info(f"Found {len(publications)} new publications on page {page_num}")
        return publications

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
            pdf_id = hashlib.md5(pdf_url.encode()).hexdigest()
            
            if pdf_id in self.downloaded_pdfs:
                logger.info(f"PDF already processed: {pdf_url}")
                return ""
            
            logger.info(f"Downloading PDF: {pdf_url}")
            
            response = self.session.get(pdf_url, timeout=120, stream=True)  # Longer timeout for large publications
            response.raise_for_status()
            
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and not pdf_url.lower().endswith('.pdf'):
                logger.warning(f"URL doesn't seem to be a PDF: {pdf_url}")
                return ""
            
            pdf_content = response.content
            
            if not pdf_content:
                logger.error(f"PDF download failed or content is empty: {pdf_url}")
                return ""
            
            self.downloaded_pdfs.add(pdf_id)
            logger.info(f"Successfully downloaded PDF: {pdf_url} ({len(pdf_content)} bytes)")
            
            return self.read_pdf_from_memory(pdf_content, pdf_url)
            
        except Exception as e:
            logger.error(f"Failed to extract PDF text from {pdf_url}: {e}")
            return ""

    def read_pdf_from_memory(self, pdf_content: bytes, pdf_url: str) -> str:
        """Read text from PDF content in memory with enhanced table extraction"""
        try:
            text_content = []
            
            # Try pdfplumber first (better for tables and complex layouts)
            try:
                import io
                pdf_file = io.BytesIO(pdf_content)
                
                with pdfplumber.open(pdf_file) as pdf:
                    logger.info(f"Processing PDF with {len(pdf.pages)} pages")
                    
                    for page_num, page in enumerate(pdf.pages):
                        try:
                            # Extract regular text
                            text = page.extract_text()
                            if text:
                                # Clean and add page text
                                clean_text = self.clean_text(text)
                                if clean_text:
                                    text_content.append(f"=== Page {page_num + 1} ===\n{clean_text}")
                            
                            # Extract tables with enhanced processing
                            tables = page.extract_tables()
                            for table_idx, table in enumerate(tables):
                                if table and len(table) > 0:
                                    table_text = []
                                    
                                    # Process table rows
                                    for row_idx, row in enumerate(table):
                                        if row:
                                            # Clean each cell and join with pipes
                                            clean_row = []
                                            for cell in row:
                                                if cell is not None:
                                                    cell_str = str(cell).strip()
                                                    if cell_str:
                                                        clean_row.append(cell_str)
                                                else:
                                                    clean_row.append("")
                                            
                                            if clean_row and any(clean_row):  # Only add non-empty rows
                                                table_text.append(" | ".join(clean_row))
                                    
                                    if table_text:
                                        table_header = f"=== Table {table_idx + 1} (Page {page_num + 1}) ==="
                                        text_content.append(f"{table_header}\n" + "\n".join(table_text))
                            
                            # Log progress for large documents
                            if (page_num + 1) % 10 == 0:
                                logger.info(f"Processed {page_num + 1}/{len(pdf.pages)} pages")
                                
                        except Exception as page_error:
                            logger.warning(f"Error processing page {page_num + 1} of {pdf_url}: {page_error}")
                            continue
                            
                    logger.info(f"Successfully processed all {len(pdf.pages)} pages with pdfplumber")
                                
            except Exception as pdfplumber_error:
                logger.warning(f"pdfplumber failed for {pdf_url}: {pdfplumber_error}")
                
                # Fallback to PyPDF2
                try:
                    import io
                    pdf_file = io.BytesIO(pdf_content)
                    reader = PyPDF2.PdfReader(pdf_file)
                    
                    logger.info(f"Falling back to PyPDF2 for {len(reader.pages)} pages")
                    
                    for page_num, page in enumerate(reader.pages):
                        try:
                            text = page.extract_text()
                            if text:
                                clean_text = self.clean_text(text)
                                if clean_text:
                                    text_content.append(f"=== Page {page_num + 1} ===\n{clean_text}")
                        except Exception as page_error:
                            logger.warning(f"Error processing page {page_num + 1} with PyPDF2: {page_error}")
                            continue
                            
                except Exception as pypdf_error:
                    logger.error(f"PyPDF2 also failed for {pdf_url}: {pypdf_error}")
            
            # Combine all content
            full_text = "\n\n".join(text_content)
            extracted_text = self.clean_text(full_text)
            
            if extracted_text:
                logger.info(f"Successfully extracted {len(extracted_text)} characters from PDF: {pdf_url}")
            else:
                logger.warning(f"No text extracted from PDF: {pdf_url}")
                
            return extracted_text
            
        except Exception as e:
            logger.error(f"Failed to read PDF content from memory: {e}")
            return ""

    def extract_publication_content(self, pub_info: Dict[str, str]) -> Optional[Dict[str, any]]:
        """Extract content from publication PDF"""
        url = pub_info['url']
        
        logger.info(f"Extracting content from publication: {pub_info['title']}")
        
        # Extract PDF content
        extracted_content = self.extract_pdf_text(url)
        
        if not extracted_content:
            logger.warning(f"No content extracted from publication: {url}")
            return None
        
        # Look for any URLs or links in the extracted content
        internal_links = []
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        found_urls = re.findall(url_pattern, extracted_content)
        
        for found_url in found_urls:
            if 'aemo.com.au' in found_url:  # Only include AEMO internal links
                internal_links.append({
                    'url': found_url,
                    'type': 'internal_link'
                })
        
        # Extract any reference numbers or document IDs from the content
        doc_refs = []
        ref_patterns = [
            r'([A-Z]{2,4}[-_]\d{4}[-_]\d{2,4})',  # Document reference patterns
            r'(ISP\s+\d{4})',  # ISP references
            r'(ESOO\s+\d{4})',  # ESOO references
            r'(RIS\s+\d{4})',  # RIS references
        ]
        
        for pattern in ref_patterns:
            matches = re.findall(pattern, extracted_content, re.IGNORECASE)
            doc_refs.extend(matches)
        
        publication_data = {
            'url': url,
            'title': pub_info['title'],
            'published_date': pub_info['published_date'],
            'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'file_size': pub_info.get('file_size', ''),
            'abstract': pub_info.get('abstract', ''),
            'content': extracted_content,
            'internal_links': internal_links,
            'document_references': list(set(doc_refs)) if doc_refs else [],  # Remove duplicates
            'publication_id': pub_info['publication_id']
        }
        
        logger.info(f"Successfully processed publication: {pub_info['title']}")
        return publication_data

    def get_total_pages(self) -> int:
        """Get total number of pages from pagination"""
        soup = self.get_page_content(PUBLICATIONS_URL)
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
        
        return 20  # Based on your note that there are 20 pages

    def scrape_all_publications(self):
        """Main scraping function"""
        logger.info("Starting AEMO publications scraping")
        
        try:
            self.setup_driver()
            
            total_pages = self.get_total_pages()
            max_pages_to_scrape = min(total_pages, MAX_PAGE) if MAX_PAGE else total_pages
            
            logger.info(f"Total pages: {total_pages}, Scraping: {max_pages_to_scrape}")
            
            all_publications = []
            
            # Extract publication links from all pages
            for page_num in range(1, max_pages_to_scrape + 1):
                try:
                    publications = self.extract_publication_links(page_num)
                    all_publications.extend(publications)
                    time.sleep(2)  # Rate limiting
                except Exception as e:
                    logger.error(f"Failed to extract from page {page_num}: {e}")
                    continue
            
            logger.info(f"Found {len(all_publications)} new publications to process")
            
            # Extract content from each publication
            for i, pub_info in enumerate(all_publications):
                try:
                    logger.info(f"Processing publication {i+1}/{len(all_publications)}: {pub_info['title']}")
                    
                    pub_data = self.extract_publication_content(pub_info)
                    if pub_data:
                        self.scraped_publications.append(pub_data)
                        self.processed_publications.add(pub_info['publication_id'])
                    
                    time.sleep(3)  # Rate limiting - longer for large PDFs
                    
                except Exception as e:
                    logger.error(f"Failed to process publication {pub_info['url']}: {e}")
                    continue
            
            # Save results
            self.save_results()
            self.save_processed_publications()
            
            logger.info(f"Scraping completed. Processed {len(self.scraped_publications)} publications")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()

    def save_results(self):
        """Save scraped publications to JSON file"""
        try:
            existing_data = []
            if OUTPUT_FILE.exists():
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            
            # Append new publications (deduplication already handled)
            existing_data.extend(self.scraped_publications)
            
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(self.scraped_publications)} new publications to {OUTPUT_FILE}")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

def main():
    """Main function"""
    scraper = AEMOPublicationsScraper()
    
    try:
        scraper.scrape_all_publications()
        print(f"✅ Publications scraping completed successfully!")
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