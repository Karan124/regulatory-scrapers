#!/usr/bin/env python3
"""
AEMO Guides Scraper
Comprehensive scraper for AEMO's guides with PDF and Excel extraction, anti-bot protection.
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

# Excel processing
try:
    import pandas as pd
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Excel processing libraries not available: {e}")
    print("Please install: pip install pandas openpyxl")
    EXCEL_AVAILABLE = False

# Configuration
MAX_PAGE = 2  # Set to None for initial run (scrape all), or set to 3 for daily runs
BASE_URL = "https://aemo.com.au"
GUIDES_URL = "https://aemo.com.au/library/guides"
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "aemo_guides.json"
PROCESSED_FILE = DATA_DIR / "processed_guides.json"
LOG_FILE = DATA_DIR / "aemo_guides_scraper.log"

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

# Suppress PDF and Excel processing warnings for cleaner logs
pdf_loggers = ['pdfplumber', 'PyPDF2', 'pdfminer', 'openpyxl', 'xlrd']
for pdf_logger_name in pdf_loggers:
    pdf_logger = logging.getLogger(pdf_logger_name)
    pdf_logger.setLevel(logging.ERROR)  # Only show errors, not warnings

class AEMOGuidesScraper:
    def __init__(self):
        self.session = requests.Session()
        self.driver = None
        self.processed_guides = self.load_processed_guides()
        self.scraped_guides = []
        self.downloaded_files = set()  # Stores file IDs
        
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

    def load_processed_guides(self) -> Set[str]:
        """Load previously processed guide identifiers"""
        if PROCESSED_FILE.exists():
            try:
                with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('processed_urls', []))
            except Exception as e:
                logger.warning(f"Could not load processed file: {e}")
        return set()

    def save_processed_guides(self):
        """Save processed guide identifiers"""
        try:
            data = {'processed_urls': list(self.processed_guides)}
            with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved {len(self.processed_guides)} processed guide IDs")
        except Exception as e:
            logger.error(f"Failed to save processed guides: {e}")

    def get_guide_identifier(self, url: str, title: str) -> str:
        """Generate unique identifier for guide"""
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

    def extract_guide_links(self, page_num: int) -> List[Dict[str, str]]:
        """Extract guide links from a guides listing page"""
        url = f"{GUIDES_URL}#e={(page_num-1)*10}"
        logger.info(f"Extracting guide links from page {page_num}: {url}")
        
        soup = self.get_page_content(url)
        if not soup:
            return []
        
        guides = []
        guide_items = soup.find_all('li')
        
        for item in guide_items:
            link = item.find('a', class_='search-result-list-item')
            if not link:
                continue
                
            href = link.get('href')
            if not href:
                continue
            
            # Check if it's a document (PDF or Excel)
            if not any(ext in href.lower() for ext in ['.pdf', '.xlsx', '.xls']):
                continue
                
            full_url = urljoin(BASE_URL, href)
            
            # Extract basic info
            title_elem = link.find('h3')
            date_elem = link.find('span', class_='is-date') or link.find('span', class_='field-publisheddate')
            abstract_elem = link.find('div', class_='field-abstract')
            size_elem = item.find('div', string=re.compile(r'Size'))
            filetype_elem = item.find('div', class_='field-extension')
            
            title = title_elem.get_text(strip=True) if title_elem else ""
            pub_date = date_elem.get_text(strip=True) if date_elem else ""
            abstract = abstract_elem.get_text(strip=True) if abstract_elem else ""
            file_size = size_elem.get_next_sibling().get_text(strip=True) if size_elem else ""
            file_type = filetype_elem.get_text(strip=True).replace('File type', '').strip() if filetype_elem else ""
            
            guide_id = self.get_guide_identifier(full_url, title)
            
            if guide_id in self.processed_guides:
                logger.info(f"Skipping already processed guide: {title}")
                continue
            
            guides.append({
                'url': full_url,
                'title': title,
                'published_date': pub_date,
                'abstract': abstract,
                'file_size': file_size,
                'file_type': file_type,
                'guide_id': guide_id
            })
        
        logger.info(f"Found {len(guides)} new guides on page {page_num}")
        return guides

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
            
            if pdf_id in self.downloaded_files:
                logger.info(f"PDF already processed: {pdf_url}")
                return ""
            
            logger.info(f"Downloading PDF: {pdf_url}")
            
            response = self.session.get(pdf_url, timeout=60, stream=True)
            response.raise_for_status()
            
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and not pdf_url.lower().endswith('.pdf'):
                logger.warning(f"URL doesn't seem to be a PDF: {pdf_url}")
                return ""
            
            pdf_content = response.content
            
            if not pdf_content:
                logger.error(f"PDF download failed or content is empty: {pdf_url}")
                return ""
            
            self.downloaded_files.add(pdf_id)
            logger.info(f"Successfully downloaded PDF: {pdf_url} ({len(pdf_content)} bytes)")
            
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

    def extract_excel_text(self, excel_url: str) -> Dict[str, str]:
        """Download and extract text from Excel file without saving"""
        if not EXCEL_AVAILABLE:
            logger.error("Excel processing libraries not available")
            return {}
        
        try:
            excel_id = hashlib.md5(excel_url.encode()).hexdigest()
            
            if excel_id in self.downloaded_files:
                logger.info(f"Excel file already processed: {excel_url}")
                return {}
            
            logger.info(f"Downloading Excel file: {excel_url}")
            
            response = self.session.get(excel_url, timeout=60, stream=True)
            response.raise_for_status()
            
            excel_content = response.content
            
            if not excel_content:
                logger.error(f"Excel download failed or content is empty: {excel_url}")
                return {}
            
            self.downloaded_files.add(excel_id)
            logger.info(f"Successfully downloaded Excel: {excel_url} ({len(excel_content)} bytes)")
            
            return self.read_excel_from_memory(excel_content, excel_url)
            
        except Exception as e:
            logger.error(f"Failed to extract Excel text from {excel_url}: {e}")
            return {}

    def read_excel_from_memory(self, excel_content: bytes, excel_url: str) -> Dict[str, str]:
        """Read text from Excel content in memory"""
        try:
            import io
            
            excel_file = io.BytesIO(excel_content)
            sheet_data = {}
            
            # Try to read all sheets
            try:
                # Get all sheet names first
                xl_file = pd.ExcelFile(excel_file)
                sheet_names = xl_file.sheet_names
                
                logger.info(f"Found {len(sheet_names)} sheets in Excel file: {sheet_names}")
                
                for sheet_name in sheet_names:
                    try:
                        # Read the sheet
                        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                        
                        # Convert all data to strings and extract text
                        sheet_text = []
                        
                        for index, row in df.iterrows():
                            row_text = []
                            for cell in row:
                                if pd.notna(cell):
                                    cell_str = str(cell).strip()
                                    if cell_str and cell_str != 'nan':
                                        row_text.append(cell_str)
                            
                            if row_text:
                                sheet_text.append(" | ".join(row_text))
                        
                        if sheet_text:
                            clean_sheet_text = self.clean_text("\n".join(sheet_text))
                            sheet_data[sheet_name] = clean_sheet_text
                            logger.info(f"Extracted {len(clean_sheet_text)} characters from sheet '{sheet_name}'")
                        
                    except Exception as sheet_error:
                        logger.warning(f"Error processing sheet '{sheet_name}': {sheet_error}")
                        continue
                        
            except Exception as excel_error:
                logger.error(f"Failed to process Excel file {excel_url}: {excel_error}")
            
            return sheet_data
            
        except Exception as e:
            logger.error(f"Failed to read Excel content from memory: {e}")
            return {}

    def extract_guide_content(self, guide_info: Dict[str, str]) -> Optional[Dict[str, any]]:
        """Extract content from guide file (PDF or Excel)"""
        url = guide_info['url']
        file_type = guide_info.get('file_type', '').lower()
        
        logger.info(f"Extracting content from guide: {guide_info['title']} ({file_type})")
        
        extracted_content = ""
        excel_sheets = {}
        
        if 'pdf' in file_type or url.lower().endswith('.pdf'):
            extracted_content = self.extract_pdf_text(url)
        elif 'xlsx' in file_type or 'xls' in file_type or url.lower().endswith(('.xlsx', '.xls')):
            excel_sheets = self.extract_excel_text(url)
            # Combine all sheets into one content for the main content field
            if excel_sheets:
                sheet_contents = []
                for sheet_name, sheet_content in excel_sheets.items():
                    sheet_contents.append(f"=== Sheet: {sheet_name} ===\n{sheet_content}")
                extracted_content = "\n\n".join(sheet_contents)
        else:
            logger.warning(f"Unknown file type for guide: {url}")
            return None
        
        if not extracted_content and not excel_sheets:
            logger.warning(f"No content extracted from guide: {url}")
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
        
        guide_data = {
            'url': url,
            'title': guide_info['title'],
            'published_date': guide_info['published_date'],
            'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'file_type': file_type,
            'file_size': guide_info.get('file_size', ''),
            'abstract': guide_info.get('abstract', ''),
            'content': extracted_content,
            'excel_sheets': excel_sheets if excel_sheets else None,
            'internal_links': internal_links,
            'guide_id': guide_info['guide_id']
        }
        
        logger.info(f"Successfully processed guide: {guide_info['title']}")
        return guide_data

    def get_total_pages(self) -> int:
        """Get total number of pages from pagination"""
        soup = self.get_page_content(GUIDES_URL)
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
        
        return 20  # Default based on your HTML example

    def scrape_all_guides(self):
        """Main scraping function"""
        logger.info("Starting AEMO guides scraping")
        
        try:
            self.setup_driver()
            
            total_pages = self.get_total_pages()
            max_pages_to_scrape = min(total_pages, MAX_PAGE) if MAX_PAGE else total_pages
            
            logger.info(f"Total pages: {total_pages}, Scraping: {max_pages_to_scrape}")
            
            all_guides = []
            
            # Extract guide links from all pages
            for page_num in range(1, max_pages_to_scrape + 1):
                try:
                    guides = self.extract_guide_links(page_num)
                    all_guides.extend(guides)
                    time.sleep(2)  # Rate limiting
                except Exception as e:
                    logger.error(f"Failed to extract from page {page_num}: {e}")
                    continue
            
            logger.info(f"Found {len(all_guides)} new guides to process")
            
            # Extract content from each guide
            for i, guide_info in enumerate(all_guides):
                try:
                    logger.info(f"Processing guide {i+1}/{len(all_guides)}: {guide_info['title']}")
                    
                    guide_data = self.extract_guide_content(guide_info)
                    if guide_data:
                        self.scraped_guides.append(guide_data)
                        self.processed_guides.add(guide_info['guide_id'])
                    
                    time.sleep(3)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Failed to process guide {guide_info['url']}: {e}")
                    continue
            
            # Save results
            self.save_results()
            self.save_processed_guides()
            
            logger.info(f"Scraping completed. Processed {len(self.scraped_guides)} guides")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()

    def save_results(self):
        """Save scraped guides to JSON file"""
        try:
            existing_data = []
            if OUTPUT_FILE.exists():
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            
            # Append new guides (deduplication already handled)
            existing_data.extend(self.scraped_guides)
            
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(self.scraped_guides)} new guides to {OUTPUT_FILE}")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

def main():
    """Main function"""
    scraper = AEMOGuidesScraper()
    
    try:
        scraper.scrape_all_guides()
        print(f"✅ Guides scraping completed successfully!")
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