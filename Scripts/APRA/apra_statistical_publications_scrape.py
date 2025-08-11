#!/usr/bin/env python3
"""
APRA Statistical Publications Scraper
=====================================

A robust, production-ready scraper for APRA's statistical publications.
Designed to extract clean, comprehensive data for LLM analysis.

Requirements:
    pip install requests beautifulsoup4 PyPDF2 pandas openpyxl xlrd lxml selenium webdriver-manager selenium-stealth

Usage:
    python apra_scraper.py [--max-pages MAX_PAGES] [--full-scrape] [--debug]
"""

import os
import sys
import json
import hashlib
import logging
import time
import random
import argparse
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import re

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# PDF and Excel processing
try:
    import PyPDF2
    # Suppress PyPDF2 warnings about unknown widths and other formatting issues
    import PyPDF2.errors
    warnings.filterwarnings("ignore", category=PyPDF2.errors.PdfReadWarning)
    warnings.filterwarnings("ignore", message="unknown widths")
    warnings.filterwarnings("ignore", message="Xref table not zero-indexed")
    warnings.filterwarnings("ignore", message="Invalid destination")
    
    # Also suppress the specific logger that generates these warnings
    pdf_logger = logging.getLogger('PyPDF2')
    pdf_logger.setLevel(logging.ERROR)  # Only show errors, not warnings
    
except ImportError:
    PyPDF2 = None
    print("Warning: PyPDF2 not installed. PDF extraction will be skipped.")

try:
    import pandas as pd
    import openpyxl
    # Suppress pandas warnings about data types
    warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)
except ImportError:
    pd = None
    openpyxl = None
    print("Warning: pandas/openpyxl not installed. Excel extraction will be skipped.")

try:
    from selenium_stealth import stealth
except ImportError:
    stealth = None
    print("Warning: selenium-stealth not available, using basic anti-detection")

from io import BytesIO

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.apra.gov.au"
STATISTICS_URL = "https://www.apra.gov.au/statistics"
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "apra_statistical_publications.json"
LOG_FILE = DATA_DIR / "scraper.log"

# Default settings
DEFAULT_MAX_PAGES = 1
FULL_SCRAPE_PAGES = 50
REQUEST_DELAY = (2, 5)
MAX_RETRIES = 3
TIMEOUT = 30
PAGE_LOAD_TIMEOUT = 30
ARTICLE_TIMEOUT = 20

# Browser headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0"
}

class APRAScraper:
    def __init__(self, max_pages: int = DEFAULT_MAX_PAGES, debug: bool = False, headless: bool = True):
        self.max_pages = max_pages
        self.debug = debug
        self.headless = headless
        self.session = None
        self.driver = None
        self.scraped_urls: Set[str] = set()
        self.processed_hashes: Set[str] = set()
        self.publications: List[Dict] = []
        
        # Setup directories
        DATA_DIR.mkdir(exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Load existing data
        self._load_existing_data()
        
    def _setup_logging(self):
        """Configure comprehensive logging."""
        # Clear any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        log_level = logging.DEBUG if self.debug else logging.INFO
            
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== APRA Statistical Publications Scraper Started ===")
        if self.debug:
            self.logger.info("Debug mode enabled")
        
    def _load_existing_data(self):
        """Load existing publications to enable deduplication."""
        if OUTPUT_FILE.exists():
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    if isinstance(existing_data, list):
                        self.publications = existing_data
                    else:
                        self.publications = []
                        
                # Build deduplication sets
                for pub in self.publications:
                    if 'url' in pub:
                        self.scraped_urls.add(pub['url'])
                    if 'content_hash' in pub:
                        self.processed_hashes.add(pub['content_hash'])
                        
                self.logger.info(f"Loaded {len(self.publications)} existing publications")
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                self.publications = []
        else:
            self.publications = []
    
    def _setup_driver(self):
        """Setup Chrome driver with webdriver-manager for automatic driver management"""
        chrome_options = Options()
        
        # Basic options
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Anti-detection options
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        
        # Set user agent
        chrome_options.add_argument(f'user-agent={HEADERS["User-Agent"]}')
        
        # Experimental options to avoid detection
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            # Use webdriver-manager to automatically download and manage ChromeDriver
            self.logger.info("Setting up ChromeDriver with webdriver-manager...")
            service = Service(ChromeDriverManager().install())
            
            # Create driver
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Apply stealth settings if available
            if stealth:
                stealth(self.driver,
                        languages=["en-US", "en"],
                        vendor="Google Inc.",
                        platform="Win32",
                        webgl_vendor="Intel Inc.",
                        renderer="Intel Iris OpenGL Engine",
                        fix_hairline=True,
                )
            
            # Remove automation indicators
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
            
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            
            self.logger.info("Chrome driver setup completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup Chrome driver: {e}")
            return False
                
    def _setup_session(self):
        """Setup requests session with retry strategy and browser-like headers."""
        self.session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Browser-like headers
        self.session.headers.update(HEADERS)
        self.session.verify = False  # Disable SSL verification if needed
        
    def _random_delay(self):
        """Add random delay between requests."""
        delay = random.uniform(*REQUEST_DELAY)
        self.logger.debug(f"Waiting {delay:.2f} seconds")
        time.sleep(delay)
        
    def _generate_content_hash(self, content: str) -> str:
        """Generate hash for content deduplication."""
        return hashlib.md5(content.encode('utf-8')).hexdigest()
        
    def _simulate_human_browsing(self):
        """Simulate human-like browsing behavior"""
        try:
            self.logger.info("Starting human-like browsing simulation")
            
            # Go to homepage first
            self.driver.get(BASE_URL)
            time.sleep(2)
            
            # Scroll a bit
            self.driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(1)
            
            self.logger.info("Human-like browsing simulation completed")
            return True
            
        except Exception as e:
            self.logger.warning(f"Error in browsing simulation: {e}")
            return False
            
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        # Remove non-printable characters except newlines
        text = re.sub(r'[^\x20-\x7E\n\r\t]', '', text)
        return text
            
    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text content from PDF files with robust error handling and Brotli support."""
        if not PyPDF2:
            self.logger.warning("PyPDF2 not available, skipping PDF extraction")
            return None
            
        try:
            self.logger.info(f"Extracting PDF: {pdf_url}")
            
            # Create a fresh session for PDF download with proper headers
            pdf_session = requests.Session()
            pdf_session.headers.update({
                'User-Agent': HEADERS['User-Agent'],
                'Accept': 'application/pdf,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',  # Important: Accept Brotli compression
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            })
            
            # Download PDF with streaming and proper decompression
            response = pdf_session.get(pdf_url, timeout=TIMEOUT, stream=True)
            response.raise_for_status()
            
            # Debug: Log response headers to understand compression
            content_encoding = response.headers.get('content-encoding', 'none')
            content_type = response.headers.get('content-type', 'unknown')
            self.logger.debug(f"PDF response - Content-Type: {content_type}, Content-Encoding: {content_encoding}")
            
            # Validate content type
            if 'pdf' not in content_type.lower() and not pdf_url.lower().endswith('.pdf'):
                self.logger.warning(f"URL doesn't appear to be a PDF: {pdf_url} (Content-Type: {content_type})")
                return None
            
            # Check file size (skip very large files to avoid memory issues)
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > 50 * 1024 * 1024:  # 50MB limit
                self.logger.warning(f"PDF file too large ({content_length} bytes), skipping: {pdf_url}")
                return None
            
            # Read content with size limit - requests automatically handles decompression
            # when we access response.content, it automatically decompresses Brotli/gzip
            pdf_content_bytes = response.content
            
            # Validate PDF file signature (PDF files start with %PDF-)
            if not pdf_content_bytes.startswith(b'%PDF-'):
                self.logger.error(f"Downloaded content is not a valid PDF file: {pdf_url}")
                self.logger.debug(f"Content starts with: {pdf_content_bytes[:50]}")
                return None
            
            pdf_content = BytesIO(pdf_content_bytes)
            
            # Try multiple PyPDF2 strategies for robust extraction
            text_parts = []
            
            try:
                # Strategy 1: Standard PyPDF2 reader
                reader = PyPDF2.PdfReader(pdf_content)
                
                # Check if PDF is encrypted
                if reader.is_encrypted:
                    self.logger.info(f"PDF is encrypted, attempting to decrypt: {pdf_url}")
                    # Try to decrypt with empty password (common case)
                    try:
                        reader.decrypt("")
                    except Exception as decrypt_error:
                        self.logger.warning(f"Could not decrypt PDF {pdf_url}: {decrypt_error}")
                        return None
                
                # Extract text from all pages
                for page_num, page in enumerate(reader.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text and page_text.strip():
                            cleaned_page_text = self._clean_text(page_text)
                            if len(cleaned_page_text) > 10:  # Only add pages with meaningful content
                                text_parts.append(f"Page {page_num + 1}:\n{cleaned_page_text}")
                        
                    except Exception as page_error:
                        self.logger.debug(f"Error extracting page {page_num + 1} from {pdf_url}: {page_error}")
                        continue
                        
            except Exception as reader_error:
                self.logger.warning(f"PyPDF2 reader failed for {pdf_url}: {reader_error}")
                
                # Strategy 2: Try with strict=False for corrupted PDFs
                try:
                    pdf_content.seek(0)
                    reader = PyPDF2.PdfReader(pdf_content, strict=False)
                    
                    for page_num, page in enumerate(reader.pages):
                        try:
                            page_text = page.extract_text()
                            if page_text and page_text.strip():
                                cleaned_page_text = self._clean_text(page_text)
                                if len(cleaned_page_text) > 10:
                                    text_parts.append(f"Page {page_num + 1}:\n{cleaned_page_text}")
                        except Exception:
                            continue
                            
                except Exception as strict_error:
                    self.logger.warning(f"Even non-strict PyPDF2 failed for {pdf_url}: {strict_error}")
            
            # Process extracted text
            if text_parts:
                full_text = "\n\n".join(text_parts)
                
                # Additional text cleaning and validation
                if len(full_text.strip()) < 50:  # Very short content, probably not useful
                    self.logger.warning(f"PDF extracted text too short, might be image-based: {pdf_url}")
                    return None
                
                # Check for garbled text (common with corrupted PDFs)
                readable_chars = sum(1 for c in full_text if c.isprintable() or c.isspace())
                if len(full_text) > 0 and readable_chars / len(full_text) < 0.7:  # Less than 70% readable
                    self.logger.warning(f"PDF text appears garbled, skipping: {pdf_url}")
                    return None
                
                self.logger.info(f"Successfully extracted {len(full_text)} characters from PDF: {pdf_url}")
                return full_text
            else:
                self.logger.warning(f"No readable text extracted from PDF (might be image-based): {pdf_url}")
                return None
                
        except requests.RequestException as e:
            self.logger.error(f"Network error downloading PDF {pdf_url}: {e}")
            return None
        except MemoryError:
            self.logger.error(f"Memory error processing PDF {pdf_url} - file too large")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error extracting PDF {pdf_url}: {e}")
            return None
            
    def _extract_excel_data(self, excel_url: str) -> Optional[str]:
        """Extract data from Excel files in text format with robust error handling."""
        if not pd or not openpyxl:
            self.logger.warning("pandas/openpyxl not available, skipping Excel extraction")
            return None
            
        try:
            self.logger.info(f"Extracting Excel: {excel_url}")
            response = self.session.get(excel_url, timeout=TIMEOUT, stream=True)
            response.raise_for_status()
            
            # Validate content type
            content_type = response.headers.get('content-type', '').lower()
            file_extension = excel_url.lower().split('.')[-1]
            
            if not any(ext in content_type for ext in ['excel', 'spreadsheet']) and file_extension not in ['xlsx', 'xls']:
                self.logger.warning(f"URL doesn't appear to be an Excel file: {excel_url}")
                return None
            
            # Check file size (allow larger files for data analysis but warn)
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > 500 * 1024 * 1024:  # 500MB limit (increased)
                self.logger.warning(f"Excel file very large ({content_length} bytes), may take time to process: {excel_url}")
            
            # Download with increased size limit
            excel_content = BytesIO()
            downloaded_size = 0
            max_size = 500 * 1024 * 1024  # 500MB limit (increased from 100MB)
            
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    downloaded_size += len(chunk)
                    if downloaded_size > max_size:
                        self.logger.warning(f"Excel download size exceeded limit, truncating: {excel_url}")
                        break
                    excel_content.write(chunk)
            
            excel_content.seek(0)
            
            # Try multiple engines for robust extraction
            df_dict = None
            engines_to_try = []
            
            if file_extension == 'xlsx':
                engines_to_try = ['openpyxl', 'xlrd']
            elif file_extension == 'xls':
                engines_to_try = ['xlrd', 'openpyxl']
            else:
                engines_to_try = ['openpyxl', 'xlrd']
            
            for engine in engines_to_try:
                try:
                    self.logger.debug(f"Trying to read Excel with engine: {engine}")
                    df_dict = pd.read_excel(excel_content, sheet_name=None, engine=engine)
                    self.logger.debug(f"Successfully read Excel with engine: {engine}")
                    break
                except Exception as engine_error:
                    self.logger.debug(f"Engine {engine} failed: {engine_error}")
                    excel_content.seek(0)  # Reset for next attempt
                    continue
            
            if df_dict is None:
                self.logger.error(f"Could not read Excel file with any engine: {excel_url}")
                return None
                
            extracted_parts = []
            total_rows = 0
            
            for sheet_name, df in df_dict.items():
                if df.empty:
                    self.logger.debug(f"Sheet '{sheet_name}' is empty, skipping")
                    continue
                
                # Process all rows - no truncation for data analysis
                total_rows += len(df)
                
                # Log progress for large sheets
                if len(df) > 50000:
                    self.logger.info(f"Processing large sheet '{sheet_name}' with {len(df)} rows - this may take a while...")
                
                try:
                    sheet_text = f"Sheet: {sheet_name}\nRows: {len(df)}\nColumns: {len(df.columns)}\n\n"
                    
                    # For very large datasets, provide summary statistics first
                    if len(df) > 10000:
                        # Add column information
                        sheet_text += f"Columns: {', '.join(df.columns.astype(str))}\n\n"
                        
                        # Add data type information
                        dtypes_info = df.dtypes.to_string()
                        sheet_text += f"Data Types:\n{dtypes_info}\n\n"
                        
                        # Add summary statistics for numeric columns
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            summary_stats = df[numeric_cols].describe()
                            sheet_text += f"Summary Statistics (Numeric Columns):\n{summary_stats.to_string()}\n\n"
                        
                        # Add value counts for categorical columns (top 10 values)
                        categorical_cols = df.select_dtypes(include=['object']).columns
                        for col in categorical_cols[:5]:  # Limit to first 5 categorical columns
                            if col in df.columns:
                                try:
                                    value_counts = df[col].value_counts().head(10)
                                    sheet_text += f"Top values in '{col}':\n{value_counts.to_string()}\n\n"
                                except Exception:
                                    continue
                    
                    # Handle different data types and clean the DataFrame
                    # Convert all columns to string to avoid type issues, but preserve structure
                    df_str = df.astype(str)
                    
                    # Replace common pandas null representations
                    df_str = df_str.replace(['nan', 'NaN', 'None', '<NA>'], '')
                    
                    # For large datasets, use more efficient string conversion
                    if len(df) > 10000:
                        # Process in chunks to manage memory
                        chunk_size = 5000
                        data_chunks = []
                        
                        for start_idx in range(0, len(df_str), chunk_size):
                            end_idx = min(start_idx + chunk_size, len(df_str))
                            chunk = df_str.iloc[start_idx:end_idx]
                            
                            chunk_string = chunk.to_string(
                                index=False,
                                na_rep='',
                                max_cols=None,
                                max_rows=None,
                                max_colwidth=200  # Increased for better data preservation
                            )
                            
                            data_chunks.append(f"\nRows {start_idx + 1} to {end_idx}:\n{chunk_string}")
                            
                            # Progress logging for very large sheets
                            if len(df) > 50000 and start_idx % 25000 == 0:
                                self.logger.debug(f"Processed {end_idx}/{len(df)} rows of sheet '{sheet_name}'")
                        
                        df_string = "\n".join(data_chunks)
                    else:
                        # Standard conversion for smaller datasets
                        df_string = df_str.to_string(
                            index=False, 
                            na_rep='', 
                            max_cols=None, 
                            max_rows=None,
                            max_colwidth=200  # Increased for better data preservation
                        )
                    
                    # Clean up the string representation (but preserve more structure)
                    df_string = re.sub(r'\n\s*\n\s*\n', '\n\n', df_string)  # Remove triple+ newlines only
                    
                    sheet_text += "\nData:\n" + df_string
                    
                    if len(sheet_text.strip()) > 50:  # Only add sheets with meaningful content
                        extracted_parts.append(sheet_text)
                        self.logger.info(f"Successfully processed sheet '{sheet_name}' with {len(df)} rows and {len(df.columns)} columns")
                    else:
                        self.logger.debug(f"Sheet '{sheet_name}' has minimal content, skipping")
                        
                except Exception as sheet_error:
                    self.logger.warning(f"Error processing sheet '{sheet_name}': {sheet_error}")
                    # For critical data analysis, try a simpler approach if the main method fails
                    try:
                        self.logger.info(f"Attempting simplified extraction for sheet '{sheet_name}'")
                        simple_text = f"Sheet: {sheet_name}\n{df.to_csv(index=False)}"
                        extracted_parts.append(simple_text)
                        self.logger.info(f"Successfully extracted sheet '{sheet_name}' using simplified method")
                    except Exception as fallback_error:
                        self.logger.error(f"Even simplified extraction failed for sheet '{sheet_name}': {fallback_error}")
                    continue
                    
            if extracted_parts:
                full_text = "\n\n" + "="*80 + "\n\n".join(extracted_parts)
                cleaned_text = self._clean_text(full_text)
                
                # For data analysis, don't reject files based on length
                self.logger.info(f"Successfully extracted {len(cleaned_text)} characters from Excel ({total_rows} total rows across {len(extracted_parts)} sheets): {excel_url}")
                return cleaned_text
            else:
                self.logger.warning(f"No meaningful data extracted from Excel: {excel_url}")
                return None
                
        except requests.RequestException as e:
            self.logger.error(f"Network error downloading Excel {excel_url}: {e}")
            return None
        except MemoryError:
            self.logger.error(f"Memory error processing Excel {excel_url} - file too large even for data analysis")
            # Try one more time with minimal processing
            try:
                self.logger.info("Attempting memory-efficient extraction...")
                # Re-read with minimal processing
                excel_content.seek(0)
                df_dict = pd.read_excel(excel_content, sheet_name=None)
                simple_parts = []
                for sheet_name, df in df_dict.items():
                    if not df.empty:
                        # Just convert to CSV format which is more memory efficient
                        csv_data = df.to_csv(index=False)
                        simple_parts.append(f"Sheet: {sheet_name}\n{csv_data}")
                
                if simple_parts:
                    return "\n\n".join(simple_parts)
                
            except Exception:
                self.logger.error("Memory-efficient extraction also failed")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error extracting Excel {excel_url}: {e}")
            return None
    
    def _get_publication_links_selenium(self, page_num: int = 0) -> List[str]:
        """Extract publication links using Selenium for JavaScript-heavy pages."""
        try:
            if page_num == 0:
                url = STATISTICS_URL
            else:
                url = f"{STATISTICS_URL}?page={page_num}"
                
            self.logger.info(f"Getting links from: {url}")
            self._random_delay()
            
            # Load page with Selenium
            self.driver.get(url)
            WebDriverWait(self.driver, PAGE_LOAD_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Additional wait for dynamic content to load
            time.sleep(3)
            
            # Debug: save the HTML content
            if self.debug:
                debug_file = DATA_DIR / f"debug_selenium_page_{page_num}.html"
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)
                self.logger.info(f"Saved debug HTML to {debug_file}")
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            links = []
            
            # Try multiple potential selectors based on APRA patterns
            selectors_to_try = [
                'a.tile__link-cover',  # From the provided HTML sample
                'a[href*="statistics"]',  # Any link containing "statistics"  
                'a[href*="monthly-"]',  # Monthly publications
                'a[href*="quarterly-"]',  # Quarterly publications
                'a[href*="annual-"]',  # Annual publications
                'a[href*="fund-level"]',  # Fund level statistics
                'a[href*="deposit-taking"]',  # Deposit-taking institution stats
                'a[href*="superannuation"]',  # Superannuation stats
                'a[href*="insurance"]',  # Insurance stats
                'article a',  # Links within article elements
                '.tile a',  # Links within tile elements
                '.views-row a',  # Links within view rows
                'h4 a',  # Links within h4 headings
                'h3 a',  # Links within h3 headings
                'h2 a',  # Links within h2 headings
                '.publication-item a',  # Publication item links
                '.news-item a',  # News item links (may contain stats)
                '.content a',  # Content area links
                'main a',  # Main content links
                'a[class*="link"]',  # Any link with "link" in class name
            ]
            
            processed_urls = set()  # Track processed URLs to avoid duplicates
            
            for selector in selectors_to_try:
                potential_links = soup.select(selector)
                self.logger.debug(f"Selector '{selector}' found {len(potential_links)} elements")
                
                for link in potential_links:
                    href = link.get('href')
                    if not href:
                        continue
                        
                    # Skip unwanted links
                    if (href.startswith('#') or 
                        href.startswith('javascript:') or 
                        href.startswith('mailto:') or
                        'facebook.com' in href or
                        'twitter.com' in href or
                        'linkedin.com' in href or
                        '?page=' in href or
                        '?industry=' in href or
                        '?filter=' in href):
                        continue
                        
                    # Make URL absolute
                    if not href.startswith('http'):
                        href = urljoin(BASE_URL, href)
                    
                    # Avoid duplicates
                    if href in processed_urls:
                        continue
                    
                    # Only include statistical publication links
                    link_text = self._clean_text(link.get_text())
                    
                    # Check URL patterns and link text for statistical content
                    statistical_indicators = [
                        'statistics', 'monthly', 'quarterly', 'annual', 
                        'publication', 'deposit-taking', 'superannuation',
                        'insurance', 'fund-level', 'performance', 'data'
                    ]
                    
                    if (any(indicator in href.lower() for indicator in statistical_indicators) or
                        any(indicator in link_text.lower() for indicator in statistical_indicators)):
                        
                        # Additional filters to exclude navigation/general pages
                        exclude_patterns = [
                            '/statistics$',  # Main statistics page itself
                            '/statistics/$',
                            'statistics-and-reporting$',
                            'statistics-and-reporting/$'
                        ]
                        
                        if not any(re.search(pattern, href) for pattern in exclude_patterns):
                            links.append(href)
                            processed_urls.add(href)
                            self.logger.debug(f"Found potential link: {href}")
                            
            # Remove duplicates while preserving order
            unique_links = list(dict.fromkeys(links))  # Preserves order unlike set()
                    
            self.logger.info(f"Found {len(unique_links)} unique publication links on page {page_num + 1}")
            
            # Debug: log the first few links found
            if self.debug:
                for i, link in enumerate(unique_links[:10]):
                    self.logger.debug(f"Link {i+1}: {link}")
                
            return unique_links
            
        except Exception as e:
            self.logger.error(f"Error getting publication links from page {page_num}: {e}")
            return []
            
    def _extract_publication_details(self, pub_url: str) -> Optional[Dict]:
        """Extract detailed content from a publication page using Selenium."""
        if pub_url in self.scraped_urls:
            self.logger.info(f"Skipping already scraped URL: {pub_url}")
            return None
            
        try:
            self.logger.info(f"Extracting publication: {pub_url}")
            self._random_delay()
            
            # Load page with Selenium
            self.driver.get(pub_url)
            WebDriverWait(self.driver, ARTICLE_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Small delay to ensure full page load
            time.sleep(2)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Initialize publication data
            publication = {
                'url': pub_url,
                'scraped_date': datetime.now(timezone.utc).isoformat(),
                'headline': '',
                'theme': '',
                'published_date': '',
                'associated_image': None,
                'related_links': [],
                'main_content': '',
                'pdf_content': [],
                'excel_content': [],
                'content_hash': ''
            }
            
            # Extract headline
            headline_selectors = ['h1', 'main h1', 'article h1', '[class*="title"] h1']
            for selector in headline_selectors:
                h1_elem = soup.select_one(selector)
                if h1_elem:
                    headline = self._clean_text(h1_elem.get_text())
                    if len(headline) > 10:  # Ensure meaningful headline
                        publication['headline'] = headline
                        break
                
            # Extract published date
            time_elem = soup.select_one('time[datetime]')
            if time_elem:
                publication['published_date'] = time_elem.get('datetime', time_elem.get_text(strip=True))
            else:
                # Look for date patterns
                date_patterns = ['.date', '[class*="date"]', '.published', '[class*="published"]']
                for pattern in date_patterns:
                    date_elem = soup.select_one(pattern)
                    if date_elem:
                        date_text = self._clean_text(date_elem.get_text())
                        if any(month in date_text.lower() for month in ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']):
                            publication['published_date'] = date_text
                            break
                
            # Extract theme/category
            category_selectors = ['.field-field-category', '.category', '[class*="category"]', '.type', '[class*="type"]']
            for selector in category_selectors:
                category_elem = soup.select_one(selector)
                if category_elem:
                    publication['theme'] = self._clean_text(category_elem.get_text())
                    break
                
            # Extract main content with focus on rich-text areas (APRA pattern)
            main_content = ""
            
            # Try APRA-specific content areas first
            rich_text_area = soup.select_one('.rich-text')
            if rich_text_area:
                # Extract text preserving structure
                content_parts = []
                for elem in rich_text_area.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'blockquote']):
                    if elem.name == 'li':
                        li_text = self._clean_text(elem.get_text())
                        if li_text and len(li_text) > 5:
                            content_parts.append(f"• {li_text}")
                    elif elem.name in ['ul', 'ol']:
                        continue  # Skip list containers
                    else:
                        elem_text = self._clean_text(elem.get_text())
                        if elem_text and len(elem_text) > 10:
                            content_parts.append(elem_text)
                
                main_content = '\n\n'.join(content_parts)
            
            # Fallback content extraction
            if not main_content or len(main_content) < 100:
                content_selectors = ['[class*="content"] p', 'main p', 'article p', '[class*="body"] p']
                for selector in content_selectors:
                    content_elements = soup.select(selector)
                    if content_elements:
                        content_parts = [self._clean_text(elem.get_text()) for elem in content_elements]
                        content_parts = [part for part in content_parts if len(part) > 20]
                        if content_parts:
                            main_content = "\n\n".join(content_parts)
                            break
            
            publication['main_content'] = main_content
                
            # Find and process document links
            document_links = soup.find_all('a', class_='document-link')
            
            # If no document-link class, try generic file links
            if not document_links:
                # Look for PDF and Excel links
                file_links = soup.find_all('a', href=lambda x: x and (x.lower().endswith('.pdf') or x.lower().endswith('.xlsx') or x.lower().endswith('.xls')))
                document_links = file_links
            
            for link in document_links:
                href = link.get('href')
                if not href:
                    continue
                    
                # Make URL absolute
                if not href.startswith('http'):
                    href = urljoin(BASE_URL, href)
                    
                link_text = self._clean_text(link.get_text())
                
                # Try to determine file type
                file_type = ''
                if '.pdf' in href.lower():
                    file_type = 'PDF'
                elif '.xlsx' in href.lower():
                    file_type = 'XLSX'
                elif '.xls' in href.lower():
                    file_type = 'XLS'
                else:
                    # Try to find file type in link structure
                    file_type_elem = link.find('span', class_='document-link__type')
                    if file_type_elem:
                        file_type = file_type_elem.get_text(strip=True)
                
                # Add to related links
                publication['related_links'].append({
                    'url': href,
                    'text': link_text,
                    'type': file_type
                })
                
                # Process based on file type
                if file_type.upper() == 'PDF' and PyPDF2:
                    pdf_text = self._extract_pdf_text(href)
                    if pdf_text:
                        publication['pdf_content'].append({
                            'url': href,
                            'title': link_text,
                            'content': pdf_text
                        })
                        
                elif file_type.upper() in ['XLSX', 'XLS'] and pd and openpyxl:
                    excel_text = self._extract_excel_data(href)
                    if excel_text:
                        publication['excel_content'].append({
                            'url': href,
                            'title': link_text,
                            'content': excel_text
                        })
                        
            # Generate content hash for deduplication
            content_for_hash = f"{publication['headline']}{publication['main_content']}"
            publication['content_hash'] = self._generate_content_hash(content_for_hash)
            
            # Check for duplicates
            if publication['content_hash'] in self.processed_hashes:
                self.logger.info(f"Duplicate content detected, skipping: {pub_url}")
                return None
                
            # Mark as processed
            self.scraped_urls.add(pub_url)
            self.processed_hashes.add(publication['content_hash'])
            
            self.logger.info(f"Successfully extracted: {publication['headline'][:50]}...")
            return publication
            
        except Exception as e:
            self.logger.error(f"Error extracting publication {pub_url}: {e}")
            return None
            
    def _has_next_page_selenium(self, page_num: int) -> bool:
        """Check if there's a next page available using Selenium."""
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Look for various pagination indicators
            pagination_selectors = [
                'a[class*="next"]',  # Generic next buttons
                'a[rel="next"]',  # Standard rel=next
                'a.icon--arrow-right-black[rel="next"]',  # Specific from sample
                '.pagination a[href*="page="]',  # Pagination links
                'a[title*="next" i]',  # Title containing "next"
                'a[aria-label*="next" i]',  # Aria-label containing "next"
                '.pagination__next a'  # APRA-specific pagination
            ]
            
            for selector in pagination_selectors:
                next_elements = soup.select(selector)
                if next_elements:
                    self.logger.debug(f"Found next page indicator with selector: {selector}")
                    return True
                    
            # Also check for numbered pagination
            pagination_links = soup.select('a[href*="page="]')
            current_page_num = page_num + 1
            for link in pagination_links:
                href = link.get('href', '')
                if f'page={current_page_num}' in href:  # Next page exists
                    return True
                    
            self.logger.debug(f"No next page found for page {page_num + 1}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking next page: {e}")
            return False
            
    def scrape(self):
        """Main scraping method."""
        try:
            # Setup browser and session
            if not self._setup_driver():
                raise Exception("Failed to setup Chrome driver")
            
            self._setup_session()
            
            # Simulate human browsing
            if not self._simulate_human_browsing():
                self.logger.warning("Human browsing simulation failed, continuing anyway...")
            
            new_publications = []
            page = 0
            
            self.logger.info(f"Starting scrape with max_pages={self.max_pages}")
            
            while page < self.max_pages:
                self.logger.info(f"Scraping page {page + 1}/{self.max_pages}")
                
                # Get publication links from current page
                pub_links = self._get_publication_links_selenium(page)
                
                if not pub_links:
                    self.logger.info("No publications found on this page")
                    break
                    
                # Process each publication
                for pub_url in pub_links:
                    publication = self._extract_publication_details(pub_url)
                    if publication:
                        new_publications.append(publication)
                        
                # Check if there's a next page (only if we haven't reached max pages)
                if page + 1 < self.max_pages and not self._has_next_page_selenium(page):
                    self.logger.info("No more pages available")
                    break
                    
                page += 1
                
            # Add new publications to existing data
            if new_publications:
                self.publications.extend(new_publications)
                # Save results
                self._save_results()
            
            self.logger.info(f"Scraping completed. {len(new_publications)} new publications added.")
            self.logger.info(f"Total publications in dataset: {len(self.publications)}")
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            raise
        finally:
            self._cleanup()
            
    def _save_results(self):
        """Save results to JSON file."""
        try:
            # Sort by published date (newest first)
            self.publications.sort(key=lambda x: x.get('published_date', ''), reverse=True)
            
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.publications, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Results saved to {OUTPUT_FILE}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            raise
            
    def _cleanup(self):
        """Clean up resources."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
                
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass
                
        self.logger.info("=== APRA Statistical Publications Scraper Completed ===")


def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description='APRA Statistical Publications Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python apra_scraper.py                              # Daily run (3 pages)
  python apra_scraper.py --max-pages 5               # Custom page limit
  python apra_scraper.py --full-scrape               # Full scrape (50 pages)
  python apra_scraper.py --debug                     # Debug mode with verbose logging
  python apra_scraper.py --debug --no-headless       # Debug with visible browser
        """
    )
    parser.add_argument('--max-pages', type=int, default=DEFAULT_MAX_PAGES,
                       help=f'Maximum pages to scrape (default: {DEFAULT_MAX_PAGES})')
    parser.add_argument('--full-scrape', action='store_true',
                       help=f'Perform full scrape of up to {FULL_SCRAPE_PAGES} pages')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug mode with verbose logging')
    parser.add_argument('--no-headless', action='store_true',
                       help='Run browser in visible mode (useful for debugging)')
    
    args = parser.parse_args()
    
    # Determine max pages
    if args.full_scrape:
        max_pages = FULL_SCRAPE_PAGES
    else:
        max_pages = args.max_pages
        
    headless = not args.no_headless
        
    print(f"Starting APRA Statistical Publications scraper...")
    print(f"Max pages: {max_pages}")
    print(f"Debug mode: {args.debug}")
    print(f"Headless mode: {headless}")
    
    try:
        scraper = APRAScraper(max_pages=max_pages, debug=args.debug, headless=headless)
        scraper.scrape()
        print("Scraping completed successfully!")
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Scraping failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()