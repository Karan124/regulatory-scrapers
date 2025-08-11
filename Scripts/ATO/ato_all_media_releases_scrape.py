#!/usr/bin/env python3
"""
ATO Media Centre Scraper - Enhanced with PDF Extraction
Selenium-based approach with improved content extraction
ENHANCED PDF EXTRACTION - FULL CONTENT FOR LLM
"""

import json
import csv
import time
import logging
import hashlib
import re
import io
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import requests
import PyPDF2

# Try importing additional PDF libraries for better extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
    print("✅ pdfplumber available for enhanced PDF extraction")
except ImportError:
    HAS_PDFPLUMBER = False
    print("⚠️ pdfplumber not available - install with: pip install pdfplumber")

try:
    import pymupdf as fitz
    HAS_PYMUPDF = True
    print("✅ PyMuPDF available for enhanced PDF extraction")
except ImportError:
    HAS_PYMUPDF = False
    print("⚠️ PyMuPDF not available - install with: pip install PyMuPDF")

@dataclass
class Article:
    """Data class for ATO article with PDF support"""
    hash_id: str
    url: str
    title: str
    publication_date: str
    article_type: str
    content_text: str
    pdf_content: str
    pdf_url: str
    related_links: List[str]
    scraped_date: str
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)

class ATOMediaScraperSelenium:
    """ATO Media Centre scraper using Selenium for JavaScript content with PDF extraction"""
    
    def __init__(self, data_dir: str = None, max_pages: int = 10, headless: bool = True, initial_run: bool = False):
        """Initialize the scraper with dynamic configuration"""
        self.base_url = "https://www.ato.gov.au"
        self.media_centre_url = f"{self.base_url}/media-centre"
        
        # Setup directories
        self.script_dir = Path(__file__).parent
        self.data_dir = Path(data_dir) if data_dir else self.script_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        
        # File paths
        self.json_file = self.data_dir / "ato_all_media_releases.json"
        self.csv_file = self.data_dir / "ato_all_media_releases.csv"
        self.log_file = self.script_dir / "ato_scraper.log"
        
        # Setup logging
        self._setup_logging()
        
        # Load existing articles
        self.existing_articles = self._load_existing_articles()
        self.existing_urls = {article['url'] for article in self.existing_articles}
        self.existing_hashes = {article.get('hash_id') for article in self.existing_articles 
                               if article.get('hash_id')}
        
        # Configuration attributes (set before _configure_run_settings)
        self.headless = headless
        
        # DYNAMIC CONFIGURATION BASED ON RUN TYPE
        self.initial_run = initial_run
        self._configure_run_settings(max_pages)
        
        # Selenium driver
        self.driver = None
        
        # Statistics
        self.stats = {
            'pages_processed': 0,
            'articles_found': 0,
            'articles_scraped': 0,
            'articles_skipped': 0,
            'pdfs_extracted': 0,
            'errors': 0
        }
        
        self.logger.info("="*60)
        if self.initial_run:
            self.logger.info("ATO Media Scraper initialized - INITIAL RUN MODE")
        else:
            self.logger.info("ATO Media Scraper initialized - DAILY RUN MODE")
        self.logger.info(f"Existing articles: {len(self.existing_articles)}")
        self.logger.info(f"Max pages: {self.max_pages}")
        self.logger.info(f"Date cutoff: {self.cutoff_date.strftime('%B %d, %Y')} onwards")
        self.logger.info(f"Early stopping: {'DISABLED' if self.initial_run else f'{self.early_stop_threshold} old articles'}")
        self.logger.info(f"Headless mode: {self.headless}")
        self.logger.info(f"PDF Libraries: PyMuPDF={HAS_PYMUPDF}, pdfplumber={HAS_PDFPLUMBER}, PyPDF2=True")
        self.logger.info("="*60)
    
    def _configure_run_settings(self, max_pages_override: int = None):
        """Configure scraper settings based on run type"""
        if self.initial_run:
            # INITIAL RUN SETTINGS - Comprehensive historical scrape
            self.max_pages = max_pages_override or 20  # More pages for comprehensive scrape
            self.cutoff_date = datetime(2023, 1, 1)  # Go back 2 years for comprehensive coverage
            self.early_stop_threshold = 50  # High threshold - essentially disabled
            self.apply_date_filter = False  # Minimal date filtering for initial run
            self.request_delay = 1.0  # Faster requests for bulk scraping
            self.logger.info("🚀 INITIAL RUN MODE: Comprehensive historical scrape")
            self.logger.info(f"   📅 Date range: {self.cutoff_date.strftime('%B %d, %Y')} onwards")
            self.logger.info(f"   📄 Max pages: {self.max_pages}")
            self.logger.info(f"   🛑 Early stopping: DISABLED")
        else:
            # DAILY RUN SETTINGS - Recent items only with 6-month dynamic cutoff
            self.max_pages = max_pages_override or 5  # Only load a few pages for recent items
            self.cutoff_date = self._calculate_six_months_ago()  # Dynamic 6-month cutoff
            self.early_stop_threshold = 10  # Stop early if finding old articles
            self.apply_date_filter = True  # Strict date filtering for daily runs
            self.request_delay = 2.0  # More conservative requests for daily runs
            self.logger.info("📅 DAILY RUN MODE: Recent items with 6-month cutoff")
            self.logger.info(f"   📅 Dynamic cutoff: {self.cutoff_date.strftime('%B %d, %Y')} onwards")
            self.logger.info(f"   📄 Max pages: {self.max_pages}")
            self.logger.info(f"   🛑 Early stopping: {self.early_stop_threshold} old articles")
    
    def _calculate_six_months_ago(self) -> datetime:
        """Calculate dynamic 6-month cutoff date"""
        today = datetime.now()
        
        # Calculate 6 months ago, handling year boundaries
        if today.month >= 7:
            # Current month is July or later
            six_months_ago = today.replace(month=today.month - 6, day=1)
        else:
            # Current month is Jan-June, need to go to previous year
            six_months_ago = today.replace(year=today.year - 1, month=today.month + 6, day=1)
        
        self.logger.info(f"🕒 Dynamic 6-month cutoff calculated: {six_months_ago.strftime('%B %d, %Y')}")
        return six_months_ago
    
    def _is_article_too_old(self, publication_date: str) -> bool:
        """Check if article is too old based on run type and cutoff date"""
        if not self.apply_date_filter:
            return False  # Initial run - don't filter by date
        
        if publication_date == "Unknown":
            return False  # Don't filter articles without dates
        
        try:
            article_date = datetime.strptime(publication_date, '%Y-%m-%d')
            return article_date < self.cutoff_date
        except ValueError:
            return False  # Don't filter unparseable dates
    
    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler = logging.FileHandler(self.log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        self.logger = logging.getLogger('ATOScraperSelenium')
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def _setup_driver(self) -> webdriver.Chrome:
        """Setup Chrome WebDriver with enhanced anti-detection options"""
        chrome_options = Options()
        
        # ORCHESTRATOR COMPATIBILITY: Always run headless in production
        if self.headless or self._is_running_in_orchestrator():
            chrome_options.add_argument("--headless")
        
        # Essential arguments for server environments
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        
        # ENHANCED ANTI-DETECTION MEASURES
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Realistic user agent
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Additional stealth options
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        # ORCHESTRATOR COMPATIBILITY: Disable images but NOT CSS (needed for content structure)
        chrome_options.add_experimental_option("prefs", {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0
        })
        
        try:
            # ORCHESTRATOR COMPATIBILITY: Try different driver initialization methods
            driver = None
            
            # Method 1: Try system chromedriver
            try:
                driver = webdriver.Chrome(options=chrome_options)
                self.logger.info("Using system Chrome driver")
            except Exception as e1:
                self.logger.debug(f"System Chrome driver failed: {e1}")
                
                # Method 2: Try webdriver-manager (if available)
                try:
                    from webdriver_manager.chrome import ChromeDriverManager
                    service = Service(ChromeDriverManager().install())
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                    self.logger.info("Using webdriver-manager Chrome driver")
                except ImportError:
                    self.logger.debug("webdriver-manager not available")
                except Exception as e2:
                    self.logger.debug(f"webdriver-manager failed: {e2}")
                    
                    # Method 3: Try specific chromedriver path (common in orchestrator environments)
                    try:
                        service = Service("/usr/bin/chromedriver")  # Common Linux path
                        driver = webdriver.Chrome(service=service, options=chrome_options)
                        self.logger.info("Using /usr/bin/chromedriver")
                    except Exception as e3:
                        self.logger.debug(f"Specific path failed: {e3}")
                        raise e1  # Raise the original error
            
            if not driver:
                raise Exception("Could not initialize Chrome driver with any method")
            
            # ENHANCED ANTI-DETECTION: Remove webdriver property
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Set realistic navigator properties
            driver.execute_script("""
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
            """)
                
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            self.logger.info("ORCHESTRATOR SETUP INSTRUCTIONS:")
            self.logger.info("1. Ensure Chrome browser is installed")
            self.logger.info("2. Install chromedriver: apt-get install chromium-chromedriver")
            self.logger.info("3. Or install webdriver-manager: pip install webdriver-manager")
            raise
        
        # Set timeouts
        driver.implicitly_wait(10)
        driver.set_page_load_timeout(30)
        
        return driver
    
    def _is_running_in_orchestrator(self) -> bool:
        """Check if script is running in an orchestrator environment"""
        import os
        
        # Check for common orchestrator environment indicators
        orchestrator_indicators = [
            'ORCHESTRATOR_ENV',
            'DOCKER_CONTAINER',
            'CI',
            'GITHUB_ACTIONS',
            'JENKINS_URL'
        ]
        
        for indicator in orchestrator_indicators:
            if os.environ.get(indicator):
                return True
        
        # Check if running in a headless environment (no display)
        if not os.environ.get('DISPLAY') and os.name != 'nt':  # Not Windows
            return True
            
        return False
    
    def _generate_hash(self, url: str, title: str, date: str) -> str:
        """Generate unique hash for article"""
        content = f"{url}_{title}_{date}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def _clean_pdf_text(self, text: str) -> str:
        """Clean extracted PDF text for LLM consumption"""
        if not text:
            return ""
        
        # Remove excessive whitespace while preserving paragraph structure
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Max 2 consecutive newlines
        text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces/tabs to single space
        text = re.sub(r'^\s+|\s+$', '', text, flags=re.MULTILINE)  # Trim each line
        
        # Remove page numbers and headers/footers patterns
        text = re.sub(r'\n\s*\d+\s*\n', '\n', text)  # Standalone page numbers
        text = re.sub(r'\n\s*Page \d+ of \d+\s*\n', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'\n\s*\d+\s*/\s*\d+\s*\n', '\n', text)  # Page x/y format
        
        # Clean up common PDF artifacts
        text = re.sub(r'[^\w\s\.\,\;\:\!\?\-\(\)\[\]\{\}\"\'\/\\\@\#\$\%\&\*\+\=\<\>\~\`\|\n]', '', text)
        
        # Remove excessive line breaks but preserve structure
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        return text.strip()
    
    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Enhanced PDF text extraction with multiple methods - FULL CONTENT for LLM"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            self.logger.info(f"📄 Downloading PDF from: {pdf_url}")
            response = requests.get(pdf_url, headers=headers, timeout=60)
            if response.status_code != 200:
                self.logger.error(f"Failed to download PDF: {pdf_url} (Status: {response.status_code})")
                return ""
            
            pdf_content = response.content
            self.logger.info(f"Downloaded PDF, size: {len(pdf_content)/1024/1024:.1f}MB")
            
            extracted_text = ""
            
            # Method 1: Try PyMuPDF first (best for complete extraction)
            if HAS_PYMUPDF and not extracted_text:
                try:
                    self.logger.info("🔍 Trying PyMuPDF for complete PDF extraction...")
                    pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
                    text_parts = []
                    
                    self.logger.info(f"📖 Processing {pdf_document.page_count} pages with PyMuPDF")
                    
                    for page_num in range(pdf_document.page_count):
                        try:
                            page = pdf_document[page_num]
                            
                            # Get text blocks for better structure
                            blocks = page.get_text("blocks")
                            page_text_parts = []
                            
                            for block in blocks:
                                if len(block) > 4 and block[4].strip():
                                    page_text_parts.append(block[4].strip())
                            
                            if page_text_parts:
                                page_text = "\n".join(page_text_parts)
                                text_parts.append(page_text)
                                
                            # Progress logging for large PDFs
                            if page_num % 50 == 0 and page_num > 0:
                                self.logger.info(f"PyMuPDF: Processed {page_num} pages...")
                                
                        except Exception as e:
                            self.logger.warning(f"PyMuPDF error on page {page_num + 1}: {e}")
                            continue
                    
                    pdf_document.close()
                    
                    if text_parts:
                        extracted_text = "\n\n".join(text_parts)
                        self.logger.info(f"✅ PyMuPDF extracted {len(extracted_text)} characters from {len(text_parts)} pages")
                    
                except Exception as e:
                    self.logger.warning(f"❌ PyMuPDF extraction failed: {e}")
            
            # Method 2: Try pdfplumber if PyMuPDF failed (excellent for tables)
            if HAS_PDFPLUMBER and not extracted_text:
                try:
                    self.logger.info("🔍 Trying pdfplumber for complete PDF extraction...")
                    with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                        text_parts = []
                        
                        self.logger.info(f"📖 Processing {len(pdf.pages)} pages with pdfplumber")
                        
                        for page_num, page in enumerate(pdf.pages):
                            try:
                                # Extract regular text
                                page_text = page.extract_text()
                                if page_text and page_text.strip():
                                    text_parts.append(page_text.strip())
                                
                                # Extract tables separately for better structure
                                tables = page.extract_tables()
                                if tables:
                                    for table_num, table in enumerate(tables):
                                        if table:
                                            table_text = "\n".join([
                                                " | ".join([str(cell) if cell else "" for cell in row])
                                                for row in table if row
                                            ])
                                            if table_text.strip():
                                                text_parts.append(f"[TABLE {table_num + 1}]\n{table_text}\n[END TABLE]")
                                
                                # Progress logging
                                if page_num % 50 == 0 and page_num > 0:
                                    self.logger.info(f"pdfplumber: Processed {page_num} pages...")
                                    
                            except Exception as e:
                                self.logger.warning(f"pdfplumber error on page {page_num + 1}: {e}")
                                continue
                        
                        if text_parts:
                            extracted_text = "\n\n".join(text_parts)
                            self.logger.info(f"✅ pdfplumber extracted {len(extracted_text)} characters from {len(text_parts)} pages")
                    
                except Exception as e:
                    self.logger.warning(f"❌ pdfplumber extraction failed: {e}")
            
            # Method 3: Fallback to PyPDF2 with ALL pages
            if not extracted_text:
                try:
                    self.logger.info("🔍 Trying PyPDF2 for complete PDF extraction...")
                    pdf_file = io.BytesIO(pdf_content)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    
                    self.logger.info(f"📖 Processing ALL {len(pdf_reader.pages)} pages with PyPDF2")
                    
                    text_parts = []
                    # Extract from ALL pages
                    for page_num in range(len(pdf_reader.pages)):
                        try:
                            page = pdf_reader.pages[page_num]
                            page_text = page.extract_text()
                            if page_text and page_text.strip():
                                text_parts.append(page_text.strip())
                                
                            # Progress logging for large PDFs
                            if page_num % 50 == 0 and page_num > 0:
                                self.logger.info(f"PyPDF2: Processed {page_num} pages...")
                                
                        except Exception as e:
                            self.logger.warning(f"PyPDF2 error on page {page_num + 1}: {e}")
                            continue
                    
                    if text_parts:
                        extracted_text = "\n\n".join(text_parts)
                        self.logger.info(f"✅ PyPDF2 extracted {len(extracted_text)} characters from {len(text_parts)} pages")
                    
                except Exception as e:
                    self.logger.error(f"❌ PyPDF2 extraction failed: {e}")
            
            # Clean the extracted text for LLM consumption
            if extracted_text:
                cleaned_text = self._clean_pdf_text(extracted_text)
                
                self.logger.info(f"📝 Final cleaned PDF text: {len(cleaned_text)} characters (FULL CONTENT)")
                
                # Add metadata for LLM context
                metadata_header = f"\n--- PDF EXTRACTION METADATA ---\n"
                metadata_header += f"Source: {pdf_url}\n"
                metadata_header += f"Extraction Date: {datetime.now().isoformat()}\n"
                metadata_header += f"Content Length: {len(cleaned_text)} characters\n"
                metadata_header += f"Extraction Method: {'PyMuPDF' if HAS_PYMUPDF else 'pdfplumber' if HAS_PDFPLUMBER else 'PyPDF2'}\n"
                metadata_header += f"--- END METADATA ---\n\n"
                
                return metadata_header + cleaned_text
            else:
                self.logger.warning(f"❌ No text extracted from PDF: {pdf_url}")
                return ""
                
        except Exception as e:
            self.logger.error(f"❌ Error extracting PDF text from {pdf_url}: {e}")
            return ""
    
    def _load_existing_articles(self) -> List[Dict]:
        """Load existing articles from JSON file"""
        if not self.json_file.exists():
            self.logger.info("No existing articles file found - starting fresh")
            return []
        
        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                articles = json.load(f)
                if not isinstance(articles, list):
                    return []
                
                for article in articles:
                    if not article.get('hash_id'):
                        article['hash_id'] = self._generate_hash(
                            article.get('url', ''),
                            article.get('title', ''),
                            article.get('publication_date', '')
                        )
                    # Ensure PDF fields exist
                    if 'pdf_content' not in article:
                        article['pdf_content'] = ''
                    if 'pdf_url' not in article:
                        article['pdf_url'] = ''
                
                self.logger.info(f"Loaded {len(articles)} existing articles")
                return articles
                
        except Exception as e:
            self.logger.error(f"Error loading existing articles: {e}")
            return []
    
    def _wait_for_search_results(self, driver: webdriver.Chrome, timeout: int = 20) -> bool:
        """Wait for search results to load"""
        try:
            self.logger.info("Waiting for search results to load...")
            
            # Wait for the specific ATO search result items to appear
            wait = WebDriverWait(driver, timeout)
            
            # Wait for at least one search result item
            wait.until(EC.presence_of_element_located((
                By.CSS_SELECTOR, 
                'div.AtoSearchResultsItem_result-item__DBedq[data-testid="AtoSearchResultItem-element"]'
            )))
            
            self.logger.info("Search results loaded successfully")
            return True
            
        except TimeoutException:
            self.logger.warning(f"Timeout waiting for search results after {timeout} seconds")
            
            # Check if there's a loading indicator
            try:
                loading_elements = driver.find_elements(By.CSS_SELECTOR, '[class*="loading"], [class*="spinner"]')
                if loading_elements:
                    self.logger.info("Still loading, waiting a bit more...")
                    time.sleep(5)
                    return self._wait_for_search_results(driver, timeout=10)
            except:
                pass
            
            return False
    
    def _extract_articles_from_page(self, driver: webdriver.Chrome) -> List[Dict]:
        """Extract articles from the current page"""
        articles = []
        
        try:
            # Find all search result items using the exact selector from the HTML you provided
            result_items = driver.find_elements(
                By.CSS_SELECTOR, 
                'div.AtoSearchResultsItem_result-item__DBedq[data-testid="AtoSearchResultItem-element"]'
            )
            
            self.logger.info(f"Found {len(result_items)} search result items")
            
            for item in result_items:
                try:
                    article_data = self._extract_article_from_element(item)
                    if article_data:
                        articles.append(article_data)
                except Exception as e:
                    self.logger.debug(f"Error extracting article from element: {e}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error finding search result items: {e}")
        
        return articles
    
    def _extract_article_from_element(self, element) -> Optional[Dict]:
        """Extract article data from a Selenium WebElement with date filtering"""
        
        try:
            # Extract URL using the exact selector
            link_element = element.find_element(By.CSS_SELECTOR, 'a.AtoSearchResultsItem_result-item__title__i4i4q')
            href = link_element.get_attribute('href')
            
            if not href:
                return None
            
            # Skip if already exists
            if href in self.existing_urls:
                self.stats['articles_skipped'] += 1
                return None
            
            # Extract title using the exact selector
            title_element = link_element.find_element(By.CSS_SELECTOR, 'h2.AtoSearchResultsItem_result-item__heading__fpfOa')
            title = title_element.text.strip()
            
            if not title:
                return None
            
            # Extract date from tag containers
            date = self._extract_date_from_element(element)
            
            # Apply date filtering based on run type
            if self._is_article_too_old(date):
                self.logger.debug(f"❌ Article too old for {('initial' if self.initial_run else 'daily')} run: {title[:50]}... ({date})")
                self.stats['articles_skipped'] += 1
                return None
            
            # Extract article type
            try:
                type_element = element.find_element(By.CSS_SELECTOR, 'span.AtoSearchResultsItem_result-item__tag__WPGUA')
                article_type = type_element.text.strip()
            except NoSuchElementException:
                article_type = "Media Release"
            
            # Generate hash
            hash_id = self._generate_hash(href, title, date)
            
            # Skip if hash exists
            if hash_id in self.existing_hashes:
                self.stats['articles_skipped'] += 1
                return None
            
            self.stats['articles_found'] += 1
            run_type = "initial" if self.initial_run else "daily"
            self.logger.info(f"Found article ({run_type} run): {title[:60]}... ({date})")
            
            return {
                'hash_id': hash_id,
                'url': href,
                'title': title,
                'publication_date': date,
                'article_type': article_type
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting article data: {e}")
            return None
    
    def _extract_date_from_element(self, element) -> str:
        """Extract publication date from element"""
        
        try:
            # Find all tag containers
            tag_containers = element.find_elements(By.CSS_SELECTOR, 'div.AtoSearchResultsItem_result-item__tag-container__igzuF')
            
            for container in tag_containers:
                text = container.text.strip()
                # Skip if it contains "Media releases" or other non-date text
                if any(word in text.lower() for word in ['media', 'release', 'news', 'article']):
                    continue
                
                # Try to parse as date
                parsed = self._parse_date(text)
                if parsed:
                    return parsed
            
        except Exception as e:
            self.logger.debug(f"Error extracting date: {e}")
        
        return "Unknown"
    
    def _parse_date(self, date_text: str) -> Optional[str]:
        """Parse various date formats"""
        if not date_text:
            return None
            
        date_text = date_text.strip()
        
        if re.match(r'^\d{4}-\d{2}-\d{2}', date_text):
            return date_text[:10]
        
        date_formats = [
            '%d %B %Y',      # 26 June 2025
            '%d %b %Y',      # 26 Jun 2025
            '%B %d, %Y',     # June 26, 2025
            '%d/%m/%Y',      # 26/06/2025
            '%Y-%m-%d',      # 2025-06-26
        ]
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_text, fmt)
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        return None
    
    def _scrape_article_content(self, article_data: Dict) -> Article:
        """Scrape full content from individual article page with enhanced error handling"""
        url = article_data['url']
        
        article = Article(
            hash_id=article_data['hash_id'],
            url=url,
            title=article_data['title'],
            publication_date=article_data['publication_date'],
            article_type=article_data.get('article_type', 'Media Release'),
            content_text='',
            pdf_content='',
            pdf_url='',
            related_links=[],
            scraped_date=datetime.now().isoformat()
        )
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.logger.debug(f"Scraping content from: {url} (attempt {retry_count + 1})")
                
                # Navigate to URL with error handling
                try:
                    self.driver.get(url)
                except Exception as nav_error:
                    self.logger.warning(f"Navigation error for {url}: {nav_error}")
                    if "ERR_BLOCKED_BY_CLIENT" in str(nav_error) or "ERR_ACCESS_DENIED" in str(nav_error):
                        self.logger.error(f"❌ Access denied or blocked for {url}")
                        break
                    raise nav_error
                
                # Check for access denied page
                page_source = self.driver.page_source.lower()
                if any(error_text in page_source for error_text in [
                    'access denied', 'permission denied', 'blocked', 
                    'reference #', 'errors.edgesuite.net'
                ]):
                    self.logger.error(f"❌ Access denied detected for {url}")
                    article.content_text = "ACCESS_DENIED"
                    break
                
                # Wait for content to load properly
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.AtoContentWrapper_rich-text-content__HY8CB'))
                    )
                    self.logger.debug("Content area loaded successfully")
                except TimeoutException:
                    self.logger.warning("Timeout waiting for content area to load")
                
                # Additional wait for dynamic content
                time.sleep(2)
                
                # Extract content using BeautifulSoup for easier parsing
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # ENHANCED CONTENT EXTRACTION - ATO specific selectors
                content_text = ""
                
                # Method 1: Try primary ATO content container (most specific)
                content_elem = soup.select_one('div.AtoContentWrapper_rich-text-content__HY8CB')
                if content_elem:
                    self.logger.info("✅ Found content using primary ATO selector: div.AtoContentWrapper_rich-text-content__HY8CB")
                    
                    # Remove unwanted elements
                    for tag in content_elem(['script', 'style', 'aside', 'nav', 'footer', 'header', 'button']):
                        tag.decompose()
                    
                    # Extract text with proper spacing
                    content_text = content_elem.get_text(separator='\n', strip=True)
                    
                    # Clean up the text for LLM consumption
                    content_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', content_text)  # Max 2 newlines
                    content_text = re.sub(r'[ \t]+', ' ', content_text)  # Multiple spaces to single
                    content_text = content_text.strip()
                    
                    self.logger.info(f"📝 Extracted {len(content_text)} characters of content")
                
                # Method 2: Fallback to broader content area
                if not content_text:
                    content_elem = soup.select_one('section.StructuredContentPageLayout_content-page__content__Cay7b')
                    if content_elem:
                        self.logger.info("⚠️ Using fallback selector: section.StructuredContentPageLayout_content-page__content__Cay7b")
                        
                        # Remove unwanted elements
                        for tag in content_elem(['script', 'style', 'aside', 'nav', 'footer', 'header', 'button']):
                            tag.decompose()
                        
                        content_text = content_elem.get_text(separator='\n', strip=True)
                        content_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', content_text)
                        content_text = re.sub(r'[ \t]+', ' ', content_text)
                        content_text = content_text.strip()
                        
                        self.logger.info(f"📝 Extracted {len(content_text)} characters of content (fallback)")
                
                # Method 3: Even broader fallback
                if not content_text:
                    content_elem = soup.select_one('section#content')
                    if content_elem:
                        self.logger.info("⚠️ Using broad fallback selector: section#content")
                        
                        # Remove unwanted elements
                        for tag in content_elem(['script', 'style', 'aside', 'nav', 'footer', 'header', 'button']):
                            tag.decompose()
                        
                        content_text = content_elem.get_text(separator='\n', strip=True)
                        content_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', content_text)
                        content_text = re.sub(r'[ \t]+', ' ', content_text)
                        content_text = content_text.strip()
                        
                        self.logger.info(f"📝 Extracted {len(content_text)} characters of content (broad fallback)")
                
                # Final fallback to generic selectors if ATO specific ones fail
                if not content_text:
                    fallback_selectors = [
                        'main article',
                        'div.article-content',
                        'div.page-content',
                        'div.rich-text-content',
                        '.main-content',
                        'main'
                    ]
                    
                    for selector in fallback_selectors:
                        content_elem = soup.select_one(selector)
                        if content_elem:
                            self.logger.info(f"⚠️ Using generic fallback selector: {selector}")
                            for tag in content_elem(['script', 'style', 'aside', 'nav', 'footer', 'header']):
                                tag.decompose()
                            
                            content_text = content_elem.get_text(separator='\n', strip=True)
                            content_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', content_text)
                            content_text = re.sub(r'[ \t]+', ' ', content_text)
                            content_text = content_text.strip()
                            break
                
                # Debug logging if no content found
                if not content_text:
                    self.logger.error(f"❌ No content extracted from {url}")
                    # Save debug HTML for investigation
                    debug_file = self.script_dir / f"debug_no_content_{article_data['hash_id'][:8]}.html"
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(self.driver.page_source)
                    self.logger.error(f"Saved debug HTML to {debug_file}")
                
                article.content_text = content_text
                
                # ENHANCED PDF EXTRACTION (only if content extraction succeeded)
                if content_text and content_text != "ACCESS_DENIED":
                    pdf_url = self._find_pdf_link(soup, url)
                    if pdf_url:
                        article.pdf_url = pdf_url
                        self.logger.info(f"🔍 Found PDF link: {pdf_url}")
                        
                        # Extract PDF content with full content extraction
                        pdf_text = self._extract_pdf_text(pdf_url)
                        if pdf_text:
                            article.pdf_content = pdf_text
                            self.stats['pdfs_extracted'] += 1
                            self.logger.info(f"✅ Successfully extracted {len(pdf_text)} characters from PDF")
                        else:
                            self.logger.warning(f"❌ Failed to extract text from PDF: {pdf_url}")
                
                # Extract related links from the content area
                links = []
                if content_text and content_text != "ACCESS_DENIED":
                    content_area = soup.select_one('div.AtoContentWrapper_rich-text-content__HY8CB') or soup.select_one('section#content')
                    if content_area:
                        for a_tag in content_area.find_all('a', href=True)[:10]:
                            href = a_tag['href']
                            if href.startswith('/'):
                                href = urljoin(self.base_url, href)
                            elif not href.startswith('http'):
                                href = urljoin(url, href)
                            if href != url and href not in links:
                                links.append(href)
                
                article.related_links = links
                
                # Success - break out of retry loop
                if content_text:
                    content_status = "ACCESS_DENIED" if content_text == "ACCESS_DENIED" else f"{len(content_text)} chars"
                    self.logger.info(f"Scraped: {article.title[:60]}... (Content: {content_status})")
                    self.stats['articles_scraped'] += 1
                    break
                else:
                    retry_count += 1
                    if retry_count < max_retries:
                        self.logger.warning(f"Retrying content extraction for {url} (attempt {retry_count + 1})")
                        time.sleep(5)  # Wait before retry
                    
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Error scraping content from {url} (attempt {retry_count}): {e}")
                
                if "net::ERR_BLOCKED_BY_CLIENT" in str(e) or "Access Denied" in str(e):
                    self.logger.error(f"❌ Blocked/denied access to {url}")
                    article.content_text = "ACCESS_DENIED"
                    break
                
                if retry_count < max_retries:
                    self.logger.info(f"Retrying in 5 seconds... (attempt {retry_count + 1}/{max_retries})")
                    time.sleep(5)
                else:
                    self.logger.error(f"Failed to scrape {url} after {max_retries} attempts")
                    import traceback
                    self.logger.debug(traceback.format_exc())
                    self.stats['errors'] += 1
        
        return article
    
    def _find_pdf_link(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Find PDF download links on the page"""
        pdf_patterns = [
            # Direct PDF links
            soup.find('a', href=re.compile(r'\.pdf, re.I')),
            soup.find('a', href=re.compile(r'\.pdf\?', re.I)),
            
            # Links with PDF text
            soup.find('a', string=re.compile(r'pdf', re.I)),
            soup.find('a', text=re.compile(r'download', re.I)),
            
            # Common ATO PDF selectors
            soup.find('a', class_=re.compile(r'pdf', re.I)),
            soup.find('a', class_=re.compile(r'download', re.I)),
            
            # Print and download section
            soup.select_one('div.PrintAndDownload_print-and-download___FBWo a[href*=".pdf"]'),
            
            # Generic download links
            soup.find('a', {'data-download': True}),
            soup.find('a', {'download': True})
        ]
        
        for pattern in pdf_patterns:
            if pattern:
                href = pattern.get('href', '')
                if href and '.pdf' in href.lower():
                    # Ensure absolute URL
                    if href.startswith('/'):
                        return urljoin(self.base_url, href)
                    elif not href.startswith('http'):
                        return urljoin(base_url, href)
                    else:
                        return href
        
        return None
    
    def _handle_pagination(self, driver: webdriver.Chrome, page_num: int) -> bool:
        """Handle pagination using ATO's numbered pagination with early stopping for daily runs"""
        
        try:
            # For daily runs, check if we should stop early based on old articles
            if not self.initial_run:
                current_items = driver.find_elements(
                    By.CSS_SELECTOR, 
                    'div.AtoSearchResultsItem_result-item__DBedq[data-testid="AtoSearchResultItem-element"]'
                )
                
                # Check recent items for age
                recent_items = current_items[-10:] if len(current_items) >= 10 else current_items
                old_items_count = 0
                
                for item in recent_items:
                    try:
                        date = self._extract_date_from_element(item)
                        if self._is_article_too_old(date):
                            old_items_count += 1
                    except:
                        continue
                
                if old_items_count >= self.early_stop_threshold:
                    self.logger.info(f"🛑 Found {old_items_count} old articles in recent batch, stopping early (daily run)")
                    return False
            
            # Look for ATO pagination - try Next button first
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, 'button.AtoPagination_pagination__btn--next__cl5GH')
                if next_button.is_enabled() and next_button.is_displayed():
                    run_type = "initial" if self.initial_run else "daily"
                    self.logger.info(f"Clicking 'Next' button for page {page_num} ({run_type} run)")
                    
                    # Scroll to button and click
                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_button)
                    
                    # Wait for new page to load
                    time.sleep(3)
                    
                    # Wait for new content to load
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((
                                By.CSS_SELECTOR, 
                                'div.AtoSearchResultsItem_result-item__DBedq[data-testid="AtoSearchResultItem-element"]'
                            ))
                        )
                        return True
                    except TimeoutException:
                        self.logger.warning("Timeout waiting for new page content")
                        return False
                        
            except NoSuchElementException:
                self.logger.debug("Next button not found")
            
            # Fallback: Try clicking specific page number button
            try:
                page_button_selector = f'button.AtoPagination_pagination__btn__Yynmp[aria-label="Goto Page{page_num}"]'
                page_button = driver.find_element(By.CSS_SELECTOR, page_button_selector)
                
                if page_button.is_enabled() and page_button.is_displayed():
                    run_type = "initial" if self.initial_run else "daily"
                    self.logger.info(f"Clicking page {page_num} button ({run_type} run)")
                    
                    # Scroll to button and click
                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", page_button)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", page_button)
                    
                    # Wait for new page to load
                    time.sleep(3)
                    
                    # Wait for new content to load
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((
                                By.CSS_SELECTOR, 
                                'div.AtoSearchResultsItem_result-item__DBedq[data-testid="AtoSearchResultItem-element"]'
                            ))
                        )
                        return True
                    except TimeoutException:
                        self.logger.warning(f"Timeout waiting for page {page_num} content")
                        return False
                        
            except NoSuchElementException:
                self.logger.debug(f"Page {page_num} button not found")
            
            # If both methods fail, check if we've reached the end
            try:
                # Check if there are any more page buttons available
                all_page_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.AtoPagination_pagination__btn__Yynmp[aria-label*="Goto Page"]')
                available_pages = []
                
                for button in all_page_buttons:
                    aria_label = button.get_attribute('aria-label')
                    if aria_label and 'Goto Page' in aria_label:
                        try:
                            page_number = int(aria_label.replace('Goto Page', ''))
                            available_pages.append(page_number)
                        except ValueError:
                            continue
                
                if available_pages:
                    max_available_page = max(available_pages)
                    if page_num > max_available_page:
                        self.logger.info(f"Reached end of pagination. Max available page: {max_available_page}")
                        return False
                
            except Exception as e:
                self.logger.debug(f"Error checking pagination limits: {e}")
            
            self.logger.info(f"No pagination option found for page {page_num}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error handling pagination: {e}")
            return False
    
    def scrape_media_releases(self) -> List[Article]:
        """Main method to scrape media releases with adaptive behavior"""
        all_new_articles = []
        
        try:
            run_type = "INITIAL" if self.initial_run else "DAILY"
            self.logger.info(f"Starting ATO media release scraping - {run_type} RUN MODE")
            
            # Setup driver
            self.driver = self._setup_driver()
            
            # Navigate to media centre
            self.logger.info(f"Navigating to: {self.media_centre_url}")
            self.driver.get(self.media_centre_url)
            
            # Wait for search results to load
            if not self._wait_for_search_results(self.driver):
                self.logger.error("Failed to load search results")
                return []
            
            # Save debug HTML after content loads
            debug_file = self.script_dir / f"debug_selenium_page_{run_type.lower()}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            self.logger.info(f"Saved loaded page HTML to {debug_file}")
            
            # Extract articles from initial page
            articles = self._extract_articles_from_page(self.driver)
            self.logger.info(f"Found {len(articles)} articles on initial page ({run_type} run)")
            
            # Process pagination with adaptive behavior
            current_page = 1
            for page_num in range(2, self.max_pages + 1):
                self.logger.info(f"Attempting to navigate to page {page_num}...")
                
                if not self._handle_pagination(self.driver, page_num):
                    reason = "early stopping" if not self.initial_run else "no more pages"
                    self.logger.info(f"Stopping pagination after page {current_page}: {reason}")
                    break
                
                # Successfully navigated to next page
                current_page = page_num
                
                # Wait a moment for page to fully load
                time.sleep(2)
                
                # Extract articles from new page
                page_articles = self._extract_articles_from_page(self.driver)
                
                # Filter out articles we already have
                new_articles = [
                    article for article in page_articles 
                    if article['url'] not in [existing['url'] for existing in articles]
                ]
                
                self.logger.info(f"Found {len(page_articles)} total articles on page {page_num}, {len(new_articles)} new")
                
                if len(new_articles) == 0:
                    if not self.initial_run:  # Daily run - stop if no new articles
                        self.logger.info(f"No new articles found on page {page_num} (daily run) - stopping")
                        break
                    else:
                        self.logger.info(f"No new articles found on page {page_num} (initial run) - continuing")
                
                articles.extend(new_articles)
                self.stats['pages_processed'] += 1
            
            # Remove duplicates
            unique_articles = []
            seen_urls = set()
            for article in articles:
                if article['url'] not in seen_urls:
                    unique_articles.append(article)
                    seen_urls.add(article['url'])
            
            self.logger.info(f"Total unique articles found: {len(unique_articles)} ({run_type} run)")
            
            # Scrape content for each article with enhanced extraction
            for i, article_data in enumerate(unique_articles, 1):
                self.logger.info(f"Processing article {i}/{len(unique_articles)}: {article_data['title'][:50]}...")
                article = self._scrape_article_content(article_data)
                all_new_articles.append(article)
                
                # Update tracking sets
                self.existing_urls.add(article.url)
                self.existing_hashes.add(article.hash_id)
                
                # Add delay between requests
                time.sleep(self.request_delay)
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
        
        finally:
            if self.driver:
                self.driver.quit()
        
        run_type = "initial" if self.initial_run else "daily"
        self.logger.info(f"Scraped {len(all_new_articles)} new articles ({run_type} run)")
        return all_new_articles
    
    def save_articles(self, new_articles: List[Article]) -> None:
        """Save articles to JSON and CSV files"""
        if not new_articles:
            self.logger.info("No new articles to save")
            return
        
        # Debug: Log the new articles
        self.logger.info("New articles found:")
        for i, article in enumerate(new_articles, 1):
            self.logger.info(f"  {i}. {article.title}")
            self.logger.info(f"     URL: {article.url}")
            self.logger.info(f"     Date: {article.publication_date}")
            if article.pdf_url:
                self.logger.info(f"     PDF: {article.pdf_url}")
        
        # Convert to dictionaries
        new_articles_data = [article.to_dict() for article in new_articles]
        
        # Combine with existing
        all_articles = self.existing_articles + new_articles_data
        
        # Remove duplicates
        seen_hashes = set()
        unique_articles = []
        for article in all_articles:
            hash_id = article.get('hash_id')
            if hash_id and hash_id not in seen_hashes:
                seen_hashes.add(hash_id)
                unique_articles.append(article)
        
        # Sort by date
        unique_articles.sort(
            key=lambda x: x.get('publication_date', '0000-00-00'),
            reverse=True
        )
        
        # Save JSON
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(unique_articles, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved {len(unique_articles)} total articles to {self.json_file}")
        except Exception as e:
            self.logger.error(f"Error saving JSON: {e}")
            return
        
        # Save CSV
        try:
            fieldnames = [
                'hash_id', 'url', 'title', 'publication_date', 
                'article_type', 'content_text', 'pdf_content', 'pdf_url',
                'related_links', 'scraped_date'
            ]
            
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for article in unique_articles:
                    row = {}
                    for field in fieldnames:
                        value = article.get(field, '')
                        if field == 'related_links' and isinstance(value, list):
                            row[field] = '|'.join(value)
                        elif field in ['content_text', 'pdf_content'] and len(str(value)) > 1000:
                            # Store full content in CSV for LLM analysis
                            row[field] = str(value)
                        else:
                            row[field] = value
                    writer.writerow(row)
            
            self.logger.info(f"Saved to {self.csv_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving CSV: {e}")
    
    def print_summary(self) -> int:
        """Print summary of scraping results"""
        self.logger.info("\n" + "="*60)
        self.logger.info("SCRAPING SUMMARY - ENHANCED WITH PDF EXTRACTION")
        self.logger.info("="*60)
        self.logger.info(f"Pages processed: {self.stats['pages_processed']}")
        self.logger.info(f"Articles found: {self.stats['articles_found']}")
        self.logger.info(f"Articles scraped: {self.stats['articles_scraped']}")
        self.logger.info(f"PDFs extracted: {self.stats['pdfs_extracted']}")
        self.logger.info(f"Articles skipped: {self.stats['articles_skipped']}")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info("🔥 FULL PDF CONTENT EXTRACTED FOR LLM ANALYSIS")
        self.logger.info("="*60)
        
        # ORCHESTRATOR COMPATIBILITY: Print to stdout for orchestrator
        print(f"ATO Media Scraper completed: {self.stats['articles_scraped']} new articles, {self.stats['pdfs_extracted']} PDFs extracted")
        
        # ORCHESTRATOR COMPATIBILITY: Return exit code based on success
        if self.stats['errors'] > 0:
            print("ERRORS OCCURRED - Check log file for details")
            return 1
        elif self.stats['articles_scraped'] == 0:
            print("No new articles found")
            return 0
        else:
            print("SUCCESS")
            return 0
    
    def run(self) -> int:
        """Main execution method - ORCHESTRATOR COMPATIBLE"""
        exit_code = 0
        
        try:
            new_articles = self.scrape_media_releases()
            self.save_articles(new_articles)
            exit_code = self.print_summary()
            
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
            exit_code = 1
            
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            print(f"FATAL ERROR: {e}")
            exit_code = 1
        
        finally:
            if hasattr(self, 'driver') and self.driver:
                try:
                    self.driver.quit()
                except:
                    pass  # Ignore cleanup errors
        
        return exit_code


def main():
    """Main function with dynamic run mode selection - ORCHESTRATOR COMPATIBLE"""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='Scrape media releases from ATO using Selenium with PDF extraction')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='Maximum number of pages to scrape (default: auto-configured based on run type)')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Directory for output files (default: data subdirectory)')
    parser.add_argument('--headless', action='store_true', default=True,
                        help='Run browser in headless mode (default: True)')
    parser.add_argument('--show-browser', action='store_false', dest='headless',
                        help='Show browser window (for debugging)')
    parser.add_argument('--initial', action='store_true', default=False,
                        help='Run in initial mode (comprehensive historical scrape)')
    
    args = parser.parse_args()
    
    # Determine run mode
    initial_run = args.initial
    
    scraper = ATOMediaScraperSelenium(
        data_dir=args.data_dir,
        max_pages=args.max_pages,
        headless=args.headless,
        initial_run=initial_run
    )
    
    # ORCHESTRATOR COMPATIBILITY: Return proper exit code
    exit_code = scraper.run()
    
    # Usage instructions
    if exit_code == 0:
        print("\n💡 USAGE:")
        print("   python script.py           # Daily run (6-month cutoff, 5 pages)")
        print("   python script.py --initial # Initial run (2 years, 20 pages)")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()