#!/usr/bin/env python3
"""
APRA Information Papers Scraper
Scrapes all Information Papers from APRA's website with stealth techniques
Handles PDFs, deduplication, and anti-bot measures
Enhanced version with comprehensive embedded PDF detection and Brotli decompression support

Required packages:
pip install requests selenium beautifulsoup4 PyPDF2 brotli

Optional (for better PDF extraction):
pip install pdfplumber PyMuPDF
"""

import json
import csv
import logging
import os
import re
import hashlib
import time
import random
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin, urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Selenium imports for JavaScript handling
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains

# PDF and HTML processing
import PyPDF2
from bs4 import BeautifulSoup
import io

# Optional PDF libraries - import with error handling
try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

# Compression support
try:
    import brotli
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False
    print("⚠️ WARNING: brotli library not available. PDF extraction may fail.")
    print("   Install with: pip install brotli")

# Configure logging
os.makedirs('data', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/apra_info_papers_scraper.log', mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set some loggers to WARNING to reduce noise
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

# APRA boilerplate patterns to remove
APRA_BOILERPLATE_PATTERNS = [
    r"The Australian Prudential Regulation Authority \(APRA\) is the prudential regulator of the financial services industry\. It oversees banks, mutuals, general insurance and reinsurance companies, life insurance, private health insurers, friendly societies, and most members of the superannuation industry\. APRA currently supervises institutions holding around \$9 trillion in assets for Australian depositors, policyholders and superannuation fund members\.?\s*",
    r"APRA acknowledges the Traditional Custodians of the lands and waters of Australia and pays respect to Aboriginal and Torres Strait Islander peoples past and present\. We would like to recognise our Aboriginal and Torres Strait Islander employees who are an integral part of our workforce\.?\s*",
    r"Media enquiries\s*Contact APRA Media Unit, on\s*\+61 2 9210 3636\s*All other enquiries\s*For more information contact APRA on\s*1300 558 849\.?\s*"
]

class APRAInfoPapersScraper:
    """
    APRA Information Papers scraper with stealth techniques and PDF processing
    """
    
    def __init__(self):
        self.base_url = "https://www.apra.gov.au"
        self.target_url = f"{self.base_url}/apra-information-papers"
        self.data_folder = Path("data")
        self.data_folder.mkdir(exist_ok=True)
        
        # File paths
        self.json_file = self.data_folder / "apra_info_papers.json"
        self.csv_file = self.data_folder / "apra_info_papers.csv"
        
        self.driver = None
        self.session = None
        self.existing_hashes = set()
        
        # Check for required libraries
        self._check_dependencies()
        
        # Load existing data for deduplication
        self._load_existing_data()
        
        # Setup stealth session
        self._setup_requests_session()
    
    def _check_dependencies(self):
        """Check for required and optional dependencies"""
        logger.info("🔍 Checking dependencies...")
        
        # Check required libraries
        required_libs = ['requests', 'selenium', 'bs4', 'PyPDF2']
        missing_required = []
        
        for lib in required_libs:
            try:
                __import__(lib)
                logger.debug(f"✅ {lib}: Available")
            except ImportError:
                missing_required.append(lib)
                logger.error(f"❌ {lib}: Missing")
        
        if missing_required:
            logger.error(f"Missing required libraries: {missing_required}")
            logger.error("Install with: pip install " + " ".join(missing_required))
            raise ImportError(f"Missing required libraries: {missing_required}")
        
        # Check critical library for APRA PDFs
        if not BROTLI_AVAILABLE:
            logger.error("❌ brotli: Missing (CRITICAL for APRA PDF extraction)")
            logger.error("   APRA's PDFs use Brotli compression and will fail without this library")
            logger.error("   Install with: pip install brotli")
            raise ImportError("brotli library is required for APRA PDF extraction")
        else:
            logger.info("✅ brotli: Available (required for APRA PDFs)")
        
        # Check optional libraries
        optional_status = []
        if PDFPLUMBER_AVAILABLE:
            optional_status.append("✅ pdfplumber")
        else:
            optional_status.append("⚠️ pdfplumber (recommended)")
            
        if PYMUPDF_AVAILABLE:
            optional_status.append("✅ PyMuPDF")
        else:
            optional_status.append("⚠️ PyMuPDF (recommended)")
        
        logger.info(f"Optional libraries: {', '.join(optional_status)}")
        
        if not PDFPLUMBER_AVAILABLE or not PYMUPDF_AVAILABLE:
            logger.info("💡 Install optional libraries for better PDF extraction:")
            logger.info("   pip install pdfplumber PyMuPDF")
    
    def _load_existing_data(self):
        """Load existing information papers to prevent duplicates"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    self.existing_hashes = {item.get('hash_id', '') for item in existing_data}
                    logger.info(f"Loaded {len(self.existing_hashes)} existing information paper records")
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                self.existing_hashes = set()
        else:
            logger.info("No existing data file found, starting fresh")
    
    def _setup_requests_session(self):
        """Setup requests session with stealth headers and retry strategy"""
        self.session = requests.Session()
        
        # Retry strategy - use allowed_methods instead of method_whitelist
        try:
            # Try newer urllib3 parameter name
            retry_strategy = Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"],
                backoff_factor=1
            )
        except TypeError:
            # Fallback for older urllib3 versions
            retry_strategy = Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                method_whitelist=["HEAD", "GET", "OPTIONS"],
                backoff_factor=1
            )
        except Exception:
            # Simple retry without method restrictions if all else fails
            retry_strategy = Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                backoff_factor=1
            )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Stealth headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
    
    def _generate_hash(self, url: str, title: str, published_date: str) -> str:
        """Generate unique hash for information paper"""
        content = f"{url}_{title}_{published_date}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _setup_driver(self):
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
        
        # Additional stealth measures
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")  # Speed up loading
        
        # Set realistic window size
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Updated user agent to match your Chrome version
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
        
        # Enable headless for faster execution (uncomment if needed)
        # chrome_options.add_argument("--headless=new")
        
        # Memory and performance optimizations
        chrome_options.add_argument("--max_old_space_size=4096")
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
            
            # Set additional stealth properties
            try:
                self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                    "userAgent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
                })
            except Exception as e:
                logger.warning(f"Could not set CDP user agent override: {e}")
            
            logger.info(f"Chrome driver initialized successfully")
            if chromedriver_path:
                logger.info(f"Using ChromeDriver: {chromedriver_path}")
            else:
                logger.info("Using ChromeDriver from PATH")
            
            return True
            
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
            
            return False
    
    def _establish_session(self):
        """Establish session by browsing around the site first"""
        try:
            logger.info("🔗 Establishing session with APRA website...")
            
            # First, visit the homepage to collect cookies
            logger.info("📄 Visiting APRA homepage...")
            self.driver.get(self.base_url)
            time.sleep(random.uniform(3, 5))
            
            # Scroll around to mimic human behavior
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
            time.sleep(random.uniform(1, 2))
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(random.uniform(1, 2))
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(random.uniform(1, 2))
            
            # Visit a few other pages to establish session
            pages_to_visit = [
                "/news-and-publications",
                "/about-apra"
            ]
            
            for page in pages_to_visit:
                try:
                    logger.info(f"📄 Visiting {page}...")
                    self.driver.get(f"{self.base_url}{page}")
                    time.sleep(random.uniform(2, 4))
                    
                    # Random scroll
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/4);")
                    time.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    logger.warning(f"Could not visit {page}: {e}")
                    continue
            
            logger.info("✅ Session established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error establishing session: {e}")
            return False
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats from APRA website"""
        try:
            # Clean up the date string
            original_date_str = date_str
            
            # Handle "Published 12 June 2025" format
            if "Published" in date_str:
                date_str = date_str.replace("Published", "").strip()
            
            # Handle "on 7 November 2019" format
            if date_str.lower().startswith("on "):
                date_str = date_str[3:].strip()  # Remove "on " prefix
            
            # Try multiple date formats
            date_formats = [
                "%d %B %Y",      # 12 June 2025, 7 November 2019
                "%d %b %Y",      # 12 Jun 2025, 7 Nov 2019
                "%B %d, %Y",     # June 12, 2025, November 7, 2019
                "%b %d, %Y",     # Jun 12, 2025, Nov 7, 2019
                "%B %Y",         # August 2018, October 2018
                "%b %Y",         # Aug 2018, Oct 2018
                "%Y-%m-%d",      # 2025-06-12
                "%d/%m/%Y",      # 12/06/2025
                "%m/%d/%Y",      # 06/12/2025
                "%Y-%m",         # 2018-08
                "%m/%Y",         # 08/2018
                "%Y"             # 2018 (year only)
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    # For formats without day, default to first day of month
                    if fmt in ["%B %Y", "%b %Y", "%Y-%m", "%m/%Y"]:
                        parsed_date = parsed_date.replace(day=1)
                    # For year-only format, default to January 1st
                    elif fmt == "%Y":
                        parsed_date = parsed_date.replace(month=1, day=1)
                    return parsed_date
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse date: {original_date_str}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing date '{date_str}': {e}")
            return None
    
    def _is_after_jan_2022(self, date_str: str) -> bool:
        """Check if date is after 1st Jan 2022"""
        parsed_date = self._parse_date(date_str)
        if parsed_date:
            cutoff_date = datetime(2022, 1, 1)
            return parsed_date >= cutoff_date
        return False
    
    def _extract_links_from_content(self, content: str) -> List[str]:
        """Extract all URLs from content text"""
        try:
            # Pattern to match URLs
            url_pattern = r'https?://[^\s<>"\'`|(){}[\]]*[^\s<>"\'`|(){}[\].,;:!?]'
            urls = re.findall(url_pattern, content)
            return list(set(urls))  # Remove duplicates
        except Exception as e:
            logger.warning(f"Error extracting links: {e}")
            return []

    def _remove_apra_boilerplate(self, content: str) -> str:
        """Remove standard APRA boilerplate text from content"""
        if not content:
            return ""
        
        cleaned_content = content
        
        # Remove boilerplate patterns
        for pattern in APRA_BOILERPLATE_PATTERNS:
            cleaned_content = re.sub(pattern, "", cleaned_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Clean up any resulting extra whitespace
        cleaned_content = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned_content)  # Remove triple+ newlines
        cleaned_content = re.sub(r'\s+', ' ', cleaned_content)  # Normalize spaces
        cleaned_content = cleaned_content.strip()
        
        return cleaned_content
    
    def _extract_pdf_text(self, pdf_url: str) -> tuple[str, List[str]]:
        """Extract text and links from PDF document with enhanced methods"""
        try:
            logger.debug(f"📄 Downloading PDF from: {pdf_url}")
            
            # Method 1: Try using Selenium to download (handles authentication/cookies)
            pdf_content = self._download_pdf_with_selenium(pdf_url)
            
            # Method 2: Fallback to requests if Selenium fails
            if not pdf_content:
                pdf_content = self._download_pdf_with_requests(pdf_url)
            
            if not pdf_content:
                logger.error(f"Failed to download PDF from: {pdf_url}")
                return "", []
            
            logger.debug(f"Downloaded PDF, size: {len(pdf_content)} bytes")
            
            # Validate PDF content
            if len(pdf_content) < 100:  # Too small to be a valid PDF
                logger.error(f"PDF file too small or empty: {pdf_url}")
                return "", []
            
            # Try multiple PDF extraction methods
            extraction_methods = [
                self._extract_with_pypdf2,
                self._extract_with_pypdf2_alternative,
                self._extract_with_pdfplumber,
                self._extract_with_pymupdf
            ]
            
            for method_name, method in zip(['PyPDF2', 'PyPDF2-Alt', 'pdfplumber', 'PyMuPDF'], extraction_methods):
                try:
                    logger.debug(f"Trying extraction method: {method_name}")
                    text_content, extracted_links = method(pdf_content)
                    
                    if text_content and len(text_content.strip()) > 50:  # Reasonable amount of text
                        logger.info(f"✅ PDF extraction successful with {method_name}: {len(text_content)} characters")
                        return text_content, extracted_links
                    else:
                        logger.debug(f"{method_name} extracted little/no text")
                        
                except Exception as e:
                    logger.debug(f"{method_name} failed: {e}")
                    continue
            
            # If all methods fail, log detailed info
            logger.warning(f"⚠️ All PDF extraction methods failed for: {pdf_url}")
            logger.warning(f"   PDF size: {len(pdf_content)} bytes")
            logger.warning(f"   PDF starts with: {pdf_content[:50]}")
            
            return "", []
            
        except Exception as e:
            logger.error(f"Error extracting PDF content from {pdf_url}: {e}")
            return "", []
    
    def _download_pdf_with_selenium(self, pdf_url: str) -> Optional[bytes]:
        """Download PDF using Selenium (handles cookies/authentication)"""
        try:
            if not self.driver:
                return None
            
            # Navigate to the PDF URL with Selenium
            self.driver.get(pdf_url)
            time.sleep(2)  # Wait for any redirects
            
            # Check if we got redirected to a login page or error page
            current_url = self.driver.current_url
            if 'error' in current_url.lower() or 'login' in current_url.lower():
                logger.warning(f"PDF URL may require authentication: {pdf_url}")
                return None
            
            # Get the page source to check if it's actually a PDF
            page_source = self.driver.page_source.lower()
            if 'pdf' not in page_source and len(self.driver.page_source) < 1000:
                logger.warning(f"Page doesn't appear to contain PDF: {pdf_url}")
                return None
            
            # Use requests with Selenium's cookies to download
            selenium_cookies = self.driver.get_cookies()
            
            # Convert Selenium cookies to requests format
            cookies_dict = {}
            for cookie in selenium_cookies:
                cookies_dict[cookie['name']] = cookie['value']
            
            # Download using requests with Selenium's cookies
            response = self.session.get(pdf_url, cookies=cookies_dict, timeout=60)
            if response.status_code == 200:
                return response.content
            else:
                logger.warning(f"HTTP {response.status_code} when downloading PDF with Selenium cookies")
                return None
                
        except Exception as e:
            logger.warning(f"Selenium PDF download failed: {e}")
            return None
    
    def _decompress_content(self, content: bytes, encoding: str) -> Optional[bytes]:
        """Manually decompress content if needed"""
        try:
            if not encoding:
                return content
            
            encoding = encoding.lower()
            
            if encoding == 'br':
                # Brotli decompression
                if BROTLI_AVAILABLE:
                    import brotli
                    return brotli.decompress(content)
                else:
                    logger.error("Brotli compression detected but brotli library not available")
                    logger.error("Install with: pip install brotli")
                    return None
                    
            elif encoding == 'gzip':
                import gzip
                return gzip.decompress(content)
                
            elif encoding == 'deflate':
                import zlib
                return zlib.decompress(content)
                
            else:
                logger.debug(f"Unknown encoding: {encoding}")
                return content
                
        except Exception as e:
            logger.warning(f"Content decompression failed: {e}")
            return None
    
    def _download_pdf_with_requests(self, pdf_url: str) -> Optional[bytes]:
        """Enhanced PDF download using requests with proper compression handling"""
        try:
            # Create a temporary session for this download
            session = requests.Session()
            
            # Setup headers for PDF download
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                'Accept': 'application/pdf,application/octet-stream,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',  # Important: support Brotli
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Cache-Control': 'max-age=0'
            })
            
            # First establish session by visiting homepage
            try:
                homepage_response = session.get(self.base_url, timeout=30)
                logger.debug(f"Homepage visit for PDF download: {homepage_response.status_code}")
            except Exception as e:
                logger.debug(f"Homepage visit failed: {e}")
                # Continue anyway
            
            # Set proper referrer
            session.headers['Referer'] = self.base_url
            
            # Download the PDF
            response = session.get(pdf_url, timeout=60, allow_redirects=True)
            
            if response.status_code == 200:
                logger.debug(f"PDF download successful: {len(response.content)} bytes")
                logger.debug(f"Content-Type: {response.headers.get('Content-Type')}")
                logger.debug(f"Content-Encoding: {response.headers.get('Content-Encoding')}")
                
                # Check if content is already decompressed (requests usually handles this)
                if response.content.startswith(b'%PDF'):
                    return response.content
                else:
                    # Try manual decompression if needed
                    encoding = response.headers.get('Content-Encoding')
                    if encoding:
                        logger.debug(f"Attempting manual decompression for encoding: {encoding}")
                        decompressed = self._decompress_content(response.content, encoding)
                        
                        if decompressed and decompressed.startswith(b'%PDF'):
                            logger.debug("Successfully manually decompressed PDF content")
                            return decompressed
                        else:
                            logger.warning("Manual decompression failed or didn't produce valid PDF")
                    
                    # If still no valid PDF, log details
                    logger.warning(f"Downloaded content doesn't appear to be valid PDF")
                    logger.debug(f"Content starts with: {response.content[:50]}")
                    return None
            else:
                logger.warning(f"HTTP {response.status_code} when downloading PDF with requests")
                return None
                
        except Exception as e:
            logger.warning(f"Requests PDF download failed: {e}")
            return None
    
    def _extract_with_pypdf2(self, pdf_content: bytes) -> tuple[str, List[str]]:
        """Extract PDF text using PyPDF2"""
        pdf_file = io.BytesIO(pdf_content)
        
        # Check if content starts with PDF header
        if not pdf_content.startswith(b'%PDF'):
            raise Exception("Invalid PDF format (missing %PDF header)")
        
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        text_content = []
        for page_num, page in enumerate(pdf_reader.pages):
            page_text = page.extract_text()
            if page_text and page_text.strip():
                text_content.append(page_text)
        
        if not text_content:
            raise Exception("No text extracted")
        
        full_text = " ".join(text_content)
        extracted_links = self._extract_links_from_content(full_text)
        full_text = self._clean_pdf_text(full_text)
        
        return full_text, extracted_links
    
    def _extract_with_pypdf2_alternative(self, pdf_content: bytes) -> tuple[str, List[str]]:
        """Alternative PyPDF2 extraction with different settings"""
        pdf_file = io.BytesIO(pdf_content)
        
        if not pdf_content.startswith(b'%PDF'):
            raise Exception("Invalid PDF format")
        
        pdf_reader = PyPDF2.PdfReader(pdf_file, strict=False)
        
        text_content = []
        for page in pdf_reader.pages:
            try:
                page_text = page.extract_text()
                if page_text and page_text.strip():
                    text_content.append(page_text)
            except:
                # Try alternative extraction
                try:
                    if '/Contents' in page:
                        content = page['/Contents']
                        if content:
                            text_content.append(str(content))
                except:
                    continue
        
        if not text_content:
            raise Exception("No text extracted")
        
        full_text = " ".join(text_content)
        extracted_links = self._extract_links_from_content(full_text)
        full_text = self._clean_pdf_text(full_text)
        
        return full_text, extracted_links
    
    def _extract_with_pdfplumber(self, pdf_content: bytes) -> tuple[str, List[str]]:
        """Extract PDF text using pdfplumber (if available)"""
        if not PDFPLUMBER_AVAILABLE:
            raise Exception("pdfplumber not available - install with: pip install pdfplumber")
        
        import pdfplumber
        pdf_file = io.BytesIO(pdf_content)
        
        text_content = []
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text and page_text.strip():
                    text_content.append(page_text)
        
        if not text_content:
            raise Exception("No text extracted")
        
        full_text = " ".join(text_content)
        extracted_links = self._extract_links_from_content(full_text)
        full_text = self._clean_pdf_text(full_text)
        
        return full_text, extracted_links
    
    def _extract_with_pymupdf(self, pdf_content: bytes) -> tuple[str, List[str]]:
        """Extract PDF text using PyMuPDF/fitz (if available)"""
        if not PYMUPDF_AVAILABLE:
            raise Exception("PyMuPDF not available - install with: pip install PyMuPDF")
        
        import fitz  # PyMuPDF
        
        text_content = []
        doc = fitz.open(stream=pdf_content, filetype="pdf")
        
        for page_num in range(doc.page_count):
            page = doc[page_num]
            page_text = page.get_text()
            if page_text and page_text.strip():
                text_content.append(page_text)
        
        doc.close()
        
        if not text_content:
            raise Exception("No text extracted")
        
        full_text = " ".join(text_content)
        extracted_links = self._extract_links_from_content(full_text)
        full_text = self._clean_pdf_text(full_text)
        
        return full_text, extracted_links
    
    def _clean_pdf_text(self, text: str) -> str:
        """Clean and format PDF text"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove unwanted characters but keep essential punctuation
        text = re.sub(r'[^\w\s\.\,\;\:\!\?\-\(\)\[\]\{\}\"\'\/\\\@\#\$\%\&\*\+\=\<\>\~\`]', '', text)
        
        # Remove multiple consecutive punctuation
        text = re.sub(r'([.!?]){2,}', r'\1', text)
        
        # Clean up spacing around punctuation
        text = re.sub(r'\s+([.!?,:;])', r'\1', text)
        text = re.sub(r'([.!?])\s*([A-Z])', r'\1 \2', text)
        
        # Final cleanup
        text = text.strip()
        
        # Remove APRA boilerplate
        text = self._remove_apra_boilerplate(text)
        
        return text
    
    def _extract_pdf_title_from_link(self, pdf_link) -> str:
        """Extract PDF title from link element with multiple fallback strategies"""
        try:
            # Strategy 1: Look for document-link__label span (most common)
            title_elem = pdf_link.find('span', class_='document-link__label')
            if title_elem:
                return title_elem.get_text(strip=True)
            
            # Strategy 2: Look for any span with a title-like class
            for span in pdf_link.find_all('span'):
                span_text = span.get_text(strip=True)
                if span_text and 'label' in (span.get('class', []) or []):
                    return span_text
            
            # Strategy 3: Get link text directly (excluding file type indicators)
            link_text = pdf_link.get_text(strip=True)
            # Remove common suffixes like "PDF", file sizes, etc.
            link_text = re.sub(r'\s*(PDF|pdf)\s*$', '', link_text)
            link_text = re.sub(r'\s*\d+(\.\d+)?\s*(KB|MB|kb|mb)\s*$', '', link_text)
            
            if link_text:
                return link_text
            
            # Strategy 4: Extract from href filename as last resort
            href = pdf_link.get('href', '')
            if href:
                filename = href.split('/')[-1]
                # Remove .pdf extension and clean up
                filename = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
                filename = filename.replace('%20', ' ').replace('_', ' ').replace('-', ' ')
                return filename
            
            return "Unknown PDF"
            
        except Exception as e:
            logger.warning(f"Error extracting PDF title: {e}")
            return "Unknown PDF"
    
    def _expand_year_sections(self):
        """Expand all year sections to reveal all information papers"""
        try:
            logger.info("🔍 Expanding year sections...")
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "accordion")))
            
            # Find all accordion buttons
            accordion_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".accordion__toggle")
            logger.info(f"Found {len(accordion_buttons)} year sections")
            
            expanded_count = 0
            for i, button in enumerate(accordion_buttons):
                try:
                    # Check if already expanded
                    is_expanded = button.get_attribute("aria-expanded") == "true"
                    
                    if not is_expanded:
                        # Scroll to button
                        self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", button)
                        time.sleep(1)
                        
                        # Click to expand
                        button.click()
                        expanded_count += 1
                        logger.debug(f"Expanded section {i+1}")
                        
                        # Wait a bit between clicks
                        time.sleep(random.uniform(0.5, 1.0))
                    else:
                        logger.debug(f"Section {i+1} already expanded")
                        
                except Exception as e:
                    logger.warning(f"Could not expand section {i+1}: {e}")
                    continue
            
            logger.info(f"✅ Expanded {expanded_count} year sections")
            
            # Wait for content to load
            time.sleep(3)
            return True
            
        except Exception as e:
            logger.error(f"Error expanding year sections: {e}")
            return False
    
    def _extract_information_papers(self) -> List[Dict]:
        """Extract all information papers from the page"""
        papers = []
        
        try:
            # Find all accordion content sections
            content_sections = self.driver.find_elements(By.CSS_SELECTOR, ".accordion__content")
            logger.info(f"Found {len(content_sections)} content sections")
            
            for section_idx, section in enumerate(content_sections):
                try:
                    # Find all elements that could contain information papers
                    # Look for both regular links (web pages) and embedded documents (PDFs)
                    
                    # 1. Regular paragraph links (web pages)
                    paragraphs = section.find_elements(By.TAG_NAME, "p")
                    logger.debug(f"Section {section_idx + 1}: Found {len(paragraphs)} paragraphs")
                    
                    for para_idx, paragraph in enumerate(paragraphs):
                        try:
                            paper_data = self._parse_information_paper(paragraph)
                            if paper_data:
                                papers.append(paper_data)
                                logger.info(f"📄 Found paper: {paper_data.get('title', 'Unknown')}")
                            
                        except Exception as e:
                            logger.warning(f"Error parsing paragraph {para_idx + 1} in section {section_idx + 1}: {e}")
                            continue
                    
                    # 2. Embedded document containers (PDFs)
                    embedded_docs = section.find_elements(By.CSS_SELECTOR, ".entity-embed-document-link-container, div[apra-media-type='document']")
                    logger.debug(f"Section {section_idx + 1}: Found {len(embedded_docs)} embedded documents")
                    
                    for doc_idx, doc_container in enumerate(embedded_docs):
                        try:
                            paper_data = self._parse_embedded_document(doc_container)
                            if paper_data:
                                papers.append(paper_data)
                                logger.info(f"📄 Found embedded PDF: {paper_data.get('title', 'Unknown')}")
                            
                        except Exception as e:
                            logger.warning(f"Error parsing embedded document {doc_idx + 1} in section {section_idx + 1}: {e}")
                            continue
                            
                except Exception as e:
                    logger.warning(f"Error processing section {section_idx + 1}: {e}")
                    continue
            
            logger.info(f"📊 Total papers extracted: {len(papers)}")
            return papers
            
        except Exception as e:
            logger.error(f"Error extracting information papers: {e}")
            return []
    
    def _parse_embedded_document(self, doc_container) -> Optional[Dict]:
        """Parse embedded PDF document from document container"""
        try:
            # Find the PDF link within the container
            pdf_link = doc_container.find_element(By.CSS_SELECTOR, "a.document-link, a[href*='.pdf']")
            if not pdf_link:
                return None
            
            # Extract title from the link
            title_span = pdf_link.find_element(By.CSS_SELECTOR, ".document-link__label")
            title = title_span.text.strip() if title_span else pdf_link.text.strip()
            
            if not title:
                return None
            
            # Get PDF URL
            pdf_url = pdf_link.get_attribute("href")
            if not pdf_url or not pdf_url.endswith('.pdf'):
                return None
            
            # Make URL absolute
            if not pdf_url.startswith('http'):
                pdf_url = urljoin(self.base_url, pdf_url)
            
            # Extract published date from caption or container
            published_date = ""
            try:
                caption = doc_container.find_element(By.CSS_SELECTOR, ".figure__caption")
                if caption:
                    caption_text = caption.text.strip()
                    date_match = re.search(r'Published\s+(.+)', caption_text)
                    if date_match:
                        published_date = date_match.group(1).strip()
            except:
                # Try to find date in surrounding text
                container_text = doc_container.text
                date_match = re.search(r'Published\s+(.+)', container_text)
                if date_match:
                    published_date = date_match.group(1).strip()
            
            # Check if date is after Jan 1, 2022
            if published_date and not self._is_after_jan_2022(published_date):
                logger.debug(f"Skipping PDF from before 2022: {title} ({published_date})")
                return None
            
            # Generate hash for deduplication
            hash_id = self._generate_hash(pdf_url, title, published_date)
            
            # Check if already exists
            if hash_id in self.existing_hashes:
                logger.debug(f"🔄 Skipping existing PDF: {title}")
                return None
            
            paper_data = {
                'hash_id': hash_id,
                'title': title,
                'url': pdf_url,  # For PDFs, the URL is the PDF URL
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'category': self._extract_category(title),
                'is_pdf': True,
                'pdf_url': pdf_url,
                'content': '',
                'pdf_content': '',
                'extracted_links': [],
                'image_url': ''
            }
            
            logger.info(f"🎯 Successfully parsed embedded PDF: {title} ({published_date})")
            return paper_data
            
        except Exception as e:
            logger.error(f"Error parsing embedded document: {e}")
            return None

    def _parse_information_paper(self, paragraph_element) -> Optional[Dict]:
        """Parse a single information paper from paragraph element (for web pages)"""
        try:
            # Look for links in the paragraph
            links = paragraph_element.find_elements(By.TAG_NAME, "a")
            if not links:
                return None
            
            # Get the main link (usually the first one that's not empty)
            main_link = None
            for link in links:
                link_text = link.text.strip()
                link_href = link.get_attribute("href") or ""
                
                # Skip if this is a PDF link (these are handled separately)
                if link_href.endswith('.pdf') or 'document-link' in (link.get_attribute("class") or ""):
                    continue
                    
                if link_text:
                    main_link = link
                    break
            
            if not main_link:
                return None
            
            title = main_link.text.strip()
            url = main_link.get_attribute("href")
            
            if not title or not url:
                return None
            
            # Skip if this is a PDF URL (handled by embedded document parser)
            if url.endswith('.pdf'):
                return None
            
            # Make URL absolute
            if not url.startswith('http'):
                url = urljoin(self.base_url, url)
            
            # Extract published date
            paragraph_text = paragraph_element.text
            published_date = ""
            
            # Look for "Published" followed by date
            date_match = re.search(r'Published\s+(.+)', paragraph_text)
            if date_match:
                published_date = date_match.group(1).strip()
            
            # Check if date is after Jan 1, 2022
            if published_date and not self._is_after_jan_2022(published_date):
                logger.debug(f"Skipping paper from before 2022: {title} ({published_date})")
                return None
            
            # Generate hash for deduplication
            hash_id = self._generate_hash(url, title, published_date)
            
            # Check if already exists
            if hash_id in self.existing_hashes:
                logger.debug(f"🔄 Skipping existing paper: {title}")
                return None
            
            # This is a web page (not PDF)
            paper_data = {
                'hash_id': hash_id,
                'title': title,
                'url': url,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'category': self._extract_category(title),
                'is_pdf': False,
                'pdf_url': '',
                'content': '',
                'pdf_content': '',
                'extracted_links': [],
                'image_url': ''
            }
            
            logger.info(f"🎯 Successfully parsed web page: {title} ({published_date})")
            return paper_data
            
        except Exception as e:
            logger.error(f"Error parsing information paper: {e}")
            return None
    
    def _extract_category(self, title: str) -> str:
        """Extract category from paper title"""
        title_lower = title.lower()
        
        # Common categories based on APRA's typical classifications
        categories = {
            'climate': ['climate', 'environmental', 'sustainability'],
            'capital': ['capital', 'cet1', 'tier 1', 'basel'],
            'governance': ['governance', 'accountability', 'far'],
            'superannuation': ['superannuation', 'super', 'retirement', 'pension'],
            'insurance': ['insurance', 'insurer', 'life insurance', 'general insurance'],
            'banking': ['banking', 'adi', 'credit', 'deposit', 'lending', 'licensing'],
            'methodology': ['methodology', 'approach', 'framework'],
            'risk': ['risk', 'stress', 'scenario'],
            'prudential': ['prudential', 'standard', 'requirement'],
            'data': ['data', 'reporting', 'collection']
        }
        
        for category, keywords in categories.items():
            if any(keyword in title_lower for keyword in keywords):
                return category.title()
        
        return 'General'
    
    def _extract_detailed_content(self, paper: Dict) -> Dict:
        """Extract detailed content from the paper page or PDF"""
        try:
            # Create a fresh copy to avoid reference issues
            paper_copy = paper.copy()
            
            if paper_copy['is_pdf'] and paper_copy['pdf_url']:
                # Extract from PDF
                logger.info(f"📄 Extracting PDF content for: {paper_copy['title']}")
                pdf_content, pdf_links = self._extract_pdf_text(paper_copy['pdf_url'])
                
                # Only set content for THIS paper
                paper_copy['pdf_content'] = pdf_content
                paper_copy['extracted_links'] = pdf_links
                paper_copy['content'] = pdf_content  # For PDFs, content is the same as pdf_content
                
                if pdf_content:
                    logger.info(f"✅ PDF content extracted: {len(pdf_content)} characters")
                else:
                    logger.warning(f"⚠️ No content extracted from PDF: {paper_copy['title']}")
                
            else:
                # Extract from web page
                logger.info(f"🌐 Extracting web content for: {paper_copy['title']}")
                content, links, image_url, embedded_pdf_content = self._extract_web_content(paper_copy['url'])
                
                # Only set content for THIS paper
                paper_copy['content'] = content
                paper_copy['extracted_links'] = links
                paper_copy['image_url'] = image_url
                paper_copy['pdf_content'] = embedded_pdf_content  # Include embedded PDF content
                
                if content or embedded_pdf_content:
                    total_chars = len(content) + len(embedded_pdf_content)
                    logger.info(f"✅ Web content extracted: {len(content)} chars, PDF: {len(embedded_pdf_content)} chars (Total: {total_chars})")
                else:
                    logger.warning(f"⚠️ No content extracted from web page: {paper_copy['title']}")
            
            return paper_copy
            
        except Exception as e:
            logger.error(f"Error extracting detailed content for {paper['title']}: {e}")
            # Return original paper with empty content fields to ensure no cross-contamination
            paper_safe = paper.copy()
            paper_safe['content'] = ''
            paper_safe['pdf_content'] = ''
            paper_safe['extracted_links'] = []
            paper_safe['image_url'] = ''
            return paper_safe
    
    def _extract_web_content(self, url: str) -> tuple[str, List[str], str, str]:
        """Extract content from a web page - ENHANCED VERSION with comprehensive embedded PDF detection"""
        try:
            logger.debug(f"🌐 Fetching web content from: {url}")
            
            # Use Selenium for better content extraction
            if self.driver:
                self.driver.get(url)
                time.sleep(3)  # Wait for dynamic content
                html = self.driver.page_source
            else:
                response = self.session.get(url, timeout=30)
                if response.status_code != 200:
                    logger.error(f"Failed to fetch web content: {url} (Status: {response.status_code})")
                    return "", [], "", ""
                html = response.text
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract main content with better selectors for APRA site
            content_text = ""
            image_url = ""
            embedded_pdf_content = ""
            
            # ENHANCED: Extract embedded PDFs from the ENTIRE page with comprehensive selectors
            # This covers all the different ways APRA embeds PDFs
            pdf_selectors = [
                # Primary selectors based on the HTML sample
                '.entity-embed-document-link-container a[href$=".pdf"]',
                '.entity-embed-document-link-container .document-link[href$=".pdf"]',
                
                # Alternative container structures
                '.document-link-container a[href$=".pdf"]',
                'div[apra-media-type="document"] a[href$=".pdf"]',
                'div[apra-media-type="document"] .document-link[href$=".pdf"]',
                
                # Direct document links
                'a.document-link[href$=".pdf"]',
                
                # Generic PDF links that might be embedded
                'a[href$=".pdf"]',
                
                # Field-specific selectors based on APRA's structure
                '.field-field-media-file a[href$=".pdf"]',
                '.field-type-file a[href$=".pdf"]',
                
                # Additional media embed patterns
                '[data-entity-type="media"] a[href$=".pdf"]',
                '.media-entity-embed a[href$=".pdf"]'
            ]
            
            embedded_pdfs = []
            pdf_titles_found = set()  # Track titles to avoid processing same PDF multiple times
            
            for selector in pdf_selectors:
                pdfs = soup.select(selector)  # Search entire page
                for pdf_link in pdfs:
                    href = pdf_link.get('href')
                    if not href:
                        continue
                        
                    # Get PDF title for deduplication
                    pdf_title = self._extract_pdf_title_from_link(pdf_link)
                    
                    # Skip if we've already found this PDF (by title and URL)
                    pdf_key = f"{href}|{pdf_title}"
                    if pdf_key not in pdf_titles_found:
                        embedded_pdfs.append((href, pdf_link, pdf_title))
                        pdf_titles_found.add(pdf_key)
                
                logger.debug(f"Found {len(pdfs)} PDFs with selector: {selector}")
            
            logger.info(f"Found {len(embedded_pdfs)} unique embedded PDFs on page")
            
            # Process each unique embedded PDF
            for pdf_url, pdf_link, pdf_title in embedded_pdfs:
                try:
                    # Make URL absolute
                    if not pdf_url.startswith('http'):
                        pdf_url = urljoin(url, pdf_url)
                    
                    logger.info(f"📄 Extracting embedded PDF: {pdf_title}")
                    logger.debug(f"📄 PDF URL: {pdf_url}")
                    
                    # Add timeout and retry logic for PDF extraction
                    max_retries = 2
                    retry_count = 0
                    pdf_text = ""
                    
                    while retry_count < max_retries and not pdf_text:
                        try:
                            pdf_text, _ = self._extract_pdf_text(pdf_url)
                            if pdf_text:
                                break
                        except Exception as retry_e:
                            retry_count += 1
                            logger.warning(f"PDF extraction attempt {retry_count} failed: {retry_e}")
                            if retry_count < max_retries:
                                time.sleep(2)  # Wait before retry
                    
                    if pdf_text:
                        embedded_pdf_content += f"\n\n--- Embedded PDF: {pdf_title} ---\n{pdf_text}"
                        logger.info(f"✅ Successfully extracted {len(pdf_text)} characters from embedded PDF")
                    else:
                        logger.warning(f"⚠️ No content extracted from embedded PDF after {max_retries} attempts: {pdf_title}")
                        # Continue processing other PDFs instead of failing completely
                        
                except Exception as e:
                    logger.error(f"Error extracting embedded PDF {pdf_url}: {e}")
                    # Continue with next PDF instead of failing completely
                    continue
            
            # Look for main content areas - APRA specific selectors
            main_content = (
                soup.find('div', class_='page__sections') or
                soup.find('article') or 
                soup.find('main') or 
                soup.find('div', class_='section__content')
            )
            
            if main_content:
                # Remove script, style, navigation elements
                for unwanted in main_content(['script', 'style', 'nav', 'header', 'footer', 'breadcrumb']):
                    unwanted.decompose()
                
                # Remove APRA-specific navigation elements
                nav_selectors = ['.page__toolbar', '.breadcrumb', '[class*="nav"]', '[class*="menu"]']
                for nav_selector in nav_selectors:
                    for nav_elem in main_content.select(nav_selector):
                        nav_elem.decompose()
                
                # Extract text content with better structure preservation
                content_parts = []
                
                # Get all text elements and preserve structure
                for elem in main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'blockquote', 'div']):
                    # Skip embedded document containers to avoid duplicating PDF titles
                    if any(class_name in (elem.get('class', []) or []) for class_name in 
                           ['entity-embed-document-link-container', 'document-link-container', 'figure__caption']):
                        continue
                        
                    if elem.name == 'li':
                        # Handle list items
                        li_text = elem.get_text(strip=True)
                        if li_text and len(li_text) > 5:
                            content_parts.append(f"• {li_text}")
                    elif elem.name in ['ul', 'ol']:
                        # Skip the list container itself, we handle li items
                        continue
                    elif elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        # Handle headers
                        header_text = elem.get_text(strip=True)
                        if header_text and header_text.lower() not in ['footnotes', 'media enquiries', 'all other enquiries']:
                            content_parts.append(f"\n{header_text}\n")
                    elif elem.name == 'div':
                        # Only process divs that have substantial text and aren't containers
                        div_text = elem.get_text(strip=True)
                        if (div_text and 
                            len(div_text) > 50 and 
                            not elem.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']) and  # Not a container
                            not any(skip_class in (elem.get('class', []) or []) for skip_class in ['nav', 'menu', 'footer', 'header', 'entity-embed-document-link-container'])):
                            content_parts.append(div_text)
                    else:
                        # Handle paragraphs and other elements
                        elem_text = elem.get_text(strip=True)
                        if elem_text and len(elem_text) > 10:
                            content_parts.append(elem_text)
                
                # Join content with proper spacing
                if content_parts:
                    content_text = '\n\n'.join(content_parts)
                else:
                    # Fallback to full text extraction
                    content_text = main_content.get_text(separator=' ', strip=True)
                
                # Clean content text
                content_text = re.sub(r'\s+', ' ', content_text)
                content_text = self._remove_apra_boilerplate(content_text)
                
                # Look for images
                img_tag = main_content.find('img', src=True)
                if img_tag and img_tag.get('src'):
                    image_url = img_tag.get('src')
                    if not image_url.startswith('http'):
                        image_url = urljoin(url, image_url)
            else:
                logger.warning(f"No main content area found for {url}")
            
            # Extract all links from the content
            extracted_links = self._extract_links_from_content(content_text + embedded_pdf_content)
            
            # Also extract href links from the page
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('http'):
                    extracted_links.append(href)
                elif href.startswith('/'):
                    extracted_links.append(urljoin(self.base_url, href))
            
            # Remove duplicates
            extracted_links = list(set(extracted_links))
            
            logger.debug(f"Extracted {len(content_text)} web chars, {len(embedded_pdf_content)} PDF chars, {len(extracted_links)} links")
            return content_text, extracted_links, image_url, embedded_pdf_content
            
        except Exception as e:
            logger.error(f"Error extracting web content from {url}: {e}")
            return "", [], "", ""
    
    def scrape_information_papers(self) -> List[Dict]:
        """Main scraping method"""
        logger.info("="*80)
        logger.info("🚀 Starting APRA Information Papers scraping")
        logger.info("📅 Filter: Only papers published after January 1, 2022")
        logger.info("="*80)
        
        if not self._setup_driver():
            return []
        
        try:
            # Establish session first
            if not self._establish_session():
                logger.error("Failed to establish session")
                return []
            
            # Navigate to target page
            logger.info(f"📄 Navigating to: {self.target_url}")
            self.driver.get(self.target_url)
            time.sleep(random.uniform(3, 5))
            
            # Expand all year sections
            if not self._expand_year_sections():
                logger.error("Failed to expand year sections")
                return []
            
            # Extract all information papers
            papers = self._extract_information_papers()
            
            if not papers:
                logger.info("No new information papers found")
                return []
            
            logger.info(f"Found {len(papers)} new information papers")
            logger.info("="*80)
            logger.info("📄 Processing detailed content extraction...")
            logger.info("="*80)
            
            # Extract detailed content for each paper
            detailed_papers = []
            for i, paper in enumerate(papers):
                try:
                    logger.info(f"Processing paper {i+1}/{len(papers)}: {paper['title']}")
                    detailed_paper = self._extract_detailed_content(paper)
                    detailed_papers.append(detailed_paper)
                    
                    # Add delay between requests to be respectful
                    time.sleep(random.uniform(1, 3))  # Reduced from 2-4 seconds
                    
                except Exception as e:
                    logger.error(f"Error processing paper {i+1}: {e}")
                    # Add the paper without detailed content rather than failing completely
                    paper_safe = paper.copy()
                    paper_safe['content'] = ''
                    paper_safe['pdf_content'] = ''
                    paper_safe['extracted_links'] = []
                    paper_safe['image_url'] = ''
                    detailed_papers.append(paper_safe)
                    continue
            
            logger.info("="*80)
            logger.info(f"✅ Successfully processed {len(detailed_papers)} information papers")
            logger.info("="*80)
            
            return detailed_papers
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔒 Browser closed")

    def scrape_individual_paper(self, url: str) -> Optional[Dict]:
        """Scrape a single information/consultation paper from a specific URL"""
        logger.info("="*80)
        logger.info(f"🚀 Scraping individual paper from: {url}")
        logger.info("="*80)
        
        if not self._setup_driver():
            return None
        
        try:
            # Navigate directly to the paper URL
            logger.info(f"📄 Navigating to: {url}")
            self.driver.get(url)
            time.sleep(3)
            
            # Extract content from the page
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract title from h1
            title_elem = soup.find('h1')
            title = title_elem.get_text(strip=True) if title_elem else "Unknown Title"
            
            # Extract published date - look for common APRA date patterns
            published_date = ""
            
            # Try to find date in embedded document captions first
            caption_elem = soup.find('div', class_='figure__caption')
            if caption_elem:
                caption_text = caption_elem.get_text(strip=True)
                date_match = re.search(r'Published\s+(.+)', caption_text)
                if date_match:
                    published_date = date_match.group(1).strip()
            
            # If no date found, try meta tags or other locations
            if not published_date:
                # Try meta tags
                date_meta = soup.find('meta', {'property': 'article:published_time'}) or soup.find('meta', {'name': 'date'})
                if date_meta:
                    published_date = date_meta.get('content', '')
                
                # Try time elements
                if not published_date:
                    time_elem = soup.find('time')
                    if time_elem:
                        published_date = time_elem.get_text(strip=True)
            
            if not published_date:
                published_date = "Unknown"
            
            # Check if date is after Jan 1, 2022
            if published_date != "Unknown" and not self._is_after_jan_2022(published_date):
                logger.info(f"Paper from before 2022: {title} ({published_date}) - skipping")
                return None
            
            # Generate hash for deduplication
            hash_id = self._generate_hash(url, title, published_date)
            
            # Check if already exists
            if hash_id in self.existing_hashes:
                logger.info(f"🔄 Paper already exists: {title}")
                return None
            
            # Create paper data structure
            paper_data = {
                'hash_id': hash_id,
                'title': title,
                'url': url,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'category': self._extract_category(title),
                'is_pdf': False,  # This is a web page with embedded PDF
                'pdf_url': '',
                'content': '',
                'pdf_content': '',
                'extracted_links': [],
                'image_url': ''
            }
            
            logger.info(f"🎯 Successfully parsed individual paper: {title} ({published_date})")
            
            # Extract detailed content
            detailed_paper = self._extract_detailed_content(paper_data)
            
            logger.info("="*80)
            if detailed_paper.get('content') or detailed_paper.get('pdf_content'):
                total_chars = len(detailed_paper.get('content', '')) + len(detailed_paper.get('pdf_content', ''))
                logger.info(f"✅ Successfully extracted content: {total_chars} total characters")
                logger.info(f"   Web content: {len(detailed_paper.get('content', ''))} chars")
                logger.info(f"   PDF content: {len(detailed_paper.get('pdf_content', ''))} chars")
            else:
                logger.warning(f"⚠️ No content extracted from: {url}")
            logger.info("="*80)
            
            return detailed_paper
            
        except Exception as e:
            logger.error(f"Error scraping individual paper from {url}: {e}")
            return None
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔒 Browser closed")
    
    def _load_existing_papers(self) -> List[Dict]:
        """Load existing papers from JSON file"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading existing papers: {e}")
        return []
    
    def save_papers(self, new_papers: List[Dict]):
        """Save papers to JSON and CSV files with intelligent deduplication"""
        if not new_papers:
            logger.info("No new papers to save")
            return
        
        # Load existing data
        existing_papers = self._load_existing_papers()
        
        # Create a set of existing hash_ids for faster lookup
        existing_hash_ids = {paper.get('hash_id', '') for paper in existing_papers}
        
        # Filter out truly new papers
        actually_new_papers = []
        for paper in new_papers:
            if paper.get('hash_id', '') not in existing_hash_ids:
                actually_new_papers.append(paper)
            else:
                logger.debug(f"Skipping duplicate: {paper.get('title', 'Unknown')}")
        
        if not actually_new_papers:
            logger.info("No genuinely new papers found - all were duplicates")
            return
        
        # Merge new papers with existing
        all_papers = existing_papers + actually_new_papers
        
        # Sort by published_date (most recent first)
        try:
            all_papers.sort(key=lambda x: self._parse_date(x.get('published_date', '1 January 2000')) or datetime(2000, 1, 1), reverse=True)
        except Exception as e:
            logger.warning(f"Could not sort by date: {e}")
        
        # Remove any legacy fields
        for paper in all_papers:
            paper.pop('content_html', None)
        
        # Save to JSON
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(all_papers, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved {len(all_papers)} total papers to {self.json_file}")
            logger.info(f"➕ Added {len(actually_new_papers)} new papers")
        except Exception as e:
            logger.error(f"Error saving JSON file: {e}")
        
        # Save to CSV
        if all_papers:
            try:
                fieldnames = all_papers[0].keys()
                with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(all_papers)
                logger.info(f"💾 Saved {len(all_papers)} total papers to {self.csv_file}")
            except Exception as e:
                logger.error(f"Error saving CSV file: {e}")
        
        # Log summary of new additions
        if actually_new_papers:
            logger.info("="*80)
            logger.info("📋 NEW INFORMATION PAPERS ADDED:")
            for paper in actually_new_papers:
                content_info = ""
                if paper.get('content') and paper.get('pdf_content'):
                    content_info = f" (Web: {len(paper['content'])} chars, PDF: {len(paper['pdf_content'])} chars)"
                elif paper.get('content'):
                    content_info = f" ({len(paper['content'])} chars)"
                elif paper.get('pdf_content'):
                    content_info = f" (PDF: {len(paper['pdf_content'])} chars)"
                
                logger.info(f"   • {paper.get('category', 'Unknown')}: {paper.get('title', 'No title')} ({paper.get('published_date', 'No date')}){content_info}")
            logger.info("="*80)

def main():
    """Main execution function with enhanced error handling and graceful termination"""
    scraper = None
    
    try:
        scraper = APRAInfoPapersScraper()
        
        logger.info("="*80)
        logger.info("🚀 APRA INFORMATION PAPERS SCRAPER STARTED")
        logger.info("="*80)
        
        new_papers = scraper.scrape_information_papers()
        
        if new_papers:
            logger.info(f"✅ Successfully scraped {len(new_papers)} information papers from 2022+")
            scraper.save_papers(new_papers)
            
            # Summary statistics
            categories = {}
            pdf_count = 0
            web_count = 0
            papers_with_content = 0
            papers_with_pdf_content = 0
            
            for paper in new_papers:
                category = paper.get('category', 'Unknown')
                categories[category] = categories.get(category, 0) + 1
                
                if paper.get('is_pdf'):
                    pdf_count += 1
                else:
                    web_count += 1
                
                if paper.get('content', '').strip():
                    papers_with_content += 1
                    
                if paper.get('pdf_content', '').strip():
                    papers_with_pdf_content += 1
            
            logger.info("="*80)
            logger.info("📊 SCRAPING SUMMARY:")
            logger.info(f"   Total information papers found: {len(new_papers)}")
            logger.info(f"   PDF papers: {pdf_count}")
            logger.info(f"   Web papers: {web_count}")
            logger.info("   Categories:")
            for category, count in categories.items():
                logger.info(f"     - {category}: {count}")
            
            # Content extraction summary
            papers_with_links = sum(1 for paper in new_papers if paper.get('extracted_links'))
            
            logger.info(f"   Papers with web content: {papers_with_content}")
            logger.info(f"   Papers with PDF content: {papers_with_pdf_content}")
            logger.info(f"   Papers with extracted links: {papers_with_links}")
            
            # PDF extraction success rate
            if pdf_count > 0 or papers_with_pdf_content > 0:
                total_pdfs = pdf_count + sum(1 for paper in new_papers if paper.get('pdf_content'))
                successful_extractions = papers_with_pdf_content
                success_rate = (successful_extractions / total_pdfs * 100) if total_pdfs > 0 else 0
                logger.info(f"   PDF extraction success rate: {success_rate:.1f}%")
            
            logger.info("="*80)
            
        else:
            logger.info("ℹ️  No new information papers found (all may be existing or before 2022)")
            
    except KeyboardInterrupt:
        logger.info("🛑 Scraping interrupted by user")
        logger.info("💾 Attempting to save any collected data...")
        
        # Try to save any partial data if available
        if scraper and hasattr(scraper, '_partial_papers') and scraper._partial_papers:
            try:
                scraper.save_papers(scraper._partial_papers)
                logger.info(f"💾 Saved {len(scraper._partial_papers)} papers collected before interruption")
            except Exception as save_e:
                logger.error(f"Could not save partial data: {save_e}")
        
        logger.info("👋 Graceful shutdown complete")
        
    except ImportError as e:
        logger.error(f"❌ Missing required dependencies: {e}")
        logger.error("Please install missing libraries and try again")
        
    except Exception as e:
        logger.error(f"💥 Scraping failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Try to save any partial data
        if scraper and hasattr(scraper, '_partial_papers') and scraper._partial_papers:
            try:
                scraper.save_papers(scraper._partial_papers)
                logger.info(f"💾 Saved {len(scraper._partial_papers)} papers before error")
            except Exception as save_e:
                logger.error(f"Could not save partial data: {save_e}")
        
        raise
    
    finally:
        # Ensure cleanup
        if scraper and hasattr(scraper, 'driver') and scraper.driver:
            try:
                scraper.driver.quit()
                logger.info("🔒 Browser cleanup completed")
            except Exception as cleanup_e:
                logger.warning(f"Browser cleanup warning: {cleanup_e}")

def test_pdf_extraction():
    """Test PDF extraction on a known working PDF"""
    logger.info("🧪 Testing PDF extraction capability...")
    
    test_pdf_url = "https://www.apra.gov.au/sites/default/files/2025-07/Discussion%20paper%20-%20Improving%20the%20licensing%20framework%20for%20authorised%20deposit-taking%20institutions.pdf"
    
    try:
        scraper = APRAInfoPapersScraper()
        pdf_content = scraper._download_pdf_with_requests(test_pdf_url)
        
        if pdf_content and pdf_content.startswith(b'%PDF'):
            # Try text extraction
            text_content, links = scraper._extract_with_pypdf2(pdf_content)
            
            if len(text_content) > 1000:
                logger.info(f"✅ PDF extraction test PASSED: {len(text_content)} characters extracted")
                return True
            else:
                logger.warning(f"⚠️ PDF extraction test partial: only {len(text_content)} characters")
                return False
        else:
            logger.error("❌ PDF extraction test FAILED: Could not download valid PDF")
            return False
            
    except Exception as e:
        logger.error(f"❌ PDF extraction test ERROR: {e}")
        return False

if __name__ == "__main__":
    # Check if this is a test run
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--test-pdf':
        print("🧪 Running PDF extraction test...")
        success = test_pdf_extraction()
        if success:
            print("✅ PDF extraction test passed! Ready to run full scraper.")
        else:
            print("❌ PDF extraction test failed. Check dependencies.")
            print("💡 Make sure you have installed: pip install brotli")
        sys.exit(0 if success else 1)
    else:
        main()