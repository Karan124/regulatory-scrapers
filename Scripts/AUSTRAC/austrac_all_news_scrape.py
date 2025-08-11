#!/usr/bin/env python3
"""
Enhanced AUSTRAC Updates Scraper
- Improved content extraction with better error handling
- PDF content extraction and processing
- LLM-friendly content formatting
- Daily vs Initial run differentiation
- Enhanced robustness and logging
- FIXED CHROME DRIVER SETUP
"""

import os
import json
import time
import hashlib
import logging
import re
import random
import requests
import io
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from bs4 import BeautifulSoup
import pandas as pd
import signal
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse
import PyPDF2
from io import BytesIO

# Try importing pdfplumber
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    print("Warning: pdfplumber not available. Install with: pip install pdfplumber")

# ----------------------------
# Enhanced Logging Setup
# ----------------------------
os.makedirs('data', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(funcName)s:%(lineno)d]: %(message)s',
    handlers=[
        logging.FileHandler('data/austrac_scraper.log', mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ----------------------------
# Configuration
# ----------------------------
DATA_DIR = Path("data")
JSON_PATH = DATA_DIR / "austrac_updates.json"
CSV_PATH = DATA_DIR / "austrac_updates.csv"
CHECKPOINT_PATH = DATA_DIR / "scraper_checkpoint.json"
PDF_CACHE_DIR = DATA_DIR / "pdf_cache"

BASE_URL = "https://www.austrac.gov.au"
TARGET_URL = f"{BASE_URL}/business/updates"
PAGE_LOAD_TIMEOUT = 30
ARTICLE_TIMEOUT = 20
DELAY_BETWEEN_REQUESTS = 3
MAX_RETRIES = 3
MAX_PAGES_INITIAL = 10  # For initial runs
MAX_PAGES_DAILY = 3     # For daily runs

# Create PDF cache directory
PDF_CACHE_DIR.mkdir(exist_ok=True)

# Global variables for graceful shutdown
shutdown_requested = False

# ----------------------------
# Signal Handler for Graceful Shutdown
# ----------------------------
def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class EnhancedAUSTRACScraper:
    """
    Enhanced AUSTRAC Updates scraper with PDF support and LLM-friendly content formatting
    FIXED CHROME DRIVER SETUP
    """
    
    def __init__(self, run_type: str = "daily"):
        self.base_url = BASE_URL
        self.target_url = TARGET_URL
        self.data_folder = DATA_DIR
        self.data_folder.mkdir(exist_ok=True)
        
        # Determine run type
        self.run_type = run_type.lower()  # "daily" or "initial"
        self.max_pages = MAX_PAGES_DAILY if self.run_type == "daily" else MAX_PAGES_INITIAL
        
        # File paths
        self.json_file = JSON_PATH
        self.csv_file = CSV_PATH
        self.checkpoint_file = CHECKPOINT_PATH
        self.pdf_cache_dir = PDF_CACHE_DIR
        
        self.driver = None
        self.existing_hashes = set()
        self.session = requests.Session()
        
        # Configure requests session
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        # Load existing data for deduplication
        self._load_existing_data()
        
        logger.info(f"🚀 Initialized scraper for {self.run_type.upper()} run (max {self.max_pages} pages)")
    
    def _load_existing_data(self):
        """Load existing articles to prevent duplicates"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Handle both old format (direct list) and new format (with metadata wrapper)
                    if isinstance(data, list):
                        # Old format: direct list of articles
                        existing_data = data
                    elif isinstance(data, dict) and 'articles' in data:
                        # New format: metadata wrapper with articles key
                        existing_data = data['articles']
                        if not isinstance(existing_data, list):
                            logger.warning("Articles key does not contain a list")
                            existing_data = []
                    else:
                        logger.warning("JSON file format not recognized, expected list or dict with 'articles' key")
                        existing_data = []
                    
                    self.existing_hashes = {item.get('hash_id', '') for item in existing_data if isinstance(item, dict)}
                    logger.info(f"Loaded {len(self.existing_hashes)} existing article records")
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                self.existing_hashes = set()
        else:
            logger.info("No existing data file found, starting fresh")
    
    def _should_continue_scraping(self) -> bool:
        """Determine if scraping should continue based on run type and recent articles"""
        if self.run_type == "initial":
            return True  # Always continue for initial runs
        
        # For daily runs, check if we've found recent articles
        if hasattr(self, '_recent_articles_found') and self._recent_articles_found > 5:
            return True
        
        # Stop if we've gone through enough pages without recent content
        if hasattr(self, '_pages_without_recent') and self._pages_without_recent > 2:
            logger.info("Stopping daily run - no recent articles found in recent pages")
            return False
        
        return True
    
    def _is_recent_article(self, published_date: str) -> bool:
        """Check if article is recent (within last 7 days for daily runs)"""
        if self.run_type == "initial":
            return True
        
        try:
            # Parse the date
            parsed_date = self._parse_date(published_date)
            if not parsed_date:
                return True  # If we can't parse, assume it's recent
            
            # Check if within last 7 days
            cutoff_date = datetime.now() - timedelta(days=7)
            return parsed_date >= cutoff_date
        except:
            return True  # Default to including if unsure
    
    def _generate_hash(self, url: str, headline: str, published_date: str) -> str:
        """Generate unique hash for article"""
        content = f"{url}_{headline}_{published_date}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _setup_driver(self):
        """Simplified Chrome WebDriver setup - let system handle Chrome detection."""
        chrome_options = Options()
        
        # Essential stability options for Linux
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Updated user agent to match current Chrome version
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
        
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
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")
        
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
            
            # Initialize driver with simplified service configuration
            service_kwargs = {}
            if chromedriver_path:
                service_kwargs['executable_path'] = chromedriver_path
            
            service = Service(**service_kwargs)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Chrome driver initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            return False
    
    def _wait_for_page_load(self, timeout=PAGE_LOAD_TIMEOUT):
        """Wait for page to fully load"""
        try:
            # Wait for basic page structure
            wait = WebDriverWait(self.driver, timeout)
            wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
            
            # Additional wait for dynamic content
            time.sleep(2)
            return True
            
        except TimeoutException:
            logger.warning(f"Page load timeout after {timeout} seconds")
            return False
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats from AUSTRAC website"""
        try:
            # Clean the date string
            date_str = date_str.strip()
            
            # Try multiple date formats
            date_formats = [
                "%d %B %Y",      # 3 June 2025
                "%d %b %Y",      # 3 Jun 2025  
                "%B %d, %Y",     # June 3, 2025
                "%b %d, %Y",     # Jun 3, 2025
                "%Y-%m-%d",      # 2025-06-03
                "%d/%m/%Y",      # 03/06/2025
                "%m/%d/%Y"       # 06/03/2025
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing date '{date_str}': {e}")
            return None
    
    def _extract_pdf_content(self, pdf_url: str) -> str:
        """Extract text content from PDF"""
        try:
            logger.info(f"📄 Extracting PDF content from: {pdf_url}")
            
            # Create cache filename
            pdf_filename = hashlib.md5(pdf_url.encode()).hexdigest() + ".pdf"
            cached_pdf_path = self.pdf_cache_dir / pdf_filename
            cached_text_path = self.pdf_cache_dir / (pdf_filename + ".txt")
            
            # Check if we have cached text
            if cached_text_path.exists():
                logger.info("📄 Using cached PDF content")
                with open(cached_text_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            # Download PDF if not cached
            if not cached_pdf_path.exists():
                response = self.session.get(pdf_url, timeout=30)
                response.raise_for_status()
                
                with open(cached_pdf_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"📄 Downloaded PDF: {pdf_filename}")
            
            # Extract text using pdfplumber if available
            text_content = ""
            if HAS_PDFPLUMBER:
                try:
                    with pdfplumber.open(cached_pdf_path) as pdf:
                        for page_num, page in enumerate(pdf.pages):
                            try:
                                page_text = page.extract_text()
                                if page_text:
                                    text_content += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
                            except Exception as e:
                                logger.warning(f"Error extracting page {page_num + 1}: {e}")
                                continue
                except Exception as e:
                    logger.warning(f"pdfplumber failed: {e}")
                    text_content = ""
            
            # Fallback to PyPDF2 if pdfplumber failed or not available
            if not text_content:
                try:
                    with open(cached_pdf_path, 'rb') as f:
                        pdf_reader = PyPDF2.PdfReader(f)
                        for page_num, page in enumerate(pdf_reader.pages):
                            try:
                                page_text = page.extract_text()
                                if page_text:
                                    text_content += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
                            except Exception as e:
                                logger.warning(f"Error extracting page {page_num + 1} with PyPDF2: {e}")
                                continue
                except Exception as e:
                    logger.error(f"PyPDF2 extraction failed: {e}")
                    return ""
            
            # Clean and format the text
            if text_content:
                text_content = self._clean_pdf_text(text_content)
                
                # Cache the extracted text
                with open(cached_text_path, 'w', encoding='utf-8') as f:
                    f.write(text_content)
                
                logger.info(f"📄 Successfully extracted {len(text_content)} characters from PDF")
                return text_content
            else:
                logger.warning("📄 No text content extracted from PDF")
                return ""
                
        except Exception as e:
            logger.error(f"Error extracting PDF content: {e}")
            return ""
    
    def _clean_pdf_text(self, text: str) -> str:
        """Clean and format PDF text for LLM consumption"""
        if not text:
            return ""
        
        # Remove excessive whitespace and normalize line breaks
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Multiple empty lines to double
        text = re.sub(r'\r\n', '\n', text)  # Windows line endings
        text = re.sub(r'\r', '\n', text)  # Mac line endings
        
        # Remove page headers/footers that are repeated
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines, page markers, and common PDF artifacts
            if not line:
                continue
            if line.startswith('--- Page '):
                continue
            if re.match(r'^Page \d+ of \d+$', line):
                continue
            if re.match(r'^\d+$', line) and len(line) <= 3:  # Standalone page numbers
                continue
            
            # Remove extra spaces
            line = re.sub(r'\s+', ' ', line)
            cleaned_lines.append(line)
        
        # Join lines and clean up formatting
        cleaned_text = '\n'.join(cleaned_lines)
        
        # Fix common PDF extraction issues
        cleaned_text = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_text)  # Missing spaces
        cleaned_text = re.sub(r'(\w)\n(\w)', r'\1 \2', cleaned_text)  # Join broken words
        
        return cleaned_text.strip()
    
    def _find_pdf_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find all PDF links on the page"""
        pdf_links = []
        
        # Look for direct PDF links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            
            if href.lower().endswith('.pdf'):
                # Make URL absolute
                if href.startswith('/'):
                    pdf_url = urljoin(base_url, href)
                elif not href.startswith('http'):
                    pdf_url = urljoin(base_url, href)
                else:
                    pdf_url = href
                
                pdf_links.append(pdf_url)
                logger.info(f"📎 Found PDF link: {pdf_url}")
        
        return pdf_links
    
    def _format_content_for_llm(self, article_content: str, pdf_content: str = "") -> str:
        """Format content to be LLM-friendly"""
        formatted_content = ""
        
        # Add main article content
        if article_content and article_content.strip():
            formatted_content += "ARTICLE CONTENT:\n"
            formatted_content += "=" * 50 + "\n"
            formatted_content += article_content.strip() + "\n\n"
        
        # Add PDF content if available
        if pdf_content and pdf_content.strip():
            formatted_content += "ATTACHED DOCUMENT CONTENT:\n"
            formatted_content += "=" * 50 + "\n"
            formatted_content += pdf_content.strip() + "\n\n"
        
        # If no content found
        if not formatted_content.strip():
            return "No content could be extracted from this article."
        
        return formatted_content.strip()
    
    def _extract_structured_content(self, soup: BeautifulSoup) -> str:
        """Extract structured content using improved selectors"""
        content = ""
        
        # Remove unwanted elements first
        for unwanted in soup.select('nav, .navigation, .breadcrumb, .share, .tags, .social-share, script, style, .skip-link, .visually-hidden'):
            unwanted.decompose()
        
        # Primary content selectors (ordered by specificity)
        content_selectors = [
            '.body-copy',  # AUSTRAC specific
            '.field--name-body',  # Drupal body field
            '.field__item',  # Drupal field items
            '.block-layout-builder--field--name-body',  # Layout builder body
            '.page-layout__content',  # Page layout content
            '.article-content', 
            '.main-content',
            'article .content',
            '.entry-content',
            'main',
            '.content'
        ]
        
        # Try to find content using selectors
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Clean up the content element
                for unwanted in content_elem.select('.field--label, .visually-hidden'):
                    unwanted.decompose()
                
                content = content_elem.get_text(separator='\n', strip=True)
                if content and len(content) > 100:  # Ensure substantial content
                    logger.info(f"✅ Content extracted using selector: {selector}")
                    break
        
        # Fallback: try to extract from specific AUSTRAC structure
        if not content or len(content) < 100:
            # Look for the specific structure
            title_elem = soup.select_one('h1.au-header-heading, .field--name-title h1')
            body_elem = soup.select_one('.field--name-body .field__item, .body-copy .field__item')
            
            if title_elem and body_elem:
                title = title_elem.get_text(strip=True)
                body = body_elem.get_text(separator='\n', strip=True)
                content = f"{title}\n\n{body}"
                logger.info("✅ Content extracted using fallback AUSTRAC structure")
        
        # Ultimate fallback: extract from main or body
        if not content or len(content) < 50:
            main_elem = soup.select_one('main, body')
            if main_elem:
                # Remove known non-content areas
                for unwanted in main_elem.select('header, footer, nav, .navigation, .sidebar, .menu'):
                    unwanted.decompose()
                
                content = main_elem.get_text(separator='\n', strip=True)
                logger.info("⚠️ Content extracted using ultimate fallback")
        
        return self._clean_text(content)
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove excessive whitespace and normalize
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Multiple empty lines
        text = re.sub(r'\s+', ' ', text)  # Multiple spaces
        text = re.sub(r'\n ', '\n', text)  # Space at beginning of lines
        text = re.sub(r' \n', '\n', text)  # Space at end of lines
        
        # Remove common artifacts
        text = re.sub(r'(?i)skip to main content', '', text)
        text = re.sub(r'(?i)breadcrumb.*?(?=\n)', '', text)
        
        # Clean up spacing around punctuation
        text = re.sub(r'\s+([.!?,:;])', r'\1', text)
        text = re.sub(r'([.!?])\s*([A-Z])', r'\1 \2', text)
        
        return text.strip()
    
    def _extract_article_content(self, article: Dict) -> Dict:
        """Extract full content from individual article page including PDFs"""
        try:
            logger.info(f"🌐 Extracting content for: {article['headline'][:100]}...")
            
            # Navigate to article URL
            self.driver.get(article['url'])
            
            if not self._wait_for_page_load():
                logger.warning(f"Page failed to load: {article['url']}")
                article['content'] = "Page failed to load"
                return article
            
            time.sleep(random.uniform(1, 2))  # Random delay
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract main content using improved method
            article_content = self._extract_structured_content(soup)
            
            # Extract PDF content
            pdf_content = ""
            pdf_links = self._find_pdf_links(soup, article['url'])
            
            if pdf_links:
                logger.info(f"📎 Found {len(pdf_links)} PDF(s) to process")
                pdf_texts = []
                
                for pdf_url in pdf_links:
                    pdf_text = self._extract_pdf_content(pdf_url)
                    if pdf_text:
                        pdf_texts.append(pdf_text)
                
                if pdf_texts:
                    pdf_content = "\n\n" + "="*50 + " PDF DOCUMENTS " + "="*50 + "\n\n"
                    pdf_content += "\n\n".join(pdf_texts)
            
            # Format content for LLM
            formatted_content = self._format_content_for_llm(article_content, pdf_content)
            
            # Extract related links from all content
            all_content = article_content + pdf_content
            related_links = self._extract_links_from_content(all_content)
            
            # Update article with extracted content
            article['content'] = formatted_content
            article['related_links'] = related_links
            article['pdf_links'] = pdf_links
            
            # Log results
            if formatted_content and len(formatted_content) > 100:
                logger.info(f"✅ Content extracted: {len(formatted_content)} characters")
                if pdf_content:
                    logger.info(f"📎 PDF content included: {len(pdf_content)} characters")
            else:
                logger.warning(f"⚠️ Minimal content extracted for: {article['headline'][:50]}...")
            
            return article
            
        except Exception as e:
            logger.error(f"Error extracting content for {article['url']}: {e}")
            article['content'] = f"Error extracting content: {str(e)}"
            return article
    
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
    
    def _extract_category(self, headline: str) -> str:
        """Extract category from article headline"""
        headline_lower = headline.lower()
        
        # Enhanced categories based on AUSTRAC's classifications
        categories = {
            'enforcement': ['penalty', 'infringement', 'compliance', 'audit', 'breach', 'violation', 'civil penalty', 'enforcement action'],
            'regulation': ['regulation', 'requirement', 'obligation', 'rule', 'standard', 'legislative', 'act amendment'],
            'guidance': ['guidance', 'update', 'information', 'clarification', 'advisory', 'notice'],
            'partnership': ['partnership', 'alliance', 'cooperation', 'joint', 'collaboration', 'mou'],
            'technology': ['crypto', 'digital', 'technology', 'fintech', 'blockchain', 'cryptocurrency', 'atm'],
            'industry': ['bank', 'casino', 'remitter', 'exchange', 'financial', 'gaming', 'betting'],
            'reform': ['reform', 'amendment', 'change', 'new law', 'legislative change'],
            'intelligence': ['intelligence', 'report', 'analysis', 'data', 'suspicious', 'typology'],
            'international': ['international', 'global', 'fatf', 'overseas', 'foreign', 'cross-border'],
            'education': ['forum', 'education', 'training', 'workshop', 'seminar', 'conference'],
            'scams': ['scam', 'fraud', 'illicit', 'criminal', 'money laundering'],
            'registration': ['registration', 'registered', 'provider', 'licensing']
        }
        
        for category, keywords in categories.items():
            if any(keyword in headline_lower for keyword in keywords):
                return category.title()
        
        return 'General'
    
    def _extract_articles_from_page(self) -> List[Dict]:
        """Extract articles from the current page using multiple selector strategies"""
        articles = []
        
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Strategy 1: Look for the specific selectors
            article_elements = soup.select('.latest-news__card')
            
            if not article_elements:
                # Strategy 2: Look for Drupal view content
                article_elements = soup.select('.views-row, .node, .view-content .item')
            
            if not article_elements:
                # Strategy 3: Look for common article patterns
                article_elements = soup.select('article, .news-item, .update-item, .content-item')
            
            if not article_elements:
                # Strategy 4: Look for any elements with links and dates
                article_elements = soup.select('div:has(a):has(time), li:has(a):has(time)')
            
            logger.info(f"Found {len(article_elements)} potential article elements")
            
            recent_count = 0
            for element in article_elements:
                try:
                    article_data = self._parse_article_element(element)
                    if article_data:
                        # Check if article is recent (for daily runs)
                        if self._is_recent_article(article_data.get('published_date', '')):
                            recent_count += 1
                        
                        articles.append(article_data)
                        logger.info(f"📄 Found article: {article_data.get('headline', 'Unknown')[:100]}...")
                        
                except Exception as e:
                    logger.warning(f"Error parsing article element: {e}")
                    continue
            
            # Track recent articles for daily run logic
            if not hasattr(self, '_recent_articles_found'):
                self._recent_articles_found = 0
            self._recent_articles_found += recent_count
            
            return articles
            
        except Exception as e:
            logger.error(f"Error extracting articles from page: {e}")
            return []
    
    def _parse_article_element(self, element) -> Optional[Dict]:
        """Parse individual article element with improved robustness"""
        try:
            # Extract headline and URL
            headline = None
            url = None
            
            # Enhanced headline selectors
            headline_selectors = [
                '.latest-news__card-title a',
                '.latest-news__card-title',
                '.node-title a',
                '.views-field-title a',
                'h1, h2, h3, h4, h5, h6',
                '.title a',
                '.headline a',
                'a[href*="/news/"], a[href*="/updates/"]',
                'a'
            ]
            
            for selector in headline_selectors:
                headline_elem = element.select_one(selector)
                if headline_elem:
                    headline = headline_elem.get_text(strip=True)
                    if headline_elem.name == 'a' and headline_elem.get('href'):
                        url = headline_elem.get('href')
                    elif headline_elem.find('a'):
                        url = headline_elem.find('a').get('href', '')
                    
                    if headline and len(headline) > 10:  # Ensure substantial headline
                        break
            
            if not headline or len(headline) < 10:
                return None
            
            # Look for URL if not found yet
            if not url:
                link_selectors = [
                    'a[href*="/news/"]',
                    'a[href*="/updates/"]',
                    'a[href*="/business/"]',
                    'a[href]'
                ]
                
                for selector in link_selectors:
                    link_elem = element.select_one(selector)
                    if link_elem:
                        url = link_elem.get('href')
                        break
            
            if not url:
                return None
            
            # Make URL absolute
            if url.startswith('/'):
                url = self.base_url + url
            elif not url.startswith('http'):
                url = self.base_url + '/' + url.lstrip('/')
            
            # Extract published date with enhanced selectors
            published_date = "Unknown"
            date_selectors = [
                'time',
                '.latest-news__card-date',
                '.views-field-created',
                '.date',
                '.published',
                '.field--name-field-article-dateline',
                '.post-date'
            ]
            
            for selector in date_selectors:
                date_elem = element.select_one(selector)
                if date_elem:
                    published_date = date_elem.get_text(strip=True)
                    # Also try datetime attribute
                    if not published_date and date_elem.get('datetime'):
                        published_date = date_elem.get('datetime')
                    if published_date and published_date != "Unknown":
                        break
            
            # Extract intro/summary text
            intro_text = ""
            intro_selectors = [
                '.latest-news__card-intro', 
                '.summary', 
                '.excerpt', 
                '.intro',
                '.views-field-body',
                '.field--name-body .field__item'
            ]
            
            for selector in intro_selectors:
                intro_elem = element.select_one(selector)
                if intro_elem:
                    intro_text = intro_elem.get_text(strip=True)
                    if len(intro_text) > 20:  # Ensure substantial intro
                        break
            
            # Generate hash for deduplication
            hash_id = self._generate_hash(url, headline, published_date)
            
            # Check if already exists
            if hash_id in self.existing_hashes:
                logger.debug(f"🔄 Skipping existing article: {headline[:50]}...")
                return None
            
            article_data = {
                'hash_id': hash_id,
                'headline': self._clean_text(headline),
                'url': url,
                'published_date': published_date,
                'scraped_date': datetime.now(timezone.utc).isoformat(),
                'intro_text': self._clean_text(intro_text),
                'content': '',
                'related_links': [],
                'pdf_links': [],
                'category': self._extract_category(headline),
                'run_type': self.run_type
            }
            
            return article_data
            
        except Exception as e:
            logger.error(f"Error parsing article element: {e}")
            return None
    
    def _scrape_page(self, page_num: int = 0) -> List[Dict]:
        """Scrape articles from a specific page"""
        try:
            if page_num > 0:
                url = f"{self.target_url}?page={page_num}"
            else:
                url = self.target_url
            
            logger.info(f"📄 Scraping page: {url}")
            
            self.driver.get(url)
            
            if not self._wait_for_page_load():
                logger.error(f"Failed to load page: {url}")
                return []
            
            # Random delay to appear more human
            time.sleep(random.uniform(2, 4))
            
            # Extract articles from current page
            articles = self._extract_articles_from_page()
            
            # For daily runs, check if we found recent articles
            if self.run_type == "daily":
                recent_articles = [a for a in articles if self._is_recent_article(a.get('published_date', ''))]
                if not recent_articles:
                    if not hasattr(self, '_pages_without_recent'):
                        self._pages_without_recent = 0
                    self._pages_without_recent += 1
                else:
                    self._pages_without_recent = 0
            
            logger.info(f"Found {len(articles)} articles on page {page_num + 1}")
            return articles
            
        except Exception as e:
            logger.error(f"Error scraping page {page_num + 1}: {e}")
            return []
    
    def _check_pagination(self) -> bool:
        """Check if there are more pages to scrape"""
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Look for pagination indicators
            pagination_selectors = [
                '.pager__item--next',
                '.pagination .next',
                'a[rel="next"]',
                '.next-page',
                '.pager .pager-next'
            ]
            
            for selector in pagination_selectors:
                next_link = soup.select_one(selector)
                if next_link and not next_link.get('disabled'):
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking pagination: {e}")
            return False
    
    def scrape_articles(self) -> List[Dict]:
        """Main scraping method with enhanced logic for daily vs initial runs"""
        logger.info("="*80)
        logger.info(f"🚀 Starting AUSTRAC Updates scraping ({self.run_type.upper()} run)")
        logger.info("="*80)
        
        if not self._setup_driver():
            return []
        
        try:
            all_articles = []
            page_num = 0
            consecutive_empty_pages = 0
            max_empty_pages = 3 if self.run_type == "daily" else 5
            
            # Initialize tracking variables
            self._recent_articles_found = 0
            self._pages_without_recent = 0
            
            while (consecutive_empty_pages < max_empty_pages and 
                   page_num < self.max_pages and 
                   not shutdown_requested and
                   self._should_continue_scraping()):
                
                logger.info(f"Processing page {page_num + 1}")
                
                page_articles = self._scrape_page(page_num)
                
                if not page_articles:
                    consecutive_empty_pages += 1
                    logger.info(f"Empty page encountered ({consecutive_empty_pages}/{max_empty_pages})")
                else:
                    consecutive_empty_pages = 0
                    all_articles.extend(page_articles)
                
                # Check if there are more pages
                if not self._check_pagination():
                    logger.info("No more pages to scrape")
                    break
                
                page_num += 1
                
                # Add delay between pages
                time.sleep(random.uniform(2, 4))
            
            if not all_articles:
                logger.info("No new articles found")
                return []
            
            logger.info(f"Found {len(all_articles)} new articles")
            logger.info("="*80)
            logger.info("📄 Processing content extraction...")
            logger.info("="*80)
            
            # Extract detailed content for each article
            enriched_articles = []
            for i, article in enumerate(all_articles):
                if shutdown_requested:
                    break
                
                logger.info(f"Processing article {i+1}/{len(all_articles)}")
                enriched_article = self._extract_article_content(article)
                enriched_articles.append(enriched_article)
                
                # Add delay between requests
                time.sleep(random.uniform(2, 4))
            
            logger.info("="*80)
            logger.info(f"✅ Successfully processed {len(enriched_articles)} articles")
            logger.info("="*80)
            
            return enriched_articles
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔒 Browser closed")
    
    def _load_existing_articles(self) -> List[Dict]:
        """Load existing articles from JSON file"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Handle both old format (direct list) and new format (with metadata wrapper)
                    if isinstance(data, list):
                        # Old format: direct list of articles
                        return data
                    elif isinstance(data, dict) and 'articles' in data:
                        # New format: metadata wrapper with articles key
                        articles = data['articles']
                        if isinstance(articles, list):
                            return articles
                        else:
                            logger.warning("Articles key does not contain a list")
                            return []
                    else:
                        logger.warning("JSON file format not recognized, expected list or dict with 'articles' key")
                        return []
                        
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON file: {e}")
                return []
            except Exception as e:
                logger.error(f"Error loading existing articles: {e}")
                return []
        return []
    
    def save_articles(self, new_articles: List[Dict]):
        """Save articles to JSON and CSV files with enhanced metadata"""
        if not new_articles:
            logger.info("No new articles to save")
            return
        
        # Load existing data
        existing_articles = self._load_existing_articles()
        
        # Create a set of existing hash_ids for faster lookup
        existing_hash_ids = {article.get('hash_id', '') for article in existing_articles if isinstance(article, dict)}
        
        # Filter out truly new articles
        actually_new_articles = []
        for article in new_articles:
            if article.get('hash_id', '') not in existing_hash_ids:
                actually_new_articles.append(article)
            else:
                logger.debug(f"Skipping duplicate: {article.get('headline', 'Unknown')}")
        
        if not actually_new_articles:
            logger.info("No genuinely new articles found - all were duplicates")
            return
        
        # Merge new articles with existing
        all_articles = existing_articles + actually_new_articles
        
        # Sort by scraped_date (most recent first)
        try:
            all_articles.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
        except Exception as e:
            logger.warning(f"Could not sort by date: {e}")
        
        # Save to JSON with enhanced metadata
        try:
            save_metadata = {
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'run_type': self.run_type,
                'total_articles': len(all_articles),
                'new_articles_this_run': len(actually_new_articles),
                'articles': all_articles
            }
            
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(save_metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 Saved {len(all_articles)} total articles to {self.json_file}")
            logger.info(f"➕ Added {len(actually_new_articles)} new articles")
        except Exception as e:
            logger.error(f"Error saving JSON file: {e}")
        
        # Save to CSV for easy analysis
        if all_articles:
            try:
                fieldnames = [
                    'hash_id', 'headline', 'url', 'published_date', 
                    'scraped_date', 'intro_text', 'content', 'related_links', 
                    'pdf_links', 'category', 'run_type'
                ]
                
                df = pd.DataFrame(all_articles)
                
                # Ensure all required columns exist
                for col in fieldnames:
                    if col not in df.columns:
                        df[col] = ""
                
                # Convert lists to strings for CSV
                if 'related_links' in df.columns:
                    df['related_links'] = df['related_links'].apply(lambda x: '; '.join(x) if isinstance(x, list) else str(x))
                if 'pdf_links' in df.columns:
                    df['pdf_links'] = df['pdf_links'].apply(lambda x: '; '.join(x) if isinstance(x, list) else str(x))
                
                df = df[fieldnames]
                df.to_csv(self.csv_file, index=False, encoding='utf-8')
                logger.info(f"💾 Saved {len(all_articles)} total articles to {self.csv_file}")
            except Exception as e:
                logger.error(f"Error saving CSV file: {e}")
        
        # Log detailed summary of new additions
        if actually_new_articles:
            logger.info("="*80)
            logger.info("📋 NEW ARTICLES ADDED:")
            
            # Category breakdown
            categories = {}
            content_with_pdfs = 0
            total_content_length = 0
            
            for article in actually_new_articles:
                category = article.get('category', 'Unknown')
                categories[category] = categories.get(category, 0) + 1
                
                if article.get('pdf_links'):
                    content_with_pdfs += 1
                
                content_length = len(article.get('content', ''))
                total_content_length += content_length
                
                logger.info(f"   • {category}: {article.get('headline', 'No title')[:80]}...")
                if article.get('pdf_links'):
                    logger.info(f"     📎 PDFs: {len(article.get('pdf_links', []))}")
                if content_length > 1000:
                    logger.info(f"     📄 Content: {content_length:,} chars")
            
            logger.info(f"\n📊 SUMMARY:")
            logger.info(f"   Total new articles: {len(actually_new_articles)}")
            logger.info(f"   Articles with PDFs: {content_with_pdfs}")
            if actually_new_articles:
                logger.info(f"   Average content length: {total_content_length // len(actually_new_articles):,} chars")
            logger.info(f"   Categories: {dict(categories)}")
            logger.info("="*80)

def main():
    """Main execution function with run type detection"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced AUSTRAC Updates Scraper')
    parser.add_argument('--run-type', choices=['daily', 'initial'], default='daily',
                        help='Type of run: daily (recent articles) or initial (comprehensive)')
    parser.add_argument('--max-pages', type=int, help='Override maximum pages to scrape')
    
    args = parser.parse_args()
    
    try:
        # Determine run type
        run_type = args.run_type
        
        # Auto-detect initial run if no existing data
        if not JSON_PATH.exists() and run_type == 'daily':
            logger.info("No existing data found, switching to initial run")
            run_type = 'initial'
        
        scraper = EnhancedAUSTRACScraper(run_type=run_type)
        
        # Override max pages if specified
        if args.max_pages:
            scraper.max_pages = args.max_pages
            logger.info(f"Override: Max pages set to {args.max_pages}")
        
        logger.info("="*80)
        logger.info(f"🚀 ENHANCED AUSTRAC SCRAPER STARTED ({run_type.upper()} RUN)")
        logger.info("="*80)
        
        new_articles = scraper.scrape_articles()
        
        if new_articles:
            logger.info(f"✅ Successfully scraped {len(new_articles)} articles")
            scraper.save_articles(new_articles)
            
            # Enhanced summary statistics
            categories = {}
            content_count = 0
            pdf_count = 0
            total_content_chars = 0
            
            for article in new_articles:
                category = article.get('category', 'Unknown')
                categories[category] = categories.get(category, 0) + 1
                
                content = article.get('content', '')
                if content and len(content.strip()) > 100:
                    content_count += 1
                    total_content_chars += len(content)
                
                if article.get('pdf_links'):
                    pdf_count += 1
            
            logger.info("="*80)
            logger.info("📊 ENHANCED SCRAPING SUMMARY:")
            logger.info(f"   Run type: {run_type.upper()}")
            logger.info(f"   Total articles found: {len(new_articles)}")
            logger.info(f"   Articles with substantial content: {content_count}")
            logger.info(f"   Articles with PDF attachments: {pdf_count}")
            if content_count > 0:
                logger.info(f"   Average content length: {total_content_chars // content_count:,} characters")
            logger.info("   Categories:")
            for category, count in sorted(categories.items()):
                logger.info(f"     - {category}: {count}")
            logger.info("="*80)
            
        else:
            logger.info("ℹ️  No new articles found (all may be existing)")
            
    except KeyboardInterrupt:
        logger.info("🛑 Scraping interrupted by user")
    except Exception as e:
        logger.error(f"💥 Scraping failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()