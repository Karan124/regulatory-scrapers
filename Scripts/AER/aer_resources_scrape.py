#!/usr/bin/env python3
"""
Enhanced AER News Scraper for LLM Analysis
------------------------------------------
Comprehensive scraper that extracts all content types, embedded files,
and related resources with proper categorization and error handling.
"""

import json
import os
import time
import logging
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Dict, Set
import random
import requests
from bs4 import BeautifulSoup
import hashlib

# Import Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# File processing imports
try:
    import PyPDF2
    import io
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("WARNING: PyPDF2 not found. PDF extraction will be disabled. Run: pip install PyPDF2")

try:
    import pandas as pd
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False
    print("WARNING: pandas/openpyxl not found. Excel/CSV extraction will be disabled. Run: pip install pandas openpyxl")


class EnhancedAERNewsScraper:
    """Comprehensive AER news scraper with enhanced content extraction and categorization"""
    
    def __init__(self, max_pages: int = 2):
        self.BASE_URL = "https://www.aer.gov.au"
        self.NEWS_URL = "https://www.aer.gov.au/news/articles"
        
        self.DATA_DIR = "data"
        self.JSON_FILE = os.path.join(self.DATA_DIR, "aer_news.json")
        
        # Create data directory
        os.makedirs(self.DATA_DIR, exist_ok=True)
        
        # Smart MAX_PAGES logic with better handling
        if max_pages is None:
            is_first_run = not os.path.exists(self.JSON_FILE)
            self.MAX_PAGES = 300 if is_first_run else 5  # Reduced from 350 to avoid edge case errors
            run_type = "First Run" if is_first_run else "Daily Update"
            print(f"INFO: Detected '{run_type}'. Setting MAX_PAGES to {self.MAX_PAGES}.")
        else:
            self.MAX_PAGES = max_pages
        self.setup_logging()
        
        self.driver = None
        self.session = requests.Session()
        self.existing_articles = self.load_existing_data()
        self.processed_files: Set[str] = set()
        self.session_retry_count = 0
        self.max_session_retries = 3
        
        self.setup_session()
        self.logger.info(f"Enhanced AER News Scraper initialized. Max pages to scrape: {self.MAX_PAGES}")

    def setup_logging(self):
        """Setup console-only logging"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def setup_session(self):
        """Setup session with realistic browser headers and better retry handling"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def load_existing_data(self) -> Dict[str, Dict]:
        """Load existing articles for deduplication with better error handling"""
        existing = {}
        if os.path.exists(self.JSON_FILE):
            try:
                with open(self.JSON_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for article in data:
                        if isinstance(article, dict) and 'url' in article:
                            existing[article['url']] = article
                self.logger.info(f"Loaded {len(existing)} existing articles for deduplication.")
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                # Create backup of corrupted file
                try:
                    backup_file = f"{self.JSON_FILE}.backup_{int(time.time())}"
                    os.rename(self.JSON_FILE, backup_file)
                    self.logger.info(f"Corrupted file backed up to: {backup_file}")
                except Exception as backup_error:
                    self.logger.error(f"Failed to backup corrupted file: {backup_error}")
        return existing

    def _setup_driver(self) -> Optional[webdriver.Chrome]:
        """Setup Chrome driver with enhanced stability and error handling"""
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')  # Speed up loading
        options.add_argument('--disable-javascript')  # We don't need JS for scraping
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument(f'--user-agent={self.session.headers["User-Agent"]}')
        
        # Additional stability options
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-features=TranslateUI')
        options.add_argument('--disable-default-apps')
        
        try:
            service = Service()
            driver = webdriver.Chrome(service=service, options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.implicitly_wait(15)
            driver.set_page_load_timeout(90)  # Increased timeout
            return driver
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            return None

    def establish_session(self) -> bool:
        """Establish session with improved retry logic and error handling"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass

        self.driver = self._setup_driver()
        if not self.driver:
            return False
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Session establishment attempt {attempt + 1}/{max_retries}")
                
                # Session warm-up with shorter timeout for initial test
                self.logger.info("Testing connection with homepage...")
                self.driver.get(self.BASE_URL)
                time.sleep(random.uniform(2, 4))
                
                self.logger.info("Navigating to news section...")
                self.driver.get(self.NEWS_URL)
                
                # Wait for content to load with better error handling
                WebDriverWait(self.driver, 45).until(
                    EC.any_of(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.view-content")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".views-layout__item"))
                    )
                )
                
                self.logger.info("Session established successfully.")
                self.session_retry_count = 0
                return True
                
            except TimeoutException:
                self.logger.warning(f"Session establishment timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 10))
                    continue
            except Exception as e:
                self.logger.error(f"Session establishment failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 10))
                    continue
        
        self.logger.error("Failed to establish session after all retries")
        return False

    def clean_text_for_llm(self, text: str) -> str:
        """Clean text to make it maximally LLM-friendly with enhanced processing"""
        if not text:
            return ""
        
        # Remove excessive whitespace and normalize spacing
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'(\n\s*)+\n', '\n', text)
        
        # Remove special characters that might interfere with JSON or LLM processing
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        # Clean up common HTML entities and artifacts
        html_entities = {
            '&nbsp;': ' ', '&amp;': '&', '&lt;': '<', '&gt;': '>',
            '&quot;': '"', '&#39;': "'", '&apos;': "'", '&mdash;': '—',
            '&ndash;': '–', '&hellip;': '…', '&lsquo;': ''', '&rsquo;': ''',
            '&ldquo;': '"', '&rdquo;': '"', '&bull;': '•'
        }
        for entity, replacement in html_entities.items():
            text = text.replace(entity, replacement)
        
        # Remove unwanted artifacts and normalize punctuation
        text = re.sub(r'\s*\|\s*', ' | ', text)  # Normalize pipe separators
        text = re.sub(r'\s*-\s*', ' - ', text)   # Normalize dashes
        text = re.sub(r'\.{2,}', '...', text)    # Normalize ellipsis
        text = re.sub(r'\s*,\s*', ', ', text)    # Normalize comma spacing
        text = re.sub(r'\s*;\s*', '; ', text)    # Normalize semicolon spacing
        text = re.sub(r'\s*:\s*', ': ', text)    # Normalize colon spacing
        
        # Remove redundant quotation marks and fix spacing
        text = re.sub(r'"+', '"', text)
        text = re.sub(r"'+", "'", text)
        
        # Clean up common footer/header artifacts
        unwanted_patterns = [
            r'Print this page',
            r'Share this page',
            r'Download PDF',
            r'View larger image',
            r'Skip to main content',
            r'Back to top',
            r'© Australian Energy Regulator',
            r'Australian Energy Regulator \d{4}',
            r'Last updated:.*?\d{4}'
        ]
        
        for pattern in unwanted_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        # Final cleanup
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)  # Max 2 consecutive newlines
        text = text.strip()
        
        # Ensure the text ends properly for LLM processing
        if text and not text.endswith(('.', '!', '?', ':', '"', "'")):
            text += '.'
        
        return text

    def structure_content_for_llm(self, content_dict: Dict) -> str:
        """Structure extracted content in an LLM-friendly narrative format"""
        if not content_dict:
            return ""
        
        structured_parts = []
        
        # PDF content
        pdf_content = content_dict.get('pdf_content', [])
        if pdf_content:
            structured_parts.append("DOCUMENT ATTACHMENTS:")
            for i, pdf_info in enumerate(pdf_content, 1):
                pdf_text = pdf_info.get('text', '').strip()
                if pdf_text:
                    pdf_url = pdf_info.get('url', '')
                    pdf_name = os.path.basename(pdf_url) if pdf_url else f"Document {i}"
                    structured_parts.append(f"Document {i} ({pdf_name}):\n{pdf_text}")
        
        # Spreadsheet content  
        spreadsheet_content = content_dict.get('spreadsheet_content', [])
        if spreadsheet_content:
            structured_parts.append("DATA TABLES AND SPREADSHEETS:")
            for i, ss_info in enumerate(spreadsheet_content, 1):
                ss_text = ss_info.get('text', '').strip()
                if ss_text:
                    ss_url = ss_info.get('url', '')
                    ss_name = os.path.basename(ss_url) if ss_url else f"Spreadsheet {i}"
                    structured_parts.append(f"Data Table {i} ({ss_name}):\n{ss_text}")
        
        return '\n\n'.join(structured_parts)

    def format_links_for_llm(self, links_list: List[Dict]) -> str:
        """Format links in an LLM-friendly structured way"""
        if not links_list:
            return ""
        
        formatted_links = []
        for link in links_list:
            link_text = link.get('text', '').strip()
            link_url = link.get('url', '').strip()
            if link_text and link_url:
                formatted_links.append(f"• {link_text}: {link_url}")
        
        return '\n'.join(formatted_links) if formatted_links else ""

    def extract_pdf_content(self, pdf_url: str) -> str:
        """Extract complete PDF text with enhanced error handling"""
        if not PDF_AVAILABLE:
            return ""
            
        try:
            pdf_hash = hashlib.md5(pdf_url.encode()).hexdigest()
            if pdf_hash in self.processed_files:
                self.logger.info(f"Skipping duplicate PDF: {os.path.basename(pdf_url)}")
                return ""
            
            self.logger.info(f"Extracting PDF content: {os.path.basename(pdf_url)}")
            
            # Enhanced request with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.get(pdf_url, timeout=120, stream=True)
                    response.raise_for_status()
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"PDF download attempt {attempt + 1} failed: {e}")
                        time.sleep(random.uniform(2, 5))
                        continue
                    else:
                        raise e
            
            # Check content type
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and len(response.content) < 1000:
                self.logger.warning(f"PDF {pdf_url} appears to be invalid or too small")
                return ""
            
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(response.content))
            
            if len(pdf_reader.pages) == 0:
                self.logger.warning(f"PDF {pdf_url} has no pages")
                return ""
            
            full_text_parts = []
            for i, page in enumerate(pdf_reader.pages):
                try:
                    page_text = page.extract_text()
                    if page_text and page_text.strip():
                        full_text_parts.append(self.clean_text_for_llm(page_text))
                except Exception as e:
                    self.logger.warning(f"Failed to extract text from page {i + 1} of {pdf_url}: {e}")
            
            full_text = ' '.join(full_text_parts)
            
            self.processed_files.add(pdf_hash)
            self.logger.info(f"Successfully extracted {len(full_text)} characters from PDF: {os.path.basename(pdf_url)}")
            return full_text
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF {pdf_url}: {e}")
            return ""

    def extract_excel_csv_content(self, file_url: str) -> str:
        """Extract content from Excel/CSV files with enhanced error handling"""
        if not EXCEL_AVAILABLE:
            return ""
            
        try:
            file_hash = hashlib.md5(file_url.encode()).hexdigest()
            if file_hash in self.processed_files:
                self.logger.info(f"Skipping duplicate spreadsheet: {os.path.basename(file_url)}")
                return ""

            self.logger.info(f"Extracting spreadsheet content: {os.path.basename(file_url)}")
            
            # Enhanced request with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.get(file_url, timeout=120)
                    response.raise_for_status()
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Spreadsheet download attempt {attempt + 1} failed: {e}")
                        time.sleep(random.uniform(2, 5))
                        continue
                    else:
                        raise e
            
            file_extension = os.path.splitext(urlparse(file_url).path)[1].lower()
            
            try:
                if file_extension == '.csv':
                    df = pd.read_csv(io.BytesIO(response.content), encoding='utf-8')
                else:
                    df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
            except UnicodeDecodeError:
                # Try with different encodings for CSV
                if file_extension == '.csv':
                    try:
                        df = pd.read_csv(io.BytesIO(response.content), encoding='latin-1')
                    except:
                        df = pd.read_csv(io.BytesIO(response.content), encoding='cp1252')
                else:
                    raise

            # Create comprehensive summary
            summary_parts = [
                f"File: {os.path.basename(file_url)}",
                f"Columns ({len(df.columns)}): {', '.join(df.columns.astype(str))}",
                f"Rows: {len(df)}",
                f"Data types: {dict(df.dtypes.astype(str))}",
                f"Sample data (first 3 rows):\n{df.head(3).to_string(index=False)}"
            ]
            
            # Add summary statistics for numeric columns
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                summary_parts.append(f"Numeric column statistics:\n{df[numeric_cols].describe().to_string()}")
            
            content = '\n\n'.join(summary_parts)
            
            self.processed_files.add(file_hash)
            return self.clean_text_for_llm(content)
            
        except Exception as e:
            self.logger.error(f"Error extracting spreadsheet {file_url}: {e}")
            return ""

    def extract_article_type_from_index(self, article_card_soup: BeautifulSoup) -> str:
        """Extract article type from the index page card"""
        try:
            type_elem = article_card_soup.select_one('div.field--name-field-article-type .field__item')
            if type_elem:
                return type_elem.get_text(strip=True)
        except Exception as e:
            self.logger.debug(f"Error extracting article type from index: {e}")
        return ""

    def extract_segments_from_index(self, article_card_soup: BeautifulSoup) -> List[str]:
        """Extract segments from the index page card"""
        segments = []
        try:
            segment_items = article_card_soup.select('div.field--name-field-segments .field__item')
            for item in segment_items:
                segment = item.get_text(strip=True)
                if segment:
                    segments.append(segment)
        except Exception as e:
            self.logger.debug(f"Error extracting segments from index: {e}")
        return segments

    def extract_sectors_from_index(self, article_card_soup: BeautifulSoup) -> List[str]:
        """Extract sectors from the index page card"""
        sectors = []
        try:
            # Look for electricity and gas indicators
            if article_card_soup.select_one('.field__item-electricity'):
                sectors.append('Electricity')
            if article_card_soup.select_one('.field__item-gas'):
                sectors.append('Gas')
        except Exception as e:
            self.logger.debug(f"Error extracting sectors from index: {e}")
        return sectors

    def extract_embedded_content(self, soup: BeautifulSoup) -> Dict:
        """Extract content from embedded files and all links within article paragraphs"""
        content = {
            'pdf_content': [],
            'spreadsheet_content': [],
            'embedded_links': [],  # All links within article <p> tags
            'related_content_links': []  # Related content at bottom
        }
        
        # Extract ALL links from paragraph tags within article body
        content_area = soup.find('div', class_='field--name-field-body')
        if content_area:
            # Get all links within <p> tags in the main content area
            paragraph_links = content_area.select('p a[href]')
            
            for link in paragraph_links:
                href = link.get('href', '').strip()
                if not href:
                    continue
                
                # Skip obvious non-content links
                if href.startswith(('#', 'mailto:', 'tel:', 'javascript:')):
                    continue
                
                # Convert relative URLs to absolute
                if href.startswith('/'):
                    full_url = urljoin(self.BASE_URL, href)
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue
                
                link_text = link.get_text(strip=True)
                if link_text:
                    content['embedded_links'].append({
                        'url': full_url,
                        'text': link_text,
                        'source': 'paragraph'
                    })

        # Extract related content links from bottom section with better selectors
        related_selectors = [
            'section.page__related',
            '.views-element-container.block-views-block-content-related',
            '#block-views-block-content-related',
            '.view-content-related'
        ]
        
        for selector in related_selectors:
            related_section = soup.select_one(selector)
            if related_section:
                self._extract_related_content(related_section, content['related_content_links'])
                break

        # Process all collected links for file content
        all_links = content['embedded_links'] + content['related_content_links']
        for link_info in all_links:
            href = link_info['url']
            href_lower = href.lower()
            
            if href_lower.endswith('.pdf'):
                pdf_text = self.extract_pdf_content(href)
                if pdf_text:
                    content['pdf_content'].append({
                        'url': href,
                        'text': pdf_text,
                        'source': link_info.get('source', 'unknown')
                    })
            elif href_lower.endswith(('.xlsx', '.xls', '.csv')):
                ss_text = self.extract_excel_csv_content(href)
                if ss_text:
                    content['spreadsheet_content'].append({
                        'url': href,
                        'text': ss_text,
                        'source': link_info.get('source', 'unknown')
                    })
        
        return content

    def _extract_related_content(self, related_section: BeautifulSoup, link_list: List[Dict]):
        """Extract related content with full metadata from the bottom section"""
        cards = related_section.select('.card__title a, .views-layout__item a[href]')
        
        for card_link in cards:
            href = card_link.get('href', '').strip()
            if not href:
                continue
            
            # Convert relative URLs to absolute
            full_url = urljoin(self.BASE_URL, href) if href.startswith('/') else href
            
            # Get the card title
            title = card_link.get_text(strip=True)
            
            # Try to get additional metadata from the card
            card_container = card_link.find_parent('.card__inner') or card_link.find_parent('.views-layout__item')
            
            description = ""
            content_type = ""
            sectors = []
            segments = []
            date = ""
            
            if card_container:
                # Extract description
                summary_elem = card_container.select_one('.field--name-field-summary, .card__body')
                if summary_elem:
                    description = summary_elem.get_text(strip=True)
                
                # Extract content type
                type_elem = card_container.select_one('.field--name-field-report-type .field__item, .field--name-field-article-type .field__item')
                if type_elem:
                    content_type = type_elem.get_text(strip=True)
                
                # Extract sectors
                if card_container.select_one('.field__item-electricity'):
                    sectors.append('Electricity')
                if card_container.select_one('.field__item-gas'):
                    sectors.append('Gas')
                
                # Extract segments
                segment_items = card_container.select('.field--name-field-segments .field__item')
                for item in segment_items:
                    segment = item.get_text(strip=True)
                    if segment:
                        segments.append(segment)
                
                # Extract date
                date_elem = card_container.select_one('time[datetime]')
                if date_elem:
                    date = date_elem.get('datetime', date_elem.get_text(strip=True))
            
            if title and full_url:
                link_list.append({
                    'url': full_url,
                    'title': title,
                    'description': description,
                    'type': content_type,
                    'sectors': sectors,
                    'segments': segments,
                    'date': date,
                    'source': 'related_content'
                })

    def _extract_links_from_area(self, area_soup: BeautifulSoup, link_list: List[Dict], source: str):
        """Extract and filter links from a specific area with better text content link detection"""
        # Skip patterns for unwanted links
        skip_patterns = [
            'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com',
            'mailto:', 'tel:', '#', '/about/', '/contacts/', '/sitemap',
            'javascript:', 'void(0)'
        ]
        
        # File extensions we want to capture
        wanted_extensions = ['.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx', '.txt']
        
        for link in area_soup.find_all('a', href=True):
            href = link.get('href', '').strip()
            if not href:
                continue
                
            # Convert relative URLs to absolute
            if href.startswith('/'):
                full_url = urljoin(self.BASE_URL, href)
            elif href.startswith('http'):
                full_url = href
            else:
                continue
            
            # Skip unwanted patterns
            if any(pattern in href.lower() for pattern in skip_patterns):
                continue
            
            link_text = link.get_text(strip=True)
            if not link_text:
                continue
            
            # Include links that are either files, content pages, or embedded content links
            href_lower = href.lower()
            is_file = any(href_lower.endswith(ext) for ext in wanted_extensions)
            is_content_page = ('/news/articles/' in href or 
                              '/publications/' in href or 
                              '/industry/registers/' in href or
                              '/engage/' in href)
            
            # For embedded content, also capture internal AER links within text
            if source == 'embedded' and not is_file and not is_content_page:
                # Check if it's an internal AER link (relative or absolute AER URL)
                if href.startswith('/') or 'aer.gov.au' in href:
                    is_content_page = True
            
            if is_file or is_content_page:
                link_list.append({
                    'url': full_url,
                    'text': link_text,
                    'source': source,
                    'is_file': is_file
                })

    def get_article_links_with_metadata(self, page_num=0) -> List[Dict]:
        """Get article links with metadata from index page"""
        try:
            url = f"{self.NEWS_URL}?page={page_num}"
            self.logger.info(f"Fetching page {page_num + 1}: {url}")
            
            # Enhanced page loading with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.driver.get(url)
                    WebDriverWait(self.driver, 45).until(
                        EC.any_of(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "h3.card__title a")),
                            EC.presence_of_element_located((By.CSS_SELECTOR, ".views-layout__item"))
                        )
                    )
                    break
                except TimeoutException:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Page load timeout, retry {attempt + 1}")
                        time.sleep(random.uniform(5, 10))
                        continue
                    else:
                        raise TimeoutException(f"Failed to load page {page_num + 1} after {max_retries} attempts")
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            article_cards = soup.select('.views-layout__item')
            
            articles_info = []
            for card in article_cards:
                link_elem = card.select_one('h3.card__title a')
                if not link_elem or not link_elem.get('href', '').startswith('/news/articles/'):
                    continue
                
                article_url = urljoin(self.BASE_URL, link_elem['href'])
                article_info = {
                    'url': article_url,
                    'title': link_elem.get_text(strip=True),
                    'article_type': self.extract_article_type_from_index(card),
                    'sectors': self.extract_sectors_from_index(card),
                    'segments': self.extract_segments_from_index(card)
                }
                articles_info.append(article_info)
            
            self.logger.info(f"Found {len(articles_info)} valid articles on page {page_num + 1}")
            return articles_info
            
        except Exception as e:
            self.logger.error(f"Failed to get links for page {page_num + 1}: {e}")
            return []

    def parse_article(self, article_info: Dict) -> Optional[Dict]:
        """Parse article with comprehensive content extraction"""
        url = article_info['url']
        try:
            self.logger.info(f"Parsing article: {article_info.get('title', 'Unknown')}")
            
            # Enhanced page loading with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.driver.get(url)
                    WebDriverWait(self.driver, 45).until(
                        EC.presence_of_element_located((By.TAG_NAME, "h1"))
                    )
                    break
                except TimeoutException:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Article load timeout, retry {attempt + 1}")
                        time.sleep(random.uniform(3, 7))
                        continue
                    else:
                        raise TimeoutException(f"Failed to load article after {max_retries} attempts")
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # Extract basic information
            title = soup.find('h1').get_text(strip=True) if soup.find('h1') else article_info.get('title', 'N/A')
            
            # Extract date with multiple selectors
            date = ""
            date_selectors = [
                'div.field--name-field-date time',
                '.field--label-inline:contains("Issue date") .field__item time',
                '.field--label-inline:contains("Release date") .field__item time',
                'time[datetime]'
            ]
            
            for selector in date_selectors:
                date_elem = soup.select_one(selector)
                if date_elem:
                    date = date_elem.get('datetime', date_elem.get_text(strip=True))
                    break
            
            # Extract main content
            main_content = ""
            body_elem = soup.find('div', class_='field--name-field-body')
            if body_elem:
                main_content = self.clean_text_for_llm(body_elem.get_text(strip=True))

            # Extract article type from content page (more reliable than index)
            article_type = ""
            type_elem = soup.select_one('div.field--name-field-article-type .field__item')
            if type_elem:
                article_type = type_elem.get_text(strip=True)
            else:
                article_type = article_info.get('article_type', '')

            # Extract sectors from content page
            sectors = []
            sector_items = soup.select('div.field--name-field-sectors .field__item')
            for item in sector_items:
                sector = item.get_text(strip=True)
                if sector:
                    sectors.append(sector)
            
            if not sectors:
                sectors = article_info.get('sectors', [])

            # Extract segments from content page
            segments = []
            segment_items = soup.select('div.field--name-field-segments .field__item')
            for item in segment_items:
                segment = item.get_text(strip=True)
                if segment:
                    segments.append(segment)
            
            if not segments:
                segments = article_info.get('segments', [])

            # Extract theme from breadcrumbs
            theme = ""
            breadcrumbs = soup.select('nav.breadcrumb a')
            if len(breadcrumbs) > 1:
                theme = breadcrumbs[-1].get_text(strip=True)

            # Extract image
            image_url = ""
            img_elem = soup.select_one('.field--name-field-body img, .article__image img')
            if img_elem and img_elem.get('src'):
                image_url = urljoin(self.BASE_URL, img_elem['src'])

            # Extract tables
            tables_content = []
            for table in soup.select('.field--name-field-body table'):
                table_text = self.clean_text_for_llm(table.get_text())
                if table_text:
                    tables_content.append(table_text)

            # Extract contact information
            contacts = []
            contact_items = soup.select('div.field--name-field-contacts .field__item a')
            for contact in contact_items:
                contacts.append({
                    'name': contact.get_text(strip=True),
                    'url': urljoin(self.BASE_URL, contact.get('href', ''))
                })

            # Extract embedded content and links
            embedded_content = self.extract_embedded_content(soup)

            # Build clean article data structure
            article_data = {
                'url': url,
                'headline': title,
                'published_date': date,
                'scraped_date': datetime.now().isoformat(),
                'article_type': article_type,
                'theme': theme,
                'sectors': sectors,
                'segments': segments,
                'image_url': image_url,
                'main_content': main_content,
                'embedded_links': embedded_content['embedded_links'],  # All links from <p> tags
                'related_content': embedded_content['related_content_links']  # Related content at bottom
            }
            
            # Add tables only if they exist
            if tables_content:
                article_data['tables_and_data'] = ' '.join(tables_content)
            
            # Add structured file content only if files were processed
            structured_files = self.structure_content_for_llm(embedded_content)
            if structured_files:
                article_data['structured_file_content'] = structured_files
            
            self.logger.info(f"Successfully parsed: {title[:70]}... (Type: {article_type})")
            return article_data
            
        except Exception as e:
            self.logger.error(f"Failed to parse article {url}: {e}", exc_info=False)
            return None

    def save_results(self):
        """Save results with enhanced error handling - single JSON file output only"""
        try:
            all_articles = list(self.existing_articles.values())
            
            # Create temporary backup in memory only - no backup files
            temp_backup_data = None
            if os.path.exists(self.JSON_FILE):
                try:
                    with open(self.JSON_FILE, 'r', encoding='utf-8') as f:
                        temp_backup_data = f.read()
                except Exception as e:
                    self.logger.warning(f"Failed to create temporary backup: {e}")
            
            # Save with pretty formatting - single file only
            with open(self.JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False, sort_keys=True)
            
            # Generate summary statistics for console output only
            stats = {
                'total_articles': len(all_articles),
                'by_type': {},
                'by_sector': {},
                'recent_articles': 0
            }
            
            cutoff_date = datetime.now().replace(year=datetime.now().year - 1)
            for article in all_articles:
                # Count by type
                article_type = article.get('article_type', 'Unknown')
                stats['by_type'][article_type] = stats['by_type'].get(article_type, 0) + 1
                
                # Count by sector
                sectors = article.get('sectors', [])
                for sector in sectors:
                    stats['by_sector'][sector] = stats['by_sector'].get(sector, 0) + 1
                
                # Count recent articles
                try:
                    article_date = datetime.fromisoformat(article.get('published_date', '').replace('Z', '+00:00'))
                    if article_date > cutoff_date:
                        stats['recent_articles'] += 1
                except:
                    pass
            
            self.logger.info(f"Saved results: {stats['total_articles']} total articles")
            self.logger.info(f"Article types: {dict(stats['by_type'])}")
            self.logger.info(f"Sectors: {dict(stats['by_sector'])}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            # Try to restore from memory backup if save failed
            if temp_backup_data:
                try:
                    with open(self.JSON_FILE, 'w', encoding='utf-8') as f:
                        f.write(temp_backup_data)
                    self.logger.info("Restored from temporary backup after save error")
                except:
                    self.logger.error("Failed to restore from temporary backup")

    def handle_session_recovery(self) -> bool:
        """Handle session recovery when driver fails"""
        self.session_retry_count += 1
        if self.session_retry_count >= self.max_session_retries:
            self.logger.error(f"Max session retries ({self.max_session_retries}) reached. Stopping.")
            return False
        
        self.logger.warning(f"Session recovery attempt {self.session_retry_count}/{self.max_session_retries}")
        time.sleep(random.uniform(10, 20))  # Longer wait for recovery
        return self.establish_session()

    def scrape_all_articles(self):
        """Main scraping method with enhanced resilience and error handling"""
        if not self.establish_session():
            self.logger.error("Failed to establish initial session. Exiting.")
            return
        
        self.logger.info("Starting enhanced comprehensive news scraping...")
        new_articles_count = 0
        consecutive_empty_pages = 0
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        try:
            for page_num in range(self.MAX_PAGES):
                self.logger.info(f"--- Processing page {page_num + 1}/{self.MAX_PAGES} ---")
                
                try:
                    articles_info = self.get_article_links_with_metadata(page_num)
                    consecutive_failures = 0  # Reset on success
                    
                    if not articles_info:
                        consecutive_empty_pages += 1
                        self.logger.warning(f"No articles found on page {page_num + 1}. Consecutive empty pages: {consecutive_empty_pages}")
                        
                        if consecutive_empty_pages >= 3:
                            self.logger.info("Reached end of available pages (3 consecutive empty pages).")
                            break
                        continue
                    
                    consecutive_empty_pages = 0

                    for article_info in articles_info:
                        url = article_info['url']
                        
                        if url in self.existing_articles:
                            self.logger.info(f"Skipping existing article: {article_info.get('title', url)}")
                            continue
                        
                        # Clear processed files for each new article
                        self.processed_files.clear()
                        
                        try:
                            article = self.parse_article(article_info)
                            if article:
                                self.existing_articles[url] = article
                                new_articles_count += 1
                                self.logger.info(f"Successfully scraped article {new_articles_count}: {article['headline'][:50]}...")
                            else:
                                self.logger.warning(f"Failed to parse article: {article_info.get('title', url)}")
                        
                        except Exception as article_error:
                            self.logger.error(f"Error processing article {url}: {article_error}")
                            # Try to recover session if it's a driver issue
                            if "driver" in str(article_error).lower() or "timeout" in str(article_error).lower():
                                if not self.handle_session_recovery():
                                    return
                        
                        # Periodic save
                        if new_articles_count > 0 and new_articles_count % 25 == 0:
                            self.logger.info(f"Saving progress after scraping {new_articles_count} new articles...")
                            self.save_results()

                        # Random delay between articles
                        time.sleep(random.uniform(1, 3))
                    
                    # Random delay between pages
                    time.sleep(random.uniform(2, 5))
                
                except WebDriverException as driver_error:
                    consecutive_failures += 1
                    self.logger.error(f"WebDriver error on page {page_num + 1}: {driver_error}")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        self.logger.error(f"Too many consecutive failures ({max_consecutive_failures}). Stopping.")
                        break
                    
                    if not self.handle_session_recovery():
                        break
                
                except Exception as page_error:
                    consecutive_failures += 1
                    self.logger.error(f"Error processing page {page_num + 1}: {page_error}")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        self.logger.error(f"Too many consecutive failures ({max_consecutive_failures}). Stopping.")
                        break
                    
                    # Short delay before continuing
                    time.sleep(random.uniform(5, 10))
                
        except KeyboardInterrupt:
            self.logger.warning("Scraping interrupted by user.")
        except Exception as e:
            self.logger.error(f"A critical error occurred: {e}", exc_info=True)
        finally:
            self.logger.info(f"Scraping completed. Total new articles scraped: {new_articles_count}")
            self.logger.info("Performing final save...")
            self.save_results()
            self.cleanup()

    def cleanup(self):
        """Clean up resources with enhanced error handling"""
        self.logger.info("Cleaning up resources...")
        
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Chrome driver closed successfully.")
            except Exception as e:
                self.logger.warning(f"Error closing Chrome driver: {e}")
        
        if self.session:
            try:
                self.session.close()
                self.logger.info("HTTP session closed successfully.")
            except Exception as e:
                self.logger.warning(f"Error closing HTTP session: {e}")
        
        # Clean up processed files set
        self.processed_files.clear()
        
        self.logger.info("Resource cleanup completed.")

    def get_scraper_stats(self) -> Dict:
        """Get comprehensive statistics about the scraper's data"""
        if not os.path.exists(self.JSON_FILE):
            return {"error": "No data file found"}
        
        try:
            with open(self.JSON_FILE, 'r', encoding='utf-8') as f:
                articles = json.load(f)
            
            stats = {
                'total_articles': len(articles),
                'article_types': {},
                'sectors': {},
                'segments': {},
                'files_extracted': {
                    'total_pdfs': 0,
                    'total_spreadsheets': 0,
                    'articles_with_files': 0
                },
                'date_range': {'earliest': None, 'latest': None}
            }
            
            for article in articles:
                # Count types
                article_type = article.get('article_type', 'Unknown')
                stats['article_types'][article_type] = stats['article_types'].get(article_type, 0) + 1
                
                # Count sectors
                for sector in article.get('sectors', []):
                    stats['sectors'][sector] = stats['sectors'].get(sector, 0) + 1
                
                # Count segments
                for segment in article.get('segments', []):
                    stats['segments'][segment] = stats['segments'].get(segment, 0) + 1
                
                # Count files
                embedded_files = article.get('embedded_files', {})
                pdf_count = len(embedded_files.get('pdf_content', []))
                ss_count = len(embedded_files.get('spreadsheet_content', []))
                
                stats['files_extracted']['total_pdfs'] += pdf_count
                stats['files_extracted']['total_spreadsheets'] += ss_count
                
                if pdf_count > 0 or ss_count > 0:
                    stats['files_extracted']['articles_with_files'] += 1
                
                # Track date range
                pub_date = article.get('published_date', '')
                if pub_date:
                    try:
                        date_obj = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                        if not stats['date_range']['earliest'] or date_obj < stats['date_range']['earliest']:
                            stats['date_range']['earliest'] = date_obj
                        if not stats['date_range']['latest'] or date_obj > stats['date_range']['latest']:
                            stats['date_range']['latest'] = date_obj
                    except:
                        pass
            
            # Convert datetime objects to strings for JSON serialization
            if stats['date_range']['earliest']:
                stats['date_range']['earliest'] = stats['date_range']['earliest'].isoformat()
            if stats['date_range']['latest']:
                stats['date_range']['latest'] = stats['date_range']['latest'].isoformat()
            
            return stats
            
        except Exception as e:
            return {"error": f"Failed to generate stats: {e}"}


def main():
    print("=" * 80)
    print("Enhanced AER News Scraper for LLM Analysis")
    print("=" * 80)
    print("Features:")
    print("• Extracts article types (Communication, News Release, Speech)")
    print("• Enhanced PDF and Excel/CSV content extraction")
    print("• Scrapes embedded links and related content")
    print("• Improved error handling and session recovery")
    print("• Comprehensive metadata extraction")
    print("=" * 80)
    
    scraper = EnhancedAERNewsScraper()
    
    try:
        scraper.scrape_all_articles()
    except Exception as e:
        print(f"Critical error in main: {e}")
    finally:
        # Show final statistics
        print("\n" + "=" * 80)
        print("SCRAPING COMPLETED - FINAL STATISTICS")
        print("=" * 80)
        
        stats = scraper.get_scraper_stats()
        if 'error' not in stats:
            print(f"Total articles: {stats['total_articles']}")
            print(f"Article types: {dict(stats['article_types'])}")
            print(f"Sectors: {dict(stats['sectors'])}")
            print(f"Files extracted: {stats['files_extracted']['total_pdfs']} PDFs, {stats['files_extracted']['total_spreadsheets']} spreadsheets")
            print(f"Articles with files: {stats['files_extracted']['articles_with_files']}")
            if stats['date_range']['earliest'] and stats['date_range']['latest']:
                print(f"Date range: {stats['date_range']['earliest'][:10]} to {stats['date_range']['latest'][:10]}")
        else:
            print(f"Stats error: {stats['error']}")
        
        print(f"Output saved to: {scraper.JSON_FILE}")
        print("=" * 80)


if __name__ == "__main__":
    main()