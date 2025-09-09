#!/usr/bin/env python3
"""
Enhanced Australian Bureau of Statistics Articles Scraper
LLM-optimized version with Excel/PDF extraction and comprehensive table data extraction
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import time
import random
import logging
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from pathlib import Path
import PyPDF2
from io import BytesIO
from fake_useragent import UserAgent
import urllib3
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict

# Excel processing imports
try:
    import pandas as pd
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False
    print("WARNING: pandas/openpyxl not found. Excel extraction will be disabled.")
    print("Run: pip install pandas openpyxl")

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@dataclass
class Article:
    """Enhanced data class for article information"""
    hash_id: str
    url: str
    headline: str
    published_date: str
    scraped_date: str
    theme: str = ""
    article_type: str = ""
    content_text: str = ""
    pdf_content: str = ""
    excel_content: str = ""
    image_url: str = ""
    embedded_links: List[Dict] = None
    charts_and_tables: str = ""
    
    def __post_init__(self):
        if self.embedded_links is None:
            self.embedded_links = []

class EnhancedABSArticlesScraper:
    """Enhanced ABS articles scraper with comprehensive content extraction"""
    
    def __init__(self, max_pages: int = 36, first_run: bool = True):
        """Initialize the enhanced scraper"""
        self.base_url = "https://www.abs.gov.au"
        self.articles_url = "https://www.abs.gov.au/articles"
        self.max_pages = max_pages
        self.first_run = first_run
        
        # Setup directories
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # File path - JSON only
        self.json_file = self.data_dir / "abs_all_articles.json"
        
        # Setup logging
        self.setup_logging()
        
        # Load existing articles for deduplication
        self.existing_articles = self.load_existing_articles()
        self.existing_urls = {article.get('url', '') for article in self.existing_articles}
        self.existing_hashes = {article.get('hash_id', '') for article in self.existing_articles}
        
        # Setup session with anti-bot measures
        self.session = self.setup_session()
        
        # Track processed files to avoid duplicates within same run
        self.processed_files: Set[str] = set()
        
        # Statistics
        self.stats = {
            'pages_processed': 0,
            'articles_found': 0,
            'articles_scraped': 0,
            'articles_skipped': 0,
            'pdfs_processed': 0,
            'excel_files_processed': 0,
            'errors': 0
        }
        
        self.logger.info(f"Enhanced ABS Scraper initialized - Max pages: {self.max_pages}")
        self.logger.info(f"Existing articles: {len(self.existing_articles)}")
    
    def setup_logging(self):
        """Setup console-only logging"""
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler only
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger('ABSScraper')
        self.logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
            
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def setup_session(self):
        """Setup session with comprehensive anti-bot measures"""
        session = requests.Session()
        
        try:
            ua = UserAgent()
            user_agent = ua.chrome
        except Exception:
            # Fallback user agent if fake_useragent fails
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        
        # Comprehensive headers
        session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Setup retry strategy
        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            
            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
        except Exception as e:
            self.logger.warning(f"Could not setup retry strategy: {e}")
        
        # Establish session
        try:
            self.logger.info("Establishing session...")
            response = session.get(self.base_url, timeout=30, verify=False)
            self.logger.info(f"Homepage status: {response.status_code}")
            time.sleep(random.uniform(2, 4))
            
            response = session.get(self.articles_url, timeout=30, verify=False)
            self.logger.info(f"Articles page status: {response.status_code}")
            time.sleep(random.uniform(3, 6))
        except Exception as e:
            self.logger.error(f"Error establishing session: {e}")
        
        return session
    
    def clean_text_for_llm(self, text: str) -> str:
        """Clean text to make it maximally LLM-friendly"""
        if not text:
            return ""
        
        # Remove excessive whitespace and normalize spacing
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'(\n\s*)+\n', '\n', text)
        
        # Remove special characters that might interfere with JSON or LLM processing
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        # Clean up common HTML entities
        html_entities = {
            '&nbsp;': ' ', '&amp;': '&', '&lt;': '<', '&gt;': '>',
            '&quot;': '"', '&#39;': "'", '&apos;': "'", '&mdash;': '—',
            '&ndash;': '–', '&hellip;': '…', '&lsquo;': ''', '&rsquo;': ''',
            '&ldquo;': '"', '&rdquo;': '"', '&bull;': '•'
        }
        for entity, replacement in html_entities.items():
            text = text.replace(entity, replacement)
        
        # Remove common page artifacts
        unwanted_patterns = [
            r'Skip to main content', r'Print this page', r'Share this page',
            r'Australian Bureau of Statistics', r'ABS Homepage',
            r'Back to top', r'Download.*?file', r'View.*?data',
            r'© Commonwealth of Australia'
        ]
        for pattern in unwanted_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        text = text.strip()
        if text and not text.endswith(('.', '!', '?', ':', '"', "'")):
            text += '.'
        
        return text
    
    def generate_hash(self, url: str, headline: str) -> str:
        """Generate unique hash for article"""
        try:
            content = f"{url.strip().rstrip('/')}|{headline.strip()}"
            return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
        except Exception:
            # Fallback hash
            return hashlib.sha256(f"{url}|{headline}".encode('utf-8', errors='ignore')).hexdigest()[:16]
    
    def load_existing_articles(self) -> List[Dict]:
        """Load existing articles for deduplication"""
        if not self.json_file.exists():
            self.logger.info("No existing articles file found - starting fresh")
            return []
        
        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                articles = json.load(f)
                if isinstance(articles, list):
                    self.logger.info(f"Loaded {len(articles)} existing articles")
                    return articles
                else:
                    self.logger.warning("Existing file format invalid - starting fresh")
                    return []
        except Exception as e:
            self.logger.error(f"Error loading existing articles: {e}")
            return []
    
    def safe_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Make safe request with comprehensive error handling"""
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = random.uniform(3 + attempt * 2, 6 + attempt * 3)
                    self.logger.info(f"Retry {attempt + 1} after {delay:.1f}s for {url}")
                    time.sleep(delay)
                else:
                    time.sleep(random.uniform(1.5, 3.5))
                
                # Rotate user agent occasionally
                if random.random() < 0.3:
                    try:
                        ua = UserAgent()
                        self.session.headers['User-Agent'] = ua.chrome
                    except:
                        pass
                
                response = self.session.get(url, timeout=30, verify=False)
                
                if response.status_code == 200:
                    return response
                elif response.status_code in [403, 429]:
                    self.logger.warning(f"Status {response.status_code} for {url}")
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(10, 15))
                else:
                    self.logger.warning(f"Status {response.status_code} for {url}")
                
            except requests.exceptions.Timeout:
                self.logger.error(f"Timeout for {url} on attempt {attempt + 1}")
            except Exception as e:
                self.logger.error(f"Request error for {url}: {e}")
        
        self.stats['errors'] += 1
        return None
    
    def extract_pdf_content(self, pdf_url: str) -> str:
        """Extract and clean text content from PDF"""
        try:
            # Check if already processed
            pdf_hash = hashlib.md5(pdf_url.encode()).hexdigest()
            if pdf_hash in self.processed_files:
                return ""
            
            self.logger.info(f"Extracting PDF: {os.path.basename(pdf_url)}")
            response = self.safe_request(pdf_url)
            
            if not response:
                return ""
            
            try:
                pdf_reader = PyPDF2.PdfReader(BytesIO(response.content))
            except Exception as e:
                self.logger.error(f"Error reading PDF {pdf_url}: {e}")
                return ""
            
            if len(pdf_reader.pages) == 0:
                self.logger.warning(f"PDF has no pages: {pdf_url}")
                return ""
            
            text_content = []
            for page_num, page in enumerate(pdf_reader.pages):
                try:
                    page_text = page.extract_text()
                    if page_text and page_text.strip():
                        cleaned_text = self.clean_text_for_llm(page_text)
                        if cleaned_text:
                            text_content.append(cleaned_text)
                except Exception as e:
                    self.logger.warning(f"Error extracting PDF page {page_num + 1}: {e}")
                    continue
            
            if text_content:
                full_text = ' '.join(text_content)
                self.processed_files.add(pdf_hash)
                self.stats['pdfs_processed'] += 1
                self.logger.info(f"Successfully extracted {len(full_text)} chars from PDF")
                return full_text
                
        except Exception as e:
            self.logger.error(f"Error extracting PDF {pdf_url}: {e}")
        
        return ""
    
    def extract_excel_content(self, excel_url: str) -> str:
        """Extract comprehensive content from Excel files"""
        if not EXCEL_AVAILABLE:
            self.logger.warning("Excel processing not available - missing pandas/openpyxl")
            return ""
        
        try:
            # Check if already processed
            excel_hash = hashlib.md5(excel_url.encode()).hexdigest()
            if excel_hash in self.processed_files:
                return ""
            
            self.logger.info(f"Extracting Excel: {os.path.basename(excel_url)}")
            response = self.safe_request(excel_url)
            
            if not response:
                return ""
            
            file_extension = os.path.splitext(urlparse(excel_url).path)[1].lower()
            
            try:
                if file_extension == '.csv':
                    # Try different encodings for CSV
                    for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                        try:
                            df = pd.read_csv(BytesIO(response.content), encoding=encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        self.logger.error(f"Could not decode CSV with any encoding: {excel_url}")
                        return ""
                    
                    content = self.process_dataframe_for_llm(df, "CSV Data")
                    if content:
                        self.processed_files.add(excel_hash)
                        self.stats['excel_files_processed'] += 1
                        return content
                        
                else:
                    # Handle Excel files
                    try:
                        excel_file = pd.ExcelFile(BytesIO(response.content), engine='openpyxl')
                    except Exception as e:
                        self.logger.error(f"Error reading Excel file {excel_url}: {e}")
                        return ""
                    
                    # Process all sheets
                    all_sheets_content = []
                    for sheet_name in excel_file.sheet_names:
                        try:
                            df = pd.read_excel(excel_file, sheet_name=sheet_name)
                            sheet_content = self.process_dataframe_for_llm(df, sheet_name)
                            if sheet_content:
                                all_sheets_content.append(sheet_content)
                        except Exception as e:
                            self.logger.warning(f"Error processing sheet '{sheet_name}': {e}")
                    
                    if all_sheets_content:
                        full_content = '\n\n'.join(all_sheets_content)
                        self.processed_files.add(excel_hash)
                        self.stats['excel_files_processed'] += 1
                        return full_content
                
            except Exception as e:
                self.logger.error(f"Error processing Excel file {excel_url}: {e}")
                
        except Exception as e:
            self.logger.error(f"Error extracting Excel {excel_url}: {e}")
        
        return ""
    
    def process_dataframe_for_llm(self, df: pd.DataFrame, sheet_name: str) -> str:
        """Process DataFrame into LLM-friendly format with no limits"""
        try:
            if df.empty:
                return ""
            
            content_parts = [f"DATA SHEET: {sheet_name}"]
            
            # Basic info
            content_parts.append(f"Dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
            
            # All column names
            col_names = list(df.columns.astype(str))
            content_parts.append(f"Columns: {', '.join(col_names)}")
            
            # Data types summary
            try:
                numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                if numeric_cols:
                    content_parts.append(f"Numeric columns: {', '.join(numeric_cols)}")
                    
                text_cols = df.select_dtypes(include=['object']).columns.tolist()
                if text_cols:
                    content_parts.append(f"Text columns: {', '.join(text_cols)}")
                    
                date_cols = df.select_dtypes(include=['datetime']).columns.tolist()
                if date_cols:
                    content_parts.append(f"Date columns: {', '.join(date_cols)}")
            except Exception:
                pass
            
            # Full sample data (first 10 rows instead of 3)
            try:
                sample_df = df.head(10)
                content_parts.append("Sample data (first 10 rows):")
                sample_str = sample_df.to_string(index=False)
                content_parts.append(sample_str)
            except Exception as e:
                self.logger.warning(f"Error displaying sample data: {e}")
            
            # Complete summary statistics for all numeric columns
            try:
                if numeric_cols and len(df) > 0:
                    stats_df = df[numeric_cols].describe()
                    content_parts.append("Complete summary statistics for numeric columns:")
                    content_parts.append(stats_df.to_string())
            except Exception as e:
                self.logger.warning(f"Error generating statistics: {e}")
            
            # Value counts for categorical columns (top 10 values per column)
            try:
                categorical_cols = df.select_dtypes(include=['object']).columns
                for col in categorical_cols:
                    if df[col].nunique() <= 50:  # Only for columns with reasonable number of unique values
                        value_counts = df[col].value_counts().head(10)
                        if not value_counts.empty:
                            content_parts.append(f"Top values in '{col}':")
                            content_parts.append(value_counts.to_string())
            except Exception as e:
                self.logger.warning(f"Error generating value counts: {e}")
            
            return self.clean_text_for_llm('\n'.join(content_parts))
            
        except Exception as e:
            self.logger.warning(f"Error processing DataFrame: {e}")
            return ""
    
    def extract_embedded_links(self, soup: BeautifulSoup, current_url: str) -> List[Dict]:
        """Extract embedded links from article content with strict filtering"""
        embedded_links = []
        
        try:
            # Find main content area
            main_content = (soup.find('main') or 
                          soup.find('article') or 
                          soup.find(class_='field--name-body') or
                          soup.find(class_='content-main') or
                          soup.find('body'))
            
            if not main_content:
                return embedded_links
            
            # Get all links within the main content
            all_links = main_content.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href', '').strip()
                if not href:
                    continue
                
                # Skip navigation and marketing links
                skip_patterns = [
                    '#', 'mailto:', 'tel:', 'javascript:',
                    '/about', '/contact', '/help', '/accessibility',
                    '/privacy', '/terms', '/sitemap', '/search',
                    'facebook.com', 'twitter.com', 'linkedin.com',
                    'youtube.com', 'instagram.com', 'tiktok.com'
                ]
                
                if any(pattern in href.lower() for pattern in skip_patterns):
                    continue
                
                # Convert to absolute URL
                try:
                    if href.startswith('/'):
                        full_url = urljoin(self.base_url, href)
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        continue
                except Exception:
                    continue
                
                # Skip if same as current URL
                if full_url == current_url:
                    continue
                
                link_text = link.get_text(strip=True)
                if link_text:
                    embedded_links.append({
                        'url': full_url,
                        'text': link_text
                    })
                    
                    # Limit to prevent excessive links
                    if len(embedded_links) >= 50:
                        break
        
        except Exception as e:
            self.logger.warning(f"Error extracting embedded links: {e}")
        
        return embedded_links
    
    def extract_article_links(self, soup: BeautifulSoup, page_num: int) -> List[Dict]:
        """Extract article links from listing page"""
        article_links = []
        
        try:
            # Look for article links using multiple selectors
            selectors = [
                'a[href*="/articles/"]',
                'h2 a[href*="/articles/"]',
                'h3 a[href*="/articles/"]',
                '.views-row a[href*="/articles/"]',
                '.node-title a[href*="/articles/"]'
            ]
            
            found_links = set()
            
            for selector in selectors:
                try:
                    links = soup.select(selector)
                    for link in links:
                        href = link.get('href')
                        if href and '/articles/' in href:
                            full_url = urljoin(self.base_url, href).rstrip('/')
                            
                            if full_url in found_links or full_url == self.articles_url:
                                continue
                            
                            if full_url in self.existing_urls:
                                self.stats['articles_skipped'] += 1
                                continue
                            
                            headline = link.get_text(strip=True)
                            if headline and len(headline) > 5:
                                article_links.append({
                                    'url': full_url,
                                    'headline': headline
                                })
                                found_links.add(full_url)
                                self.stats['articles_found'] += 1
                except Exception as e:
                    self.logger.debug(f"Error with selector {selector}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error extracting article links: {e}")
        
        self.logger.info(f"Page {page_num}: Found {len(article_links)} new article links")
        return article_links
    
    def process_embedded_files(self, embedded_links: List[Dict]) -> Dict[str, str]:
        """Process embedded files from links"""
        pdf_content = []
        excel_content = []
        
        try:
            for link_info in embedded_links:
                href = link_info.get('url', '')
                if not href:
                    continue
                    
                href_lower = href.lower()
                
                if href_lower.endswith('.pdf'):
                    pdf_text = self.extract_pdf_content(href)
                    if pdf_text:
                        pdf_content.append(pdf_text)
                elif href_lower.endswith(('.xlsx', '.xls', '.csv')):
                    excel_text = self.extract_excel_content(href)
                    if excel_text:
                        excel_content.append(excel_text)
        
        except Exception as e:
            self.logger.error(f"Error processing embedded files: {e}")
        
        return {
            'pdf_content': ' '.join(pdf_content) if pdf_content else "",
            'excel_content': ' '.join(excel_content) if excel_content else ""
        }
    
    def extract_charts_and_tables(self, soup: BeautifulSoup) -> str:
        """Extract comprehensive information about charts and tables including full table data"""
        chart_table_info = []
        
        try:
            # Find tables and extract complete data
            tables = soup.find_all('table')
            for i, table in enumerate(tables, 1):
                try:
                    # Get table caption
                    caption = table.find('caption')
                    caption_text = caption.get_text(strip=True) if caption else f"Table {i}"
                    
                    table_data = [f"TABLE {i}: {caption_text}"]
                    
                    # Extract headers from thead or first row
                    headers = []
                    thead = table.find('thead')
                    if thead:
                        header_rows = thead.find_all('tr')
                        for header_row in header_rows:
                            row_headers = []
                            for th in header_row.find_all(['th', 'td']):
                                header_text = th.get_text(strip=True)
                                if header_text:
                                    row_headers.append(header_text)
                            if row_headers:
                                headers.extend(row_headers)
                    else:
                        # Try first row if no thead
                        first_row = table.find('tr')
                        if first_row:
                            for th in first_row.find_all(['th', 'td']):
                                header_text = th.get_text(strip=True)
                                if header_text:
                                    headers.append(header_text)
                    
                    if headers:
                        table_data.append(f"Headers: {' | '.join(headers)}")
                    
                    # Extract data from tbody or all rows
                    data_rows = []
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                    else:
                        # Get all rows except the header row
                        all_rows = table.find_all('tr')
                        rows = all_rows[1:] if len(all_rows) > 1 else all_rows
                    
                    for row_idx, row in enumerate(rows):
                        cells = row.find_all(['td', 'th'])
                        if cells:
                            row_data = []
                            for cell in cells:
                                cell_text = cell.get_text(strip=True)
                                if cell_text:
                                    row_data.append(cell_text)
                            
                            if row_data:
                                data_rows.append(' | '.join(row_data))
                        
                        # Limit rows to prevent excessive output (increase if needed)
                        if row_idx >= 20:  # Capture first 20 rows of data
                            if len(rows) > 21:
                                data_rows.append(f"... and {len(rows) - 21} more rows")
                            break
                    
                    if data_rows:
                        table_data.append("Data:")
                        table_data.extend(data_rows)
                    
                    # Join all table information
                    chart_table_info.append('\n'.join(table_data))
                    
                except Exception as e:
                    self.logger.warning(f"Error processing table {i}: {e}")
                    continue
            
            # Find chart containers
            chart_selectors = [
                'div[class*="chart"]',
                'div[class*="graph"]',
                'img[alt*="chart" i]',
                'img[alt*="graph" i]',
                'figure'
            ]
            
            chart_count = 0
            for selector in chart_selectors:
                try:
                    elements = soup.select(selector)
                    for element in elements:
                        chart_count += 1
                        
                        alt_text = element.get('alt', '') if element.name == 'img' else ''
                        title = element.get('title', '')
                        
                        figcaption = element.find('figcaption')
                        caption = figcaption.get_text(strip=True) if figcaption else ''
                        
                        chart_info = f"CHART {chart_count}"
                        if alt_text:
                            chart_info += f": {alt_text}"
                        elif title:
                            chart_info += f": {title}"
                        elif caption:
                            chart_info += f": {caption}"
                        
                        chart_table_info.append(chart_info)
                        
                        # Limit charts to prevent excessive output  
                        if chart_count >= 20:
                            break
                except Exception:
                    continue
                    
                if chart_count >= 10:
                    break
        
        except Exception as e:
            self.logger.warning(f"Error in extract_charts_and_tables: {e}")
        
        return '\n\n'.join(chart_table_info) if chart_table_info else ""
    
    def extract_article_content(self, article_link: Dict) -> Optional[Article]:
        """Extract comprehensive content from individual article page"""
        url = article_link.get('url', '')
        headline = article_link.get('headline', '')
        
        if not url or not headline:
            return None
        
        self.logger.info(f"Extracting: {headline[:60]}...")
        
        response = self.safe_request(url)
        if not response:
            return None
        
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            self.logger.error(f"Error parsing HTML for {url}: {e}")
            return None
        
        try:
            # Generate hash for deduplication
            hash_id = self.generate_hash(url, headline)
            if hash_id in self.existing_hashes:
                self.stats['articles_skipped'] += 1
                return None
            
            # Extract better headline from page
            try:
                h1_tag = soup.find('h1')
                if h1_tag:
                    page_headline = h1_tag.get_text(strip=True)
                    if page_headline and len(page_headline) > len(headline):
                        headline = page_headline
            except Exception:
                pass
            
            # Extract published date
            published_date = self.extract_published_date(soup)
            
            # Extract theme/category
            theme = self.extract_theme(soup, url)
            
            # Extract article type
            article_type = self.determine_article_type(url, headline, soup)
            
            # Extract main content
            content_text = self.extract_main_content(soup)
            
            # Extract image URL
            image_url = self.extract_image_url(soup)
            
            # Extract embedded links (content-focused)
            embedded_links = self.extract_embedded_links(soup, url)
            
            # Extract charts and tables with full data
            charts_tables = self.extract_charts_and_tables(soup)
            
            # Process embedded files
            self.processed_files.clear()  # Reset for each article
            file_content = self.process_embedded_files(embedded_links)
            
            # Create article
            article = Article(
                hash_id=hash_id,
                url=url,
                headline=headline,
                published_date=published_date,
                scraped_date=datetime.now(timezone.utc).isoformat(),
                theme=theme,
                article_type=article_type,
                content_text=content_text,
                pdf_content=file_content['pdf_content'],
                excel_content=file_content['excel_content'],
                image_url=image_url,
                embedded_links=embedded_links,
                charts_and_tables=charts_tables
            )
            
            self.stats['articles_scraped'] += 1
            self.logger.info(f"Successfully scraped: {headline[:50]}... [{article_type}]")
            
            return article
            
        except Exception as e:
            self.logger.error(f"Error extracting content from {url}: {e}")
            self.stats['errors'] += 1
            return None
    
    def extract_published_date(self, soup: BeautifulSoup) -> str:
        """Extract published date from article page"""
        try:
            date_selectors = [
                'meta[name="dcterms.issued"]',
                'meta[property="article:published_time"]',
                'time[datetime]',
                '.field--name-field-abs-release-date time',
                '.date-display-single',
                '.published-date'
            ]
            
            for selector in date_selectors:
                try:
                    element = soup.select_one(selector)
                    if element:
                        date_value = element.get('datetime') or element.get('content')
                        if date_value:
                            return date_value
                        
                        date_text = element.get_text(strip=True)
                        if date_text:
                            return date_text
                except Exception:
                    continue
        except Exception:
            pass
        
        return "Unknown"
    
    def extract_theme(self, soup: BeautifulSoup, url: str) -> str:
        """Extract article theme/category"""
        try:
            # Try breadcrumbs
            breadcrumb = soup.find('nav', class_='breadcrumb') or soup.find('ol', class_='breadcrumb')
            if breadcrumb:
                links = breadcrumb.find_all('a')
                if len(links) > 1:
                    return links[-2].get_text(strip=True)
            
            # Try category meta
            category_meta = soup.find('meta', {'name': 'category'})
            if category_meta:
                return category_meta.get('content', '')
        except Exception:
            pass
        
        return ""
    
    def determine_article_type(self, url: str, headline: str, soup: BeautifulSoup) -> str:
        """Determine article type based on URL, headline, and content"""
        try:
            url_lower = url.lower()
            headline_lower = headline.lower()
            
            if 'media-release' in url_lower or 'media release' in headline_lower:
                return "Media Release"
            elif 'statistics' in url_lower or 'statistical' in headline_lower:
                return "Statistical Report"
            elif 'survey' in headline_lower or 'census' in headline_lower:
                return "Survey/Census"
            elif 'research' in headline_lower:
                return "Research"
            elif 'insights' in headline_lower:
                return "Insights"
            elif 'data' in headline_lower:
                return "Data Release"
        except Exception:
            pass
        
        return "Article"
    
    def extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract comprehensive main content"""
        try:
            # Remove unwanted elements
            for tag_name in ['script', 'style', 'nav', 'header', 'footer', 'aside']:
                for element in soup.find_all(tag_name):
                    element.decompose()
            
            # Remove breadcrumb elements
            for element in soup.find_all(class_='breadcrumb'):
                element.decompose()
            
            # Try content selectors in order of preference
            content_selectors = [
                'main',
                'article',
                '.field--name-body',
                '.content-main',
                '.node__content',
                '.abs-section-content'
            ]
            
            for selector in content_selectors:
                try:
                    content_elem = soup.select_one(selector)
                    if content_elem:
                        text = content_elem.get_text(separator=' ', strip=True)
                        if len(text) > 200:  # Ensure substantial content
                            return self.clean_text_for_llm(text)
                except Exception:
                    continue
            
            # Fallback to body
            try:
                body = soup.find('body')
                if body:
                    text = body.get_text(separator=' ', strip=True)
                    return self.clean_text_for_llm(text)
            except Exception:
                pass
            
        except Exception:
            pass
        
        return ""
    
    def extract_image_url(self, soup: BeautifulSoup) -> str:
        """Extract associated image URL"""
        try:
            # Try Open Graph image
            og_image = soup.find('meta', {'property': 'og:image'})
            if og_image and og_image.get('content'):
                return og_image['content']
            
            # Try Twitter card image
            twitter_image = soup.find('meta', {'name': 'twitter:image'})
            if twitter_image and twitter_image.get('content'):
                return twitter_image['content']
            
            # Try main content image
            main_content = soup.find('main') or soup.find('article')
            if main_content:
                img = main_content.find('img', src=True)
                if img:
                    return urljoin(self.base_url, img['src'])
        except Exception:
            pass
        
        return ""
    
    def scrape_articles(self) -> List[Article]:
        """Main scraping method"""
        all_new_articles = []
        current_page = 0
        consecutive_empty_pages = 0
        
        self.logger.info("=" * 60)
        self.logger.info("STARTING ENHANCED ABS ARTICLES SCRAPING")
        self.logger.info("=" * 60)
        
        while current_page < self.max_pages:
            self.logger.info(f"\n--- Processing page {current_page + 1}/{self.max_pages} ---")
            
            # Construct page URL
            if current_page == 0:
                page_url = self.articles_url
            else:
                page_url = f"{self.articles_url}?page={current_page}"
            
            response = self.safe_request(page_url)
            if not response:
                self.logger.error(f"Failed to fetch page {current_page + 1}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    break
                current_page += 1
                continue
            
            try:
                soup = BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                self.logger.error(f"Error parsing page {current_page + 1}: {e}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    break
                current_page += 1
                continue
            
            article_links = self.extract_article_links(soup, current_page + 1)
            
            if not article_links:
                self.logger.warning(f"No new articles found on page {current_page + 1}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    self.logger.info("Stopping after 3 consecutive empty pages")
                    break
                current_page += 1
                continue
            
            consecutive_empty_pages = 0  # Reset counter
            
            # Process each article
            for i, article_link in enumerate(article_links, 1):
                try:
                    article = self.extract_article_content(article_link)
                    if article:
                        all_new_articles.append(article)
                        self.existing_urls.add(article.url)
                        self.existing_hashes.add(article.hash_id)
                        
                        # Save progress periodically
                        if len(all_new_articles) % 10 == 0:
                            self.logger.info(f"Progress checkpoint: {len(all_new_articles)} articles scraped")
                    
                    # Random delay between articles
                    time.sleep(random.uniform(2, 5))
                    
                except Exception as e:
                    self.logger.error(f"Error processing article {i}: {e}")
                    continue
            
            self.stats['pages_processed'] += 1
            current_page += 1
            
            # Longer delay between pages
            time.sleep(random.uniform(5, 10))
        
        # Final statistics
        self.logger.info("\n" + "=" * 60)
        self.logger.info("SCRAPING COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"Pages processed: {self.stats['pages_processed']}")
        self.logger.info(f"Articles found: {self.stats['articles_found']}")
        self.logger.info(f"Articles scraped: {self.stats['articles_scraped']}")
        self.logger.info(f"Articles skipped: {self.stats['articles_skipped']}")
        self.logger.info(f"PDFs processed: {self.stats['pdfs_processed']}")
        self.logger.info(f"Excel files processed: {self.stats['excel_files_processed']}")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info("=" * 60)
        
        return all_new_articles
    
    def save_data(self, new_articles: List[Article]):
        """Save articles to JSON file only"""
        if not new_articles:
            self.logger.info("No new articles to save")
            return
        
        try:
            # Convert new articles to dict format
            new_articles_data = [asdict(article) for article in new_articles]
            
            # Combine with existing articles
            all_articles = self.existing_articles + new_articles_data
            
            # Remove duplicates based on hash_id
            seen_hashes = set()
            unique_articles = []
            for article in all_articles:
                hash_id = article.get('hash_id')
                if hash_id and hash_id not in seen_hashes:
                    seen_hashes.add(hash_id)
                    unique_articles.append(article)
            
            all_articles = unique_articles
            
            # Sort by published date (newest first)
            def sort_key(article):
                try:
                    pub_date = article.get('published_date', 'Unknown')
                    if pub_date != 'Unknown':
                        return datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                except Exception:
                    pass
                
                # Fallback to scraped date
                try:
                    scraped = article.get('scraped_date', '')
                    return datetime.fromisoformat(scraped.replace('Z', '+00:00'))
                except Exception:
                    return datetime.min
            
            all_articles.sort(key=sort_key, reverse=True)
            
            # Save to JSON only
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(all_articles)} total articles to {self.json_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving JSON file: {e}")
            return
        
        # Print summary
        self.print_summary(new_articles, all_articles)
    
    def print_summary(self, new_articles: List[Article], all_articles: List[Dict]):
        """Print detailed summary of scraping results"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("SUMMARY REPORT")
        self.logger.info("=" * 60)
        
        self.logger.info(f"New articles added: {len(new_articles)}")
        self.logger.info(f"Total articles in database: {len(all_articles)}")
        
        if new_articles:
            # Article types breakdown
            article_types = {}
            pdf_count = 0
            excel_count = 0
            links_count = 0
            tables_count = 0
            
            for article in new_articles:
                # Count types
                art_type = article.article_type or 'Unknown'
                article_types[art_type] = article_types.get(art_type, 0) + 1
                
                # Count content types
                if article.pdf_content:
                    pdf_count += 1
                if article.excel_content:
                    excel_count += 1
                if article.embedded_links:
                    links_count += 1
                if article.charts_and_tables and 'TABLE' in article.charts_and_tables:
                    tables_count += 1
            
            self.logger.info("\nNew articles by type:")
            for art_type, count in sorted(article_types.items(), key=lambda x: x[1], reverse=True):
                self.logger.info(f"  - {art_type}: {count}")
            
            self.logger.info(f"\nContent extraction summary:")
            self.logger.info(f"  - Articles with PDF content: {pdf_count}")
            self.logger.info(f"  - Articles with Excel content: {excel_count}")
            self.logger.info(f"  - Articles with embedded links: {links_count}")
            self.logger.info(f"  - Articles with table data: {tables_count}")
            
            # Sample of new articles
            sample_count = min(3, len(new_articles))
            self.logger.info(f"\nSample of new articles (first {sample_count}):")
            for article in new_articles[:sample_count]:
                self.logger.info(f"  - {article.headline[:70]}...")
                self.logger.info(f"    Date: {article.published_date}")
                self.logger.info(f"    Type: {article.article_type}")
                if article.theme:
                    self.logger.info(f"    Theme: {article.theme}")
                if article.pdf_content:
                    self.logger.info(f"    PDF: Yes ({len(article.pdf_content)} chars)")
                if article.excel_content:
                    self.logger.info(f"    Excel: Yes ({len(article.excel_content)} chars)")
                if article.embedded_links:
                    self.logger.info(f"    Links: {len(article.embedded_links)} embedded")
                if article.charts_and_tables:
                    table_count = article.charts_and_tables.count('TABLE')
                    chart_count = article.charts_and_tables.count('CHART')
                    if table_count > 0 or chart_count > 0:
                        self.logger.info(f"    Tables/Charts: {table_count} tables, {chart_count} charts")
        
        self.logger.info("\n" + "=" * 60)
    
    def run(self):
        """Main execution method"""
        try:
            start_time = datetime.now()
            self.logger.info(f"Enhanced ABS Articles Scraper started at {start_time}")
            
            # Scrape articles
            new_articles = self.scrape_articles()
            
            # Save results
            self.save_data(new_articles)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info(f"\nScraping completed successfully!")
            self.logger.info(f"Total execution time: {duration}")
            
        except KeyboardInterrupt:
            self.logger.info("\nScraping interrupted by user")
            
        except Exception as e:
            self.logger.error(f"\nUnexpected error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
        finally:
            # Close session
            if hasattr(self, 'session') and self.session:
                try:
                    self.session.close()
                except Exception:
                    pass


def main():
    """Main function with configuration"""
    # Configuration for fresh scrape
    MAX_PAGES = 2          # Full scrape of all pages
    FIRST_RUN = True        # Fresh start
    
    print("=" * 80)
    print("Enhanced ABS Articles Scraper for LLM Analysis")
    print("=" * 80)
    print("Features:")
    print("• Comprehensive Excel/CSV content extraction with pandas")
    print("• Enhanced PDF processing with LLM-friendly text cleaning")
    print("• Complete table data extraction (headers + full data rows)")
    print("• Content-focused embedded link filtering (no marketing/nav)")
    print("• Single JSON output (data/abs_all_articles.json)")
    print("• Robust error handling and anti-bot measures")
    print("• Progress checkpoints and detailed statistics")
    print("=" * 80)
    
    # Create and run scraper
    try:
        scraper = EnhancedABSArticlesScraper(max_pages=MAX_PAGES, first_run=FIRST_RUN)
        scraper.run()
    except Exception as e:
        print(f"\nCritical error in main: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()