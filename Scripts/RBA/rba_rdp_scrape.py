#!/usr/bin/env python3
"""
Production-Grade RBA Research Discussion Papers (RDPs) Scraper

Comprehensive scraper that extracts all RDPs from the Reserve Bank of Australia website
with deduplication, robust error handling, and LLM-friendly structured output.

Key Features:
- Extracts main papers, non-technical summaries, and supplementary PDFs
- Handles pagination correctly using RBA's about.html page
- Smart deduplication prevents re-processing existing records
- max_years parameter for efficient daily runs
- Production-grade logging and error handling
- Clean, structured JSON output optimized for LLM ingestion

Usage:
    python rba_rdp_scraper.py                    # Full scrape (all RDPs)
    python rba_rdp_scraper.py --max-years 3      # Daily runs (recent years only)

Author: Production-grade implementation
Version: 2.0.0
"""

import os
import sys
import json
import time
import logging
import requests
import re
import tempfile
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path
from typing import Dict, List, Optional, Set, Union
from dataclasses import dataclass, asdict
import traceback

# Third-party imports with error handling
try:
    import PyPDF2
    import pandas as pd
    from bs4 import BeautifulSoup
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import cloudscraper
except ImportError as e:
    print(f"Error: Missing required dependency: {e}")
    print("Please install dependencies: pip install requests beautifulsoup4 PyPDF2 pandas cloudscraper urllib3")
    sys.exit(1)


@dataclass
class RDPRecord:
    """Data structure for an RDP record with comprehensive metadata"""
    rdp_id: str
    title: str
    theme: str
    published_date: str
    scraped_date: str
    authors: str
    content_paper_pdf: str
    content_summary_pdf: str
    content_supplementary_pdf: str
    content_webpage: str
    tables: str
    associated_image: str
    embedded_links: List[Dict[str, str]]
    url: str
    
    def __post_init__(self):
        """Validate critical fields"""
        if not self.url:
            raise ValueError("URL is required for RDP record")


class RBAScraperError(Exception):
    """Custom exception for RBA scraper errors"""
    pass


class RBAScraper:
    """Production-grade scraper for RBA Research Discussion Papers"""
    
    # Configuration constants
    BASE_URL = "https://www.rba.gov.au"
    RDP_INDEX_URL = "https://www.rba.gov.au/publications/rdp/about.html"
    REQUEST_DELAY = 2.0
    MAX_RETRIES = 3
    TIMEOUT = 30
    PDF_TIMEOUT = 60
    MAX_PAGES = 2  # Safety limit for pagination
    
    def __init__(self, output_dir: str = "data", log_level: str = "INFO", max_years: Optional[int] = None):
        """
        Initialize the RBA scraper with comprehensive validation
        
        Args:
            output_dir: Directory for output files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            max_years: Maximum number of recent years to scrape (None = all years)
        """
        # Validate inputs
        if log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            raise ValueError(f"Invalid log_level: {log_level}. Must be one of: DEBUG, INFO, WARNING, ERROR")
        
        if max_years is not None and (not isinstance(max_years, int) or max_years < 1):
            raise ValueError("max_years must be a positive integer or None")
        
        # Initialize configuration
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.max_years = max_years
        self.log_level = log_level
        
        # Setup logging
        self.logger = None
        self.setup_logging()
        
        # Initialize session and data
        self.session = None
        self.setup_session()
        
        # Data management
        self.output_file = self.output_dir / "rba_research_discussion_papers.json"
        self.existing_records = self.load_existing_records()
        
        # Statistics tracking
        self.stats = {
            'start_time': None,
            'end_time': None,
            'pages_processed': 0,
            'urls_found': 0,
            'urls_processed': 0,
            'urls_skipped': 0,
            'pdf_extractions_attempted': 0,
            'pdf_extractions_successful': 0,
            'errors': []
        }
        
        # Log initialization
        self.logger.info("RBA RDP Scraper initialized")
        self.logger.info(f"Output directory: {self.output_dir.absolute()}")
        self.logger.info(f"Max years: {'All available' if self.max_years is None else self.max_years}")
        self.logger.info(f"Existing records: {len(self.existing_records)}")

    def setup_logging(self) -> None:
        """Setup production-grade logging with proper formatting"""
        # Create logger
        self.logger = logging.getLogger('rba_scraper')
        self.logger.setLevel(getattr(logging, self.log_level.upper()))
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, self.log_level.upper()))
        console_handler.setFormatter(simple_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        try:
            log_file = self.output_dir / f"rba_rdp_scraper_.log"
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)  # Always log everything to file
            file_handler.setFormatter(detailed_formatter)
            self.logger.addHandler(file_handler)
            
            self.logger.info(f"Logging initialized - Console: {self.log_level}, File: DEBUG")
            self.logger.info(f"Log file: {log_file}")
        except Exception as e:
            self.logger.warning(f"Could not setup file logging: {e}")

    def setup_session(self) -> None:
        """Setup HTTP session with proper error handling"""
        try:
            # Use standard requests session (works with your other RBA scrapers)
            self.session = requests.Session()
            
            # Configure retries
            retry_strategy = Retry(
                total=self.MAX_RETRIES,
                status_forcelist=[429, 500, 502, 503, 504],
                backoff_factor=1.0,
                respect_retry_after_header=True
            )
            
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
            
            # Set conservative headers (like your working scrapers)
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            self.logger.info("HTTP session initialized successfully")
            
        except Exception as e:
            raise RBAScraperError(f"Failed to setup HTTP session: {e}")

    def load_existing_records(self) -> Dict[str, Dict]:
        """Load existing records for deduplication with comprehensive error handling"""
        if not self.output_file.exists():
            return {}
        
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                self.logger.warning("Invalid data format in existing records file")
                return {}
            
            # Create lookup dictionary using multiple keys for robust deduplication
            records = {}
            for item in data:
                if not isinstance(item, dict):
                    continue
                    
                # Primary key: RDP ID
                rdp_id = item.get('rdp_id', '')
                if rdp_id:
                    records[rdp_id] = item
                
                # Secondary key: URL (for records without RDP ID)
                url = item.get('url', '')
                if url and rdp_id not in records:
                    records[url] = item
            
            self.logger.info(f"Loaded {len(records)} existing records")
            return records
            
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error in existing records: {e}")
            # Backup corrupted file
            backup_file = self.output_file.with_suffix(f'.backup_{int(time.time())}.json')
            try:
                self.output_file.rename(backup_file)
                self.logger.info(f"Corrupted file backed up as: {backup_file}")
            except Exception:
                pass
            return {}
            
        except Exception as e:
            self.logger.error(f"Error loading existing records: {e}")
            return {}

    def save_records(self, records: List[Union[RDPRecord, Dict]]) -> None:
        """Save records with atomic writes and validation"""
        if not records:
            self.logger.warning("No records to save")
            return
        
        try:
            # Convert records to serializable format
            data = []
            for record in records:
                if isinstance(record, RDPRecord):
                    data.append(asdict(record))
                elif isinstance(record, dict):
                    data.append(record)
                else:
                    self.logger.warning(f"Skipping invalid record type: {type(record)}")
            
            if not data:
                self.logger.error("No valid records to save")
                return
            
            # Atomic write using temporary file
            temp_file = self.output_file.with_suffix('.tmp')
            
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
            
            # Backup existing file
            if self.output_file.exists():
                backup_file = self.output_file.with_suffix(f'.backup_{int(time.time())}.json')
                self.output_file.rename(backup_file)
                self.logger.debug(f"Created backup: {backup_file.name}")
            
            # Move temp file to final location
            temp_file.rename(self.output_file)
            
            # Validate saved file
            with open(self.output_file, 'r', encoding='utf-8') as f:
                json.load(f)
            
            self.logger.info(f"Successfully saved {len(data)} records to {self.output_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving records: {e}")
            # Clean up temp file
            temp_file = self.output_file.with_suffix('.tmp')
            if temp_file.exists():
                temp_file.unlink()
            raise

    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch page content with comprehensive error handling"""
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                if attempt > 0:
                    delay = self.REQUEST_DELAY * (attempt + 1)
                    self.logger.debug(f"Retrying {url} after {delay}s delay")
                    time.sleep(delay)
                
                self.logger.debug(f"Fetching: {url} (attempt {attempt + 1})")
                
                response = self.session.get(url, timeout=self.TIMEOUT)
                response.raise_for_status()
                
                # Basic validation
                if len(response.content) < 1000:
                    raise requests.RequestException("Response too short")
                
                # Parse content
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Validate it's an RBA page
                title = soup.find('title')
                if not title:
                    raise requests.RequestException("No title found")
                
                title_text = title.get_text().lower()
                if not any(indicator in title_text for indicator in ['rba', 'reserve bank', 'australia']):
                    # Additional check for RBA content
                    if not (soup.find(href=lambda x: x and 'rba.gov.au' in x) or 
                           soup.find(class_=lambda x: x and any(term in str(x).lower() for term in ['rba', 'reserve']))):
                        raise requests.RequestException("Page doesn't appear to be RBA content")
                
                self.logger.debug(f"Successfully fetched: {url}")
                return soup
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == self.MAX_RETRIES:
                    self.logger.error(f"All attempts failed for {url}")
                    return None
            except Exception as e:
                self.logger.error(f"Unexpected error fetching {url}: {e}")
                return None
        
        return None

    def extract_pdf_content(self, pdf_url: str) -> str:
        """Extract text from PDF with robust error handling"""
        if not pdf_url:
            return ""
        
        self.stats['pdf_extractions_attempted'] += 1
        temp_path = None
        
        try:
            self.logger.debug(f"Extracting PDF: {pdf_url}")
            
            # Download PDF
            response = self.session.get(pdf_url, timeout=self.PDF_TIMEOUT, stream=True)
            response.raise_for_status()
            
            # Save to temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                temp_path = temp_file.name
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)
            
            # Validate file size
            file_size = Path(temp_path).stat().st_size
            if file_size < 1000:
                raise Exception(f"PDF file too small: {file_size} bytes")
            
            # Extract text using PyPDF2
            text = ""
            with open(temp_path, 'rb') as f:
                try:
                    reader = PyPDF2.PdfReader(f)
                    
                    # Handle encrypted PDFs
                    if reader.is_encrypted:
                        try:
                            reader.decrypt("")
                        except Exception:
                            raise Exception("Could not decrypt encrypted PDF")
                    
                    # Extract text from all pages
                    for page_num, page in enumerate(reader.pages):
                        try:
                            page_text = page.extract_text()
                            if page_text:
                                text += page_text + "\n"
                        except Exception as e:
                            self.logger.debug(f"Error extracting page {page_num + 1}: {e}")
                            continue
                            
                except Exception as e:
                    # Try alternative PDF libraries if available
                    try:
                        import pdfplumber
                        with pdfplumber.open(temp_path) as pdf:
                            for page in pdf.pages:
                                page_text = page.extract_text()
                                if page_text:
                                    text += page_text + "\n"
                    except ImportError:
                        raise e
                    except Exception:
                        raise e
            
            # Clean extracted text
            text = self.clean_text(text)
            
            if len(text.strip()) < 100:
                raise Exception(f"Extracted text too short: {len(text)} characters")
            
            self.stats['pdf_extractions_successful'] += 1
            self.logger.debug(f"Successfully extracted {len(text)} characters from PDF")
            return text
            
        except Exception as e:
            self.logger.error(f"PDF extraction failed for {pdf_url}: {e}")
            self.stats['errors'].append(f"PDF extraction failed: {pdf_url} - {e}")
            return ""
            
        finally:
            # Clean up temporary file
            if temp_path and Path(temp_path).exists():
                try:
                    Path(temp_path).unlink()
                except Exception:
                    pass

    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Remove control characters but preserve essential punctuation
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', text)
        
        # Remove common PDF artifacts
        text = re.sub(r'\f', ' ', text)  # Form feeds
        
        # Remove boilerplate patterns
        boilerplate_patterns = [
            r'Reserve Bank of Australia.*?All rights reserved\.?',
            r'©.*?Reserve Bank of Australia.*?\d{4}',
            r'This work is licensed under.*?Creative Commons.*?',
            r'Page \d+ of \d+',
            r'Printed from.*?on \d{2}/\d{2}/\d{4}',
            r'ISSN \d{4}-\d{4}',
            r'RBA\.GOV\.AU'
        ]
        
        for pattern in boilerplate_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Final cleanup
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def extract_embedded_links(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract academically relevant embedded links"""
        links = []
        seen_urls = set()
        
        # Focus on content areas
        content_selectors = [
            'div#content', '.rss-rdp-description', 'section', '.box-article-info'
        ]
        
        containers = []
        for selector in content_selectors:
            containers.extend(soup.select(selector))
        
        if not containers:
            containers = soup.find_all(['p', 'ul', 'li'])
        
        for container in containers:
            for link in container.find_all('a', href=True):
                href = link['href']
                link_text = link.get_text(strip=True)
                
                if not href or not link_text:
                    continue
                
                # Skip navigation and non-academic links
                skip_terms = [
                    'about', 'contact', 'privacy', 'terms', 'careers', 'subscribe',
                    'facebook', 'twitter', 'linkedin', 'instagram', 'youtube',
                    'share', 'print', 'email', 'search', 'navigation', 'menu'
                ]
                
                if any(term in link_text.lower() or term in href.lower() for term in skip_terms):
                    continue
                
                # Include relevant academic/research links
                relevant_terms = [
                    'doi.org', '/rdp/', '.pdf', 'supplement', 'appendix', 'data',
                    'github', 'research', 'paper', 'study', 'economic', 'policy',
                    'working paper', 'discussion paper'
                ]
                
                is_relevant = (
                    any(term in link_text.lower() or term in href.lower() for term in relevant_terms) or
                    (href.startswith('http') and 'rba.gov.au' not in href and len(link_text) > 10)
                )
                
                if is_relevant:
                    # Convert relative URLs
                    if href.startswith('/'):
                        href = urljoin(self.BASE_URL, href)
                    
                    if href not in seen_urls:
                        seen_urls.add(href)
                        links.append({
                            'url': href,
                            'text': link_text,
                            'title': link.get('title', '')
                        })
        
        return links

    def extract_tables_as_text(self, soup: BeautifulSoup) -> str:
        """Extract tables as structured text"""
        tables_text = []
        
        for table_num, table in enumerate(soup.find_all('table'), 1):
            try:
                rows = []
                
                # Process all rows
                for row in table.find_all('tr'):
                    cells = []
                    for cell in row.find_all(['td', 'th']):
                        cell_text = cell.get_text(strip=True)
                        cells.append(cell_text if cell_text else '')
                    
                    if cells and any(cell for cell in cells):
                        rows.append(' | '.join(cells))
                
                if rows:
                    table_text = f"Table {table_num}:\n" + '\n'.join(rows)
                    tables_text.append(table_text)
                    
            except Exception as e:
                self.logger.debug(f"Error extracting table {table_num}: {e}")
        
        return '\n\n'.join(tables_text)

    def extract_rdp_details(self, rdp_url: str) -> Optional[RDPRecord]:
        """Extract comprehensive details from an RDP page"""
        soup = self.get_page_content(rdp_url)
        if not soup:
            return None
        
        try:
            # Extract RDP ID from URL
            rdp_id = ""
            patterns = [
                r'/rdp/(\d{4}/\d{4}-\d{2})\.html',
                r'/(\d{4}-\d{2})\.html'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, rdp_url)
                if match:
                    rdp_id = match.group(1)
                    if '/' not in rdp_id:
                        # Add year prefix if not present
                        year = rdp_id[:4]
                        rdp_id = f"{year}/{rdp_id}"
                    break
            
            # Extract title
            title = ""
            title_elem = soup.find('h1', class_='page-title')
            if title_elem:
                # Remove publication name prefix
                span = title_elem.find('span', class_='publication-name')
                if span:
                    span.extract()
                title = re.sub(r'\s+', ' ', title_elem.get_text(strip=True))
            
            # Extract authors
            authors = ""
            author_elem = soup.find('p', class_='author')
            if author_elem:
                authors = re.sub(r'<[^>]+>', '', author_elem.get_text(strip=True))
                authors = re.sub(r'\s+', ' ', authors)
            
            # Extract published date
            published_date = ""
            info_div = soup.find('div', class_='box-article-info')
            if info_div:
                date_patterns = [r'(\d{1,2}\s+\w+\s+\d{4})', r'(\w+\s+\d{4})']
                info_text = info_div.get_text()
                for pattern in date_patterns:
                    match = re.search(pattern, info_text)
                    if match:
                        published_date = match.group(1).strip()
                        break
            
            # Extract theme
            theme = ""
            tags_div = soup.find('div', class_='js-tags')
            if tags_div:
                theme_text = tags_div.get_text(strip=True)
                if theme_text:
                    tags = [tag.strip() for tag in theme_text.split(',') if tag.strip()]
                    theme = ', '.join(tags)
            
            # Extract webpage content
            content_parts = []
            
            # Abstract
            abstract_section = soup.find('div', class_='rss-rdp-description')
            if abstract_section:
                abstract = abstract_section.get_text(strip=True)
                if abstract:
                    content_parts.append(f"ABSTRACT:\n{abstract}")
            
            # Main content
            main_section = soup.find('section')
            if main_section:
                # Remove navigation elements
                for nav_elem in main_section.find_all(['nav', 'aside']):
                    nav_elem.decompose()
                
                main_content = main_section.get_text(strip=True)
                if main_content and main_content not in str(content_parts):
                    content_parts.append(f"CONTENT:\n{main_content}")
            
            webpage_content = self.clean_text('\n\n'.join(content_parts))
            
            # Extract PDF content
            paper_pdf_content = ""
            summary_pdf_content = ""
            supplementary_pdf_content = ""
            
            if info_div:
                for link in info_div.find_all('a', href=True):
                    href = link['href']
                    link_text = link.get_text().lower()
                    
                    if href.endswith('.pdf'):
                        pdf_url = urljoin(self.BASE_URL, href) if href.startswith('/') else href
                        
                        if any(term in link_text for term in ['non-technical', 'summary']):
                            summary_pdf_content = self.extract_pdf_content(pdf_url)
                        elif any(term in link_text for term in ['supplement', 'supplementary']):
                            supplementary_pdf_content = self.extract_pdf_content(pdf_url)
                        elif any(term in link_text for term in ['download', 'paper']) or '/rdp20' in href:
                            paper_pdf_content = self.extract_pdf_content(pdf_url)
            
            # Check for HTML summary if PDF not found
            if not summary_pdf_content:
                summary_link = soup.find('a', href=lambda x: x and 'non-technical-summary.html' in x)
                if summary_link:
                    summary_url = urljoin(self.BASE_URL, summary_link['href'])
                    summary_soup = self.get_page_content(summary_url)
                    if summary_soup:
                        content_div = summary_soup.find('div', {'id': 'content'})
                        if content_div:
                            summary_pdf_content = self.clean_text(content_div.get_text())
            
            # Extract additional content
            tables_content = self.extract_tables_as_text(soup)
            embedded_links = self.extract_embedded_links(soup)
            
            # Associated images (rare but check)
            associated_image = ""
            for img in soup.find_all('img', src=True):
                src = img['src']
                if not any(skip in src.lower() for skip in ['icon', 'logo', 'nav']):
                    associated_image = urljoin(self.BASE_URL, src) if src.startswith('/') else src
                    break
            
            # Create record
            record = RDPRecord(
                rdp_id=rdp_id or f"unknown_{int(time.time())}",
                title=title or "Untitled RDP",
                theme=theme,
                published_date=published_date,
                scraped_date=datetime.now().isoformat(),
                authors=authors,
                content_paper_pdf=paper_pdf_content,
                content_summary_pdf=summary_pdf_content,
                content_supplementary_pdf=supplementary_pdf_content,
                content_webpage=webpage_content,
                tables=tables_content,
                associated_image=associated_image,
                embedded_links=embedded_links,
                url=rdp_url
            )
            
            self.stats['urls_processed'] += 1
            self.logger.info(f"Extracted RDP: {rdp_id} - {title[:50]}...")
            return record
            
        except Exception as e:
            self.logger.error(f"Error extracting RDP from {rdp_url}: {e}")
            self.stats['errors'].append(f"RDP extraction failed: {rdp_url} - {e}")
            return None

    def get_all_rdp_urls(self) -> Set[str]:
        """Get RDP URLs by iterating through year-based pages"""
        rdp_urls = set()
        current_year = datetime.now().year
        
        # Determine year range based on max_years
        if self.max_years is None:
            # Full scrape: go back to 1975 when RDPs started
            start_year = current_year
            end_year = 1974  # Will iterate down to 1975
            self.logger.info(f"Full scrape: Processing years {current_year} back to 1975")
        else:
            # Limited scrape: only recent years
            start_year = current_year
            end_year = current_year - self.max_years
            self.logger.info(f"Limited scrape: Processing {self.max_years} years from {current_year} to {end_year + 1}")
        
        # Process each year
        for year in range(start_year, end_year, -1):
            year_url = f"{self.BASE_URL}/publications/rdp/{year}/"
            
            self.logger.info(f"Processing year {year}: {year_url}")
            soup = self.get_page_content(year_url)
            
            if not soup:
                self.logger.warning(f"Failed to fetch year {year}")
                continue
            
            # Extract RDP URLs from this year's page
            year_urls = self._extract_rdp_urls_from_year_page(soup, year)
            
            if year_urls:
                rdp_urls.update(year_urls)
                self.logger.info(f"Found {len(year_urls)} RDPs for year {year}")
                self.stats['pages_processed'] += 1
            else:
                self.logger.info(f"No RDPs found for year {year}")
                
                # If it's a recent year and we found nothing, that's suspicious
                if year >= current_year - 2:
                    self.logger.warning(f"No RDPs found for recent year {year} - check if year exists")
            
            # Be polite to the server
            time.sleep(self.REQUEST_DELAY)
        
        self.stats['urls_found'] = len(rdp_urls)
        self.logger.info(f"Discovered {len(rdp_urls)} unique RDP URLs across {abs(start_year - end_year)} years")
        return rdp_urls
    
    def _extract_rdp_urls_from_year_page(self, soup: BeautifulSoup, year: int) -> Set[str]:
        """Extract individual RDP URLs from a year page"""
        urls = set()
        
        # Look for individual RDP links with the pattern: /publications/rdp/YYYY/YYYY-NN.html
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Match the exact pattern we found in the diagnostic
            if re.search(rf'/publications/rdp/{year}/{year}-\d{{2}}\.html', href):
                url = urljoin(self.BASE_URL, href) if href.startswith('/') else href
                urls.add(url)
                self.logger.debug(f"Found RDP: {href}")
        
        # Alternative method: look for any links containing the year and ending in .html
        if not urls:
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Broader pattern for RDPs in this year
                if (f'/{year}/' in href and 
                    href.endswith('.html') and 
                    '/rdp/' in href and
                    not any(skip in href.lower() for skip in ['full', 'sections', 'reference'])):
                    
                    url = urljoin(self.BASE_URL, href) if href.startswith('/') else href
                    urls.add(url)
                    self.logger.debug(f"Found RDP (alt method): {href}")
        
        return urls
    
    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if pagination has more pages"""
        pagination = soup.find('div', class_='pagination')
        if pagination:
            # Look for "Next" link
            next_link = pagination.find('a', class_='next')
            if next_link and next_link.get('href'):
                return True
            
            # Look for higher page numbers
            current_page = pagination.find('a', class_='current')
            if current_page:
                page_links = pagination.find_all('a', class_='pagenum')
                return len(page_links) > 1
        
        return False

    def run(self) -> None:
        """Main execution with comprehensive error handling and statistics"""
        self.stats['start_time'] = datetime.now()
        
        try:
            self.logger.info("Starting RBA RDP scraper")
            
            # Phase 1: Discover RDP URLs
            self.logger.info("Phase 1: Discovering RDP URLs...")
            rdp_urls = self.get_all_rdp_urls()
            
            if not rdp_urls:
                raise RBAScraperError("No RDP URLs discovered")
            
            # Phase 2: Deduplication
            self.logger.info("Phase 2: Checking for existing records...")
            new_urls = []
            existing_keys = set(self.existing_records.keys())
            
            for url in rdp_urls:
                # Check URL directly
                if url in existing_keys:
                    self.stats['urls_skipped'] += 1
                    continue
                
                # Check RDP ID if extractable
                rdp_id_match = re.search(r'/rdp/(\d{4}/\d{4}-\d{2})\.html', url)
                if rdp_id_match:
                    rdp_id = rdp_id_match.group(1)
                    if rdp_id in existing_keys:
                        self.stats['urls_skipped'] += 1
                        continue
                
                new_urls.append(url)
            
            self.logger.info(f"Found {len(new_urls)} new RDPs (skipped {self.stats['urls_skipped']} existing)")
            
            if not new_urls:
                self.logger.info("No new RDPs to process")
                self._log_final_statistics([], [])
                return
            
            # Phase 3: Extract content
            self.logger.info(f"Phase 3: Extracting content from {len(new_urls)} RDPs...")
            new_records = []
            failed_urls = []
            
            for i, url in enumerate(new_urls, 1):
                self.logger.info(f"Processing RDP {i}/{len(new_urls)}: {url}")
                
                try:
                    record = self.extract_rdp_details(url)
                    if record:
                        new_records.append(record)
                    else:
                        failed_urls.append(url)
                        
                except Exception as e:
                    failed_urls.append(url)
                    self.logger.error(f"Failed to process {url}: {e}")
                    self.stats['errors'].append(f"Processing failed: {url} - {e}")
                
                # Rate limiting
                if i < len(new_urls):
                    time.sleep(self.REQUEST_DELAY)
            
            # Phase 4: Save results
            self.logger.info("Phase 4: Saving results...")
            all_records = list(self.existing_records.values()) + new_records
            
            if all_records:
                self.save_records(all_records)
            
            self._log_final_statistics(new_records, failed_urls)
            
        except Exception as e:
            self.logger.error(f"Critical error: {e}")
            self.logger.debug(traceback.format_exc())
            raise
        
        finally:
            self.stats['end_time'] = datetime.now()

    def _log_final_statistics(self, new_records: List[RDPRecord], failed_urls: List[str]) -> None:
        """Log comprehensive final statistics"""
        if not self.stats['end_time']:
            self.stats['end_time'] = datetime.now()
        
        duration = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info("=" * 70)
        self.logger.info("SCRAPING COMPLETED")
        self.logger.info("=" * 70)
        
        # Core statistics
        self.logger.info(f"Runtime: {duration}")
        self.logger.info(f"Pages processed: {self.stats['pages_processed']}")
        self.logger.info(f"URLs discovered: {self.stats['urls_found']}")
        self.logger.info(f"URLs processed: {self.stats['urls_processed']}")
        self.logger.info(f"URLs skipped (existing): {self.stats['urls_skipped']}")
        self.logger.info(f"New records created: {len(new_records)}")
        self.logger.info(f"Failed extractions: {len(failed_urls)}")
        self.logger.info(f"Total records in database: {len(self.existing_records) + len(new_records)}")
        
        # PDF extraction statistics
        if self.stats['pdf_extractions_attempted'] > 0:
            success_rate = (self.stats['pdf_extractions_successful'] / 
                           self.stats['pdf_extractions_attempted'] * 100)
            self.logger.info(f"PDF extractions: {self.stats['pdf_extractions_successful']}/{self.stats['pdf_extractions_attempted']} ({success_rate:.1f}%)")
        
        # Performance metrics
        if new_records:
            avg_time = duration.total_seconds() / len(new_records)
            self.logger.info(f"Average time per RDP: {avg_time:.2f} seconds")
            
            # Content analysis
            total_chars = sum(len(r.content_paper_pdf) + len(r.content_webpage) 
                             for r in new_records if isinstance(r, RDPRecord))
            avg_chars = total_chars / len(new_records) if new_records else 0
            self.logger.info(f"Average content per record: {avg_chars:,.0f} characters")
            
            # Content type statistics
            with_pdf = sum(1 for r in new_records if isinstance(r, RDPRecord) and r.content_paper_pdf)
            with_summary = sum(1 for r in new_records if isinstance(r, RDPRecord) and r.content_summary_pdf)
            with_tables = sum(1 for r in new_records if isinstance(r, RDPRecord) and r.tables)
            
            self.logger.info(f"Records with PDF content: {with_pdf}/{len(new_records)} ({with_pdf/len(new_records)*100:.1f}%)")
            self.logger.info(f"Records with summaries: {with_summary}/{len(new_records)} ({with_summary/len(new_records)*100:.1f}%)")
            self.logger.info(f"Records with tables: {with_tables}/{len(new_records)} ({with_tables/len(new_records)*100:.1f}%)")
        
        # Error reporting
        if failed_urls:
            self.logger.warning(f"Failed URLs ({len(failed_urls)}):")
            for url in failed_urls[:5]:
                self.logger.warning(f"  {url}")
            if len(failed_urls) > 5:
                self.logger.warning(f"  ... and {len(failed_urls) - 5} more")
        
        if self.stats['errors']:
            self.logger.warning(f"Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:3]:
                self.logger.warning(f"  {error}")
        
        # Success indicators
        if new_records:
            self.logger.info(f"Output file: {self.output_file}")
            if len(failed_urls) / (len(new_records) + len(failed_urls)) < 0.1:
                self.logger.info("SUCCESS: High extraction success rate")
            else:
                self.logger.warning("PARTIAL: Some extractions failed")
        
        self.logger.info("=" * 70)


def main():
    """Main entry point with comprehensive argument handling"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Production-grade RBA Research Discussion Papers Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              # Full scrape (all years, ~4 hours)
  %(prog)s --max-years 3               # Daily run (last 3 years, ~10 minutes)  
  %(prog)s --max-years 1 --log-level DEBUG  # Debug recent year only
  %(prog)s --output-dir /data/rba      # Custom output directory

For daily automation:
  0 2 * * * cd /path/to/scraper && python %(prog)s --max-years 3
        """
    )
    
    parser.add_argument('--output-dir', default='data',
                       help='Output directory for results (default: data)')
    
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level (default: INFO)')
    
    parser.add_argument('--max-years', type=int, default=None,
                       help='Limit to recent N years (None=all years, 3=daily runs)')
    
    parser.add_argument('--version', action='version', version='%(prog)s 2.0.0')
    
    # Parse arguments
    args = parser.parse_args()
    
    try:
        # Validate arguments
        if args.max_years is not None and args.max_years < 1:
            parser.error("max-years must be positive")
        
        # Create and run scraper
        scraper = RBAScraper(
            output_dir=args.output_dir,
            log_level=args.log_level,
            max_years=args.max_years
        )
        
        scraper.run()
        
        print("\nScraping completed successfully!")
        print(f"Results saved to: {scraper.output_file}")
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        sys.exit(130)
        
    except RBAScraperError as e:
        print(f"\nScraper error: {e}")
        sys.exit(1)
        
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        print("Check log files for detailed error information")
        sys.exit(1)


if __name__ == "__main__":
    main()