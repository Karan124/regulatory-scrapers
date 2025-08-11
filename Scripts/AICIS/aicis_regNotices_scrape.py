#!/usr/bin/env python3
"""
AICIS Regulatory Notices Scraper

Scrapes regulatory notices from the Australian Industrial Chemicals Introduction Scheme (AICIS) website
with anti-bot measures, pagination support, and comprehensive data extraction.
"""

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Scraping limits
MAX_PAGES = 1  # Set to None for all pages, or specify a number (e.g., 3 for daily runs)
REQUEST_DELAY = 1  # Seconds between individual notice requests
PAGE_DELAY = 2     # Seconds between pagination requests
TIMEOUT = 30       # Request timeout in seconds

# Paths
DATA_DIR = "./data"
OUTPUT_FILE = "aicis_reg_notices.json"
LOG_FILE = "scraper.log"

# Website URLs
BASE_URL = "https://www.industrialchemicals.gov.au"
START_URL = f"{BASE_URL}/news-and-notices/regulatory-notices"

# Content extraction settings
MIN_CONTENT_LENGTH = 50  # Minimum content length to consider valid
MAX_PDF_SIZE_MB = 50     # Maximum PDF size to download (MB)

# ============================================================================

import os
import json
import hashlib
import logging
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import PyPDF2
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import undetected_chromedriver as uc


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class AICISScraper:
    """Main scraper class for AICIS regulatory notices."""
    
    def __init__(self, max_pages: Optional[int] = None):
        """
        Initialize the scraper.
        
        Args:
            max_pages: Maximum number of pages to scrape. None for all pages.
                      If not specified, uses MAX_PAGES from configuration.
        """
        # Use parameter if provided, otherwise use global config
        self.max_pages = max_pages if max_pages is not None else MAX_PAGES
        
        self.base_url = BASE_URL
        self.start_url = START_URL
        self.data_dir = Path(DATA_DIR)
        self.data_file = self.data_dir / OUTPUT_FILE
        self.log_file = self.data_dir / LOG_FILE
        
        # Create data directory
        self.data_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize session and driver
        self.session = None
        self.driver = None
        self.existing_notices = self._load_existing_notices()
        
        # Statistics
        self.stats = {
            'pages_visited': 0,
            'notices_found': 0,
            'new_notices': 0,
            'errors': 0,
            'start_time': datetime.now().isoformat(),
            'max_pages_limit': self.max_pages
        }
    
    def _setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def set_max_pages(self, max_pages: Optional[int]):
        """
        Set the maximum number of pages to scrape.
        
        Args:
            max_pages: Maximum pages to scrape, or None for all pages
        """
        self.max_pages = max_pages
        self.stats['max_pages_limit'] = max_pages
        self.logger.info(f"Max pages limit set to: {max_pages or 'All pages'}")
    
    def get_current_config(self) -> Dict[str, Any]:
        """Get current configuration settings."""
        return {
            'max_pages': self.max_pages,
            'request_delay': REQUEST_DELAY,
            'page_delay': PAGE_DELAY,
            'timeout': TIMEOUT,
            'min_content_length': MIN_CONTENT_LENGTH,
            'max_pdf_size_mb': MAX_PDF_SIZE_MB,
            'data_dir': str(self.data_dir),
            'output_file': str(self.data_file),
            'base_url': self.base_url
        }
    
    def _safe_datetime_convert(self, dt_obj):
        """Safely convert datetime objects to ISO format strings."""
        if isinstance(dt_obj, datetime):
            return dt_obj.isoformat()
        return dt_obj
    
    def _load_existing_notices(self) -> Dict[str, Dict]:
        """Load existing notices from JSON file."""
        if self.data_file.exists():
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {notice['url']: notice for notice in data.get('notices', [])}
            except Exception as e:
                self.logger.error(f"Error loading existing notices: {e}")
        return {}
    
    def _save_notices(self, notices: List[Dict]):
        """Save notices to JSON file."""
        # Ensure all datetime objects in stats are converted to strings
        stats_copy = {}
        for key, value in self.stats.items():
            if isinstance(value, datetime):
                stats_copy[key] = value.isoformat()
            else:
                stats_copy[key] = value
        
        output_data = {
            'scrape_metadata': {
                'last_updated': datetime.now().isoformat(),
                'total_notices': len(notices),
                'scraper_version': '1.0',
                'stats': stats_copy
            },
            'notices': notices
        }
        
        with open(self.data_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)
        
        self.logger.info(f"Saved {len(notices)} notices to {self.data_file}")
    
    def _setup_session(self):
        """Setup requests session with retry strategy and realistic headers."""
        self.session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Realistic headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _setup_driver(self):
        """Setup undetected Chrome driver for anti-bot measures."""
        try:
            options = uc.ChromeOptions()
            
            # Basic options that work with newer Chrome versions
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')
            options.add_argument('--disable-javascript')
            options.add_argument('--disable-blink-features=AutomationControlled')
            
            # Remove automation indicators (newer syntax)
            prefs = {
                "profile.default_content_setting_values": {
                    "notifications": 2,
                    "media_stream": 2,
                }
            }
            options.add_experimental_option("prefs", prefs)
            
            # Try to create driver with minimal options first
            try:
                self.driver = uc.Chrome(options=options, version_main=None)
            except Exception as e:
                self.logger.warning(f"Failed with undetected-chromedriver, trying regular selenium: {e}")
                # Fallback to regular selenium
                from selenium.webdriver.chrome.options import Options as ChromeOptions
                chrome_options = ChromeOptions()
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--disable-blink-features=AutomationControlled')
                
                from selenium.webdriver import Chrome
                self.driver = Chrome(options=chrome_options)
            
            # Execute script to remove webdriver property
            try:
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            except:
                pass  # Not critical if this fails
            
            # Get session cookies by visiting homepage first
            self.logger.info("Initializing session by visiting homepage...")
            self.driver.get(self.base_url)
            time.sleep(3)
            
            # Transfer cookies to requests session
            if self.session:
                for cookie in self.driver.get_cookies():
                    self.session.cookies.set(cookie['name'], cookie['value'])
            
        except Exception as e:
            self.logger.error(f"Error setting up driver: {e}")
            # Don't raise - we can try to continue with requests session only
            self.driver = None
            self.logger.warning("Continuing without driver - using requests session only")
    
    def _get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Get page content using both driver and session fallback."""
        try:
            # Try with driver first (for anti-bot measures)
            if self.driver:
                self.driver.get(url)
                time.sleep(2)
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                if soup.find('div', class_='content'):  # Check if we got valid content
                    return soup
            
            # Fallback to requests session
            if self.session:
                self.session.headers['Referer'] = self.base_url
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
                
        except Exception as e:
            self.logger.error(f"Error getting page content for {url}: {e}")
            self.stats['errors'] += 1
        
        return None
    
    def _extract_notice_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract notice links from the listing page."""
        links = []
        
        # Find all notice articles
        articles = soup.find_all('article', class_='node--type-news')
        
        for article in articles:
            # Find the title link
            title_link = article.find('h3', class_='teaser__title')
            if title_link:
                link_elem = title_link.find('a')
                if link_elem and link_elem.get('href'):
                    full_url = urljoin(self.base_url, link_elem['href'])
                    links.append(full_url)
        
        return links
    
    def _extract_published_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract published date from notice page."""
        # Look for published date in various locations
        date_selectors = [
            'time[datetime]',
            '.field--name-field-published-date time',
            '.health-field--name-field-published-date time'
        ]
        
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                return date_elem.get('datetime') or date_elem.get_text().strip()
        
        return None
    
    def _extract_content_text(self, soup: BeautifulSoup) -> str:
        """Extract clean content text suitable for LLM processing."""
        content_text = []
        
        # Main content areas
        content_selectors = [
            '.field--name-body',
            '.layout__region--content .content',
            '.node--view-mode-full .content'
        ]
        
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                # Remove navigation, social sharing, and other non-content elements
                for elem in content_div.find_all(['nav', '.social-share', '.pager', '.breadcrumb']):
                    elem.decompose()
                
                text = content_div.get_text(separator='\n', strip=True)
                if text:
                    content_text.append(text)
        
        return '\n\n'.join(content_text)
    
    def _extract_tables_and_charts(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract table and chart data from HTML."""
        tables_data = []
        
        # Find all tables
        tables = soup.find_all('table')
        for i, table in enumerate(tables):
            try:
                # Convert table to list of dictionaries
                rows = []
                headers = []
                
                # Get headers
                header_row = table.find('tr')
                if header_row:
                    headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
                
                # Get data rows
                for row in table.find_all('tr')[1:]:  # Skip header row
                    cells = [td.get_text().strip() for td in row.find_all(['td', 'th'])]
                    if cells and len(cells) == len(headers):
                        rows.append(dict(zip(headers, cells)))
                
                if rows:
                    tables_data.append({
                        'table_index': i,
                        'headers': headers,
                        'data': rows,
                        'raw_html': str(table)
                    })
                    
            except Exception as e:
                self.logger.warning(f"Error extracting table {i}: {e}")
        
        return tables_data
    
    def _extract_related_links(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract related links from content (excluding navigation and ads)."""
        links = []
        
        # Find links within content areas
        content_area = soup.find('div', class_='layout__region--content') or soup
        
        for link in content_area.find_all('a', href=True):
            href = link.get('href')
            text = link.get_text().strip()
            
            # Skip empty links, navigation, and social media
            if not text or not href:
                continue
            
            # Skip navigation and social links
            skip_patterns = [
                'facebook.com', 'twitter.com', 'linkedin.com',
                'mailto:', '#', 'javascript:',
                '/news-and-notices', '/home'
            ]
            
            if any(pattern in href.lower() for pattern in skip_patterns):
                continue
            
            # Make absolute URL
            full_url = urljoin(self.base_url, href)
            
            links.append({
                'text': text,
                'url': full_url,
                'type': self._classify_link_type(href)
            })
        
        return links
    
    def _classify_link_type(self, href: str) -> str:
        """Classify link type based on URL."""
        href_lower = href.lower()
        
        if href_lower.endswith('.pdf'):
            return 'pdf'
        elif href_lower.endswith(('.xlsx', '.xls')):
            return 'excel'
        elif href_lower.endswith('.csv'):
            return 'csv'
        elif href_lower.endswith(('.jpg', '.jpeg', '.png', '.gif')):
            return 'image'
        else:
            return 'webpage'
    
    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download and extract text from PDF."""
        try:
            # Check file size first
            head_response = self.session.head(pdf_url, timeout=TIMEOUT)
            content_length = head_response.headers.get('content-length')
            if content_length and int(content_length) > MAX_PDF_SIZE_MB * 1024 * 1024:
                self.logger.warning(f"PDF too large ({content_length} bytes), skipping: {pdf_url}")
                return None
                
            response = self.session.get(pdf_url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Save temporarily and extract text
            temp_path = self.data_dir / "temp.pdf"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            # Extract text using PyPDF2
            text_content = []
            with open(temp_path, 'rb') as f:
                pdf_reader = PyPDF2.PdfReader(f)
                for page in pdf_reader.pages:
                    text = page.extract_text()
                    if text.strip():
                        text_content.append(text)
            
            # Clean up temp file
            temp_path.unlink()
            
            # Clean and return text
            full_text = '\n\n'.join(text_content)
            # Remove excessive whitespace
            full_text = re.sub(r'\n\s*\n', '\n\n', full_text)
            full_text = re.sub(r'[ \t]+', ' ', full_text)
            
            return full_text.strip()
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF text from {pdf_url}: {e}")
            return None
    
    def _extract_csv_data(self, csv_url: str) -> Optional[List[Dict]]:
        """Download and extract data from CSV."""
        try:
            response = self.session.get(csv_url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Parse CSV
            df = pd.read_csv(response.content.decode('utf-8'))
            return df.to_dict('records')
            
        except Exception as e:
            self.logger.error(f"Error extracting CSV data from {csv_url}: {e}")
            return None
    
    def _extract_excel_data(self, excel_url: str) -> Optional[Dict]:
        """Download and extract data from Excel file."""
        try:
            response = self.session.get(excel_url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Save temporarily and read Excel
            temp_path = self.data_dir / "temp.xlsx"
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            # Read all sheets
            excel_data = {}
            with pd.ExcelFile(temp_path) as xls:
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    excel_data[sheet_name] = df.to_dict('records')
            
            # Clean up temp file
            temp_path.unlink()
            
            return excel_data
            
        except Exception as e:
            self.logger.error(f"Error extracting Excel data from {excel_url}: {e}")
            return None
    
    def _generate_notice_hash(self, url: str, content: str) -> str:
        """Generate hash for deduplication."""
        return hashlib.md5(f"{url}:{content[:1000]}".encode()).hexdigest()
    
    def _scrape_notice(self, notice_url: str) -> Optional[Dict]:
        """Scrape individual notice."""
        try:
            self.logger.info(f"Scraping notice: {notice_url}")
            
            soup = self._get_page_content(notice_url)
            if not soup:
                return None
            
            # Extract basic information
            headline = ""
            title_elem = soup.find('h1') or soup.find('h2')
            if title_elem:
                headline = title_elem.get_text().strip()
            
            published_date = self._extract_published_date(soup)
            content_text = self._extract_content_text(soup)
            
            # Skip if no meaningful content
            if not content_text or len(content_text) < MIN_CONTENT_LENGTH:
                self.logger.warning(f"Insufficient content for {notice_url}")
                return None
            
            # Check for duplicates
            notice_hash = self._generate_notice_hash(notice_url, content_text)
            if notice_url in self.existing_notices:
                existing_hash = self.existing_notices[notice_url].get('content_hash')
                if existing_hash == notice_hash:
                    self.logger.info(f"Notice unchanged, skipping: {notice_url}")
                    return None
            
            # Extract additional data
            related_links = self._extract_related_links(soup)
            table_data = self._extract_tables_and_charts(soup)
            
            # Extract image URL
            image_url = None
            img_elem = soup.find('img', src=True)
            if img_elem:
                image_url = urljoin(self.base_url, img_elem['src'])
            
            # Process attachments
            pdf_text = []
            csv_data = []
            excel_data = {}
            
            for link in related_links:
                if link['type'] == 'pdf':
                    pdf_content = self._extract_pdf_text(link['url'])
                    if pdf_content:
                        pdf_text.append({
                            'filename': link['text'],
                            'content': pdf_content
                        })
                elif link['type'] == 'csv':
                    csv_content = self._extract_csv_data(link['url'])
                    if csv_content:
                        csv_data.append({
                            'filename': link['text'],
                            'data': csv_content
                        })
                elif link['type'] == 'excel':
                    excel_content = self._extract_excel_data(link['url'])
                    if excel_content:
                        excel_data[link['text']] = excel_content
            
            notice_data = {
                'url': notice_url,
                'headline': headline,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'theme': None,  # Not available in the HTML structure provided
                'content_text': content_text,
                'associated_image_url': image_url,
                'related_links': related_links,
                'pdf_text': pdf_text,
                'table_and_chart_data': table_data,
                'csv_data': csv_data,
                'excel_data': excel_data,
                'content_hash': notice_hash
            }
            
            self.stats['new_notices'] += 1
            return notice_data
            
        except Exception as e:
            self.logger.error(f"Error scraping notice {notice_url}: {e}")
            self.stats['errors'] += 1
            return None
    
    def _get_next_page_url(self, soup: BeautifulSoup, current_page: int) -> Optional[str]:
        """Get URL for next page."""
        # Look for next page link
        next_link = soup.find('a', {'title': 'Go to next page'})
        if next_link and next_link.get('href'):
            return urljoin(self.base_url, next_link['href'])
        
        # Alternative: construct URL manually
        return f"{self.start_url}?page={current_page + 1}"
    
    def scrape_all_notices(self) -> List[Dict]:
        """Main scraping method."""
        self.logger.info("Starting AICIS regulatory notices scraper")
        self.logger.info(f"Max pages: {self.max_pages or 'All pages'}")
        
        # Setup session and driver
        self._setup_session()
        self._setup_driver()
        
        all_notices = list(self.existing_notices.values())
        current_page = 0
        current_url = self.start_url
        
        try:
            while True:
                if self.max_pages and current_page >= self.max_pages:
                    self.logger.info(f"Reached maximum pages limit: {self.max_pages}")
                    break
                
                self.logger.info(f"Scraping page {current_page + 1}: {current_url}")
                soup = self._get_page_content(current_url)
                
                if not soup:
                    self.logger.error(f"Failed to get content for page {current_page + 1}")
                    break
                
                # Extract notice links from this page
                notice_links = self._extract_notice_links(soup)
                self.stats['notices_found'] += len(notice_links)
                
                if not notice_links:
                    self.logger.info("No more notices found")
                    break
                
                # Scrape each notice
                for notice_url in notice_links:
                    notice_data = self._scrape_notice(notice_url)
                    if notice_data:
                        # Remove existing version if updating
                        all_notices = [n for n in all_notices if n['url'] != notice_url]
                        all_notices.append(notice_data)
                    
                    # Rate limiting
                    time.sleep(REQUEST_DELAY)
                
                # Check for next page
                next_url = self._get_next_page_url(soup, current_page)
                if not next_url or next_url == current_url:
                    self.logger.info("No more pages available")
                    break
                
                current_url = next_url
                current_page += 1
                self.stats['pages_visited'] += 1
                
                # Rate limiting between pages
                time.sleep(PAGE_DELAY)
        
        finally:
            # Cleanup
            if self.driver:
                self.driver.quit()
        
        # Save results
        self._save_notices(all_notices)
        
        # Log final statistics
        self.stats['end_time'] = datetime.now().isoformat()
        start_time = self.stats.get('start_time')
        if isinstance(start_time, datetime):
            self.stats['duration'] = (datetime.now() - start_time).total_seconds()
        else:
            # start_time is already a string, parse it back
            start_dt = datetime.fromisoformat(start_time) if isinstance(start_time, str) else datetime.now()
            self.stats['duration'] = (datetime.now() - start_dt).total_seconds()
        
        self.logger.info("Scraping completed!")
        self.logger.info(f"Statistics: {self.stats}")
        
        return all_notices


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='AICIS Regulatory Notices Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Configuration:
  Default max pages: {MAX_PAGES or 'All pages'}
  Request delay: {REQUEST_DELAY}s
  Page delay: {PAGE_DELAY}s
  Data directory: {DATA_DIR}
  Output file: {OUTPUT_FILE}

Examples:
  python aicis_scraper.py                    # Scrape all pages
  python aicis_scraper.py --max-pages 3     # Scrape first 3 pages only
  python aicis_scraper.py --show-config     # Show current configuration
        """
    )
    
    parser.add_argument('--max-pages', type=int, default=None,
                       help='Maximum number of pages to scrape (overrides config default)')
    parser.add_argument('--show-config', action='store_true',
                       help='Show current configuration and exit')
    
    args = parser.parse_args()
    
    # Create scraper instance
    scraper = AICISScraper(max_pages=args.max_pages)
    
    # Show configuration if requested
    if args.show_config:
        config = scraper.get_current_config()
        print("Current Configuration:")
        print("=" * 50)
        for key, value in config.items():
            print(f"{key:20}: {value}")
        return
    
    # Show startup info
    config = scraper.get_current_config()
    print(f"AICIS Regulatory Notices Scraper")
    print(f"=" * 40)
    print(f"Max pages: {config['max_pages'] or 'All pages'}")
    print(f"Output: {config['output_file']}")
    print(f"Request delay: {config['request_delay']}s")
    print(f"Starting scrape...")
    print()
    
    try:
        notices = scraper.scrape_all_notices()
        
        print(f"\n" + "=" * 50)
        print(f"Scraping completed successfully!")
        print(f"Total notices: {len(notices)}")
        print(f"New notices: {scraper.stats['new_notices']}")
        print(f"Pages visited: {scraper.stats['pages_visited']}")
        print(f"Errors: {scraper.stats['errors']}")
        print(f"Duration: {scraper.stats.get('duration', 0):.1f}s")
        print(f"Data saved to: {scraper.data_file}")
        print(f"Log saved to: {scraper.log_file}")
        
    except KeyboardInterrupt:
        print(f"\nScraping interrupted by user")
        print(f"Partial data may be saved to: {scraper.data_file}")
    except Exception as e:
        print(f"\nError during scraping: {e}")
        print(f"Check log file for details: {scraper.log_file}")
        raise


if __name__ == "__main__":
    main()