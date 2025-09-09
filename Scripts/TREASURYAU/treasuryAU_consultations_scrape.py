#!/usr/bin/env python3
"""
Treasury AU Consultations Scraper - Fixed Version with Status Tracking
Scrapes all consultations from Treasury AU website with comprehensive content extraction
and proper status update tracking for existing consultations
"""

import requests
import json
import os
import re
import time
import logging
import io
import hashlib
from datetime import datetime
from urllib.parse import urljoin
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from pathlib import Path

# Required imports
try:
    from bs4 import BeautifulSoup
    import PyPDF2
    import pdfplumber
    from fake_useragent import UserAgent
    import pandas as pd
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install beautifulsoup4 PyPDF2 pdfplumber fake-useragent pandas")
    exit(1)

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    SELENIUM_AVAILABLE = True
except ImportError as e:
    print("Selenium not available - this scraper requires Selenium for Treasury AU")
    print("Install with: pip install selenium webdriver-manager")
    exit(1)

# Configuration
BASE_URL = "https://treasury.gov.au"
CONSULTATIONS_URL = "https://treasury.gov.au/consultation"
DATA_DIR = Path("data")
MAX_PAGES = 2  # Set to None for first run to scrape all pages
DELAY_BETWEEN_REQUESTS = 2
PDF_TIMEOUT = 30
MAX_RETRIES = 3

# File paths
JSON_FILE = DATA_DIR / "treasuryAU_consultations.json"
CSV_FILE = DATA_DIR / "treasuryAU_consultations.csv"
LOG_FILE = DATA_DIR / "scraper.log"

@dataclass
class Consultation:
    """Data class for consultation information"""
    id: str
    url: str
    title: str
    status: str
    date_range: str
    published_date: str
    consultation_period: str
    theme: str
    content: str
    pdf_content: str
    related_links: List[str]
    image_url: Optional[str]
    scraped_date: str
    # NEW: Track status updates
    status_history: List[Dict[str, str]]  # List of {status: str, date: str}
    last_status_check: str
    unique_id: str

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

class TreasuryAUScraper:
    """Main scraper class for Treasury AU consultations with status tracking"""

    def __init__(self):
        self.session = requests.Session()
        self.driver = None
        self.ua = UserAgent()
        # FIXED: Track by unique IDs instead of URLs to allow status updates
        self.existing_consultation_ids: Set[str] = set()
        self.existing_data: List[Dict] = []
        self.existing_data_by_id: Dict[str, Dict] = {}  # For quick lookups
        self.setup_logging()
        self.setup_directories()
        self.setup_session()
        self.load_existing_data()
        
        # Stats tracking
        self.stats = {
            'new_consultations': 0,
            'updated_consultations': 0,
            'status_changes': 0,
            'skipped_no_changes': 0,
            'errors': 0
        }

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def setup_directories(self):
        """Create necessary directories"""
        DATA_DIR.mkdir(exist_ok=True)

    def setup_session(self):
        """Setup requests session with headers"""
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def setup_selenium(self):
        """Setup Selenium WebDriver"""
        if self.driver:
            return True

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(f'--user-agent={self.ua.random}')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        try:
            driver_path = ChromeDriverManager().install()
            service = Service(driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.logger.info("Selenium WebDriver initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize Selenium: {e}")
            return False

    def create_unique_id(self, title: str, url: str) -> str:
        """Create a unique identifier for deduplication"""
        # Use title and URL path (not full URL to handle parameter changes)
        url_path = url.split('?')[0]  # Remove query parameters
        content = f"{title}_{url_path}"
        return hashlib.md5(content.encode()).hexdigest()

    def load_existing_data(self):
        """FIXED: Load existing consultation data to track status changes"""
        if JSON_FILE.exists():
            try:
                with open(JSON_FILE, 'r', encoding='utf-8') as f:
                    self.existing_data = json.load(f)
                
                # Create lookup structures
                for item in self.existing_data:
                    # Handle both old and new data formats
                    if 'unique_id' not in item:
                        # Create unique ID for existing records
                        item['unique_id'] = self.create_unique_id(
                            item.get('title', ''), 
                            item.get('url', '')
                        )
                    
                    # Ensure status_history exists
                    if 'status_history' not in item:
                        item['status_history'] = [{
                            'status': item.get('status', 'Unknown'),
                            'date': item.get('scraped_date', datetime.now().isoformat())
                        }]
                    
                    # Ensure last_status_check exists
                    if 'last_status_check' not in item:
                        item['last_status_check'] = item.get('scraped_date', datetime.now().isoformat())
                    
                    unique_id = item['unique_id']
                    self.existing_consultation_ids.add(unique_id)
                    self.existing_data_by_id[unique_id] = item
                
                self.logger.info(f"Loaded {len(self.existing_data)} existing consultations with {len(self.existing_consultation_ids)} unique IDs")
                
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                self.existing_data = []

    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Get page content using Selenium"""
        for attempt in range(MAX_RETRIES):
            try:
                if not self.setup_selenium():
                    self.logger.error("Selenium setup failed")
                    return None

                self.logger.info(f"Loading page: {url}")
                self.driver.get(url)

                # Wait for content to load
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.any_of(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.views-row")),
                            EC.presence_of_element_located((By.CSS_SELECTOR, "section[data-clp='html']")),
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.tsy-container")),
                            EC.presence_of_element_located((By.TAG_NAME, "main"))
                        )
                    )
                except TimeoutException:
                    self.logger.warning(f"Timeout waiting for content on {url}")

                time.sleep(3)  # Additional wait for dynamic content

                page_source = self.driver.page_source
                self.logger.info(f"Page loaded, content length: {len(page_source)}")

                # Check for loading placeholder
                if "Loading" in page_source and "Slow connection" in page_source and len(page_source) < 5000:
                    if attempt < MAX_RETRIES - 1:
                        self.logger.warning("Got loading placeholder, retrying...")
                        time.sleep(5)
                        continue
                    else:
                        self.logger.error("Still getting loading placeholder after retries")
                        return None

                soup = BeautifulSoup(page_source, 'html.parser')
                return soup

            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(DELAY_BETWEEN_REQUESTS * (attempt + 1))
                else:
                    self.logger.error(f"All attempts failed for {url}")
                    return None

    def extract_pagination_info(self, soup: BeautifulSoup) -> int:
        """Extract total number of pages from pagination"""
        try:
            last_page_link = soup.find('a', {'title': 'Go to last page'})
            if last_page_link:
                href = last_page_link.get('href', '')
                match = re.search(r'page=(\d+)', href)
                if match:
                    total_pages = int(match.group(1)) + 1
                    self.logger.info(f"Found {total_pages} total pages")
                    return total_pages

            # Fallback
            page_links = soup.find_all('a', href=re.compile(r'\?page=\d+'))
            if page_links:
                page_numbers = []
                for link in page_links:
                    match = re.search(r'page=(\d+)', link.get('href', ''))
                    if match:
                        page_numbers.append(int(match.group(1)))
                if page_numbers:
                    return max(page_numbers) + 1

        except Exception as e:
            self.logger.error(f"Error extracting pagination info: {e}")

        return 1

    def extract_consultation_links(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract consultation links and basic info from listing page"""
        consultations = []

        try:
            consultation_rows = soup.find_all('div', class_='views-row')
            self.logger.info(f"Found {len(consultation_rows)} consultation rows")

            for row in consultation_rows:
                try:
                    # Extract status
                    status = 'Unknown'
                    status_elem = row.find('div', class_=re.compile(r'field--name-field-status'))
                    if status_elem:
                        status_item = status_elem.find('div', class_=re.compile(r'field__item'))
                        if status_item:
                            status = status_item.get_text(strip=True)

                    # Extract title and URL
                    title_elem = row.find('div', class_=re.compile(r'field--name-node-title'))
                    if not title_elem:
                        continue

                    link_elem = title_elem.find('a')
                    if not link_elem:
                        continue

                    title = link_elem.get_text(strip=True)
                    relative_url = link_elem.get('href', '')
                    full_url = urljoin(BASE_URL, relative_url)

                    # Extract date range
                    date_range = 'Unknown'
                    date_elem = row.find('div', class_=re.compile(r'field--field-date-range'))
                    if date_elem:
                        date_range = date_elem.get_text(strip=True)
                        date_range = re.sub(r'\s+', ' ', date_range).strip()

                    # Extract consultation ID from URL
                    consultation_id = relative_url.split('/')[-1] if relative_url else ''
                    
                    # Create unique ID
                    unique_id = self.create_unique_id(title, full_url)

                    consultations.append({
                        'id': consultation_id,
                        'unique_id': unique_id,
                        'url': full_url,
                        'title': title,
                        'status': status,
                        'date_range': date_range
                    })

                except Exception as e:
                    self.logger.error(f"Error extracting consultation from row: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Error extracting consultation links: {e}")

        return consultations

    def check_if_needs_update(self, consultation_info: Dict) -> tuple[bool, Optional[Dict]]:
        """FIXED: Check if consultation needs updating based on status or timing"""
        unique_id = consultation_info['unique_id']
        current_status = consultation_info['status']
        
        if unique_id not in self.existing_consultation_ids:
            # New consultation
            return True, None
        
        existing_data = self.existing_data_by_id[unique_id]
        existing_status = existing_data.get('status', 'Unknown')
        
        # Always update if status has changed
        if current_status != existing_status:
            self.logger.info(f"Status change detected for '{consultation_info['title']}': {existing_status} -> {current_status}")
            return True, existing_data
        
        # Update periodically even if status hasn't changed (e.g., content might have been updated)
        last_check = existing_data.get('last_status_check', '')
        if last_check:
            try:
                last_check_date = datetime.fromisoformat(last_check.replace('Z', '+00:00'))
                days_since_check = (datetime.now() - last_check_date.replace(tzinfo=None)).days
                
                # Update open consultations more frequently
                if current_status.lower() in ['open', 'active']:
                    if days_since_check >= 1:  # Check open consultations daily
                        self.logger.info(f"Updating open consultation '{consultation_info['title']}' (last checked {days_since_check} days ago)")
                        return True, existing_data
                else:
                    if days_since_check >= 7:  # Check closed consultations weekly
                        self.logger.info(f"Periodic update for consultation '{consultation_info['title']}' (last checked {days_since_check} days ago)")
                        return True, existing_data
            except Exception as e:
                self.logger.warning(f"Error parsing last check date: {e}")
                return True, existing_data  # Update if we can't parse the date
        
        return False, existing_data

    def extract_consultation_details(self, consultation_info: Dict) -> Optional[Consultation]:
        """FIXED: Extract detailed information with status tracking"""
        unique_id = consultation_info['unique_id']
        url = consultation_info['url']
        current_status = consultation_info['status']
        
        # Check if we need to update this consultation
        needs_update, existing_data = self.check_if_needs_update(consultation_info)
        
        if not needs_update:
            self.logger.info(f"Skipping consultation (no changes needed): {consultation_info['title']}")
            self.stats['skipped_no_changes'] += 1
            return None

        self.logger.info(f"Scraping consultation: {consultation_info['title']}")

        soup = self.get_page_content(url)
        if not soup:
            self.logger.error(f"Failed to get content for {url}")
            self.stats['errors'] += 1
            return None

        try:
            # Determine if this is an update or new consultation
            is_update = existing_data is not None
            
            if is_update:
                # Initialize with existing data
                consultation_data = existing_data.copy()
                # Update fields that might have changed
                consultation_data['status'] = current_status
                consultation_data['date_range'] = consultation_info['date_range']
                consultation_data['last_status_check'] = datetime.now().isoformat()
                
                # Track status changes
                existing_status = existing_data.get('status', 'Unknown')
                if current_status != existing_status:
                    if 'status_history' not in consultation_data:
                        consultation_data['status_history'] = []
                    consultation_data['status_history'].append({
                        'status': current_status,
                        'date': datetime.now().isoformat()
                    })
                    self.stats['status_changes'] += 1
                
                self.stats['updated_consultations'] += 1
                
                # For significant updates, re-extract content
                if current_status != existing_status or not consultation_data.get('content'):
                    self.logger.info("Re-extracting content due to status change or missing content")
                    # Extract fresh content
                    consultation_data['consultation_period'] = self.extract_consultation_period(soup)
                    consultation_data['theme'] = self.extract_theme(soup)
                    consultation_data['content'] = self.extract_main_content(soup)
                    consultation_data['related_links'] = self.extract_related_links(soup)
                    consultation_data['image_url'] = self.extract_image_url(soup)
                    consultation_data['pdf_content'] = self.extract_pdf_content(soup)
                    consultation_data['published_date'] = self.extract_published_date(soup)
                else:
                    self.logger.info("Keeping existing content (status update only)")
            else:
                # New consultation - extract all content
                consultation_period = self.extract_consultation_period(soup)
                theme = self.extract_theme(soup)
                content = self.extract_main_content(soup)
                related_links = self.extract_related_links(soup)
                image_url = self.extract_image_url(soup)
                pdf_content = self.extract_pdf_content(soup)
                published_date = self.extract_published_date(soup)
                
                consultation_data = {
                    'id': consultation_info['id'],
                    'unique_id': unique_id,
                    'url': url,
                    'title': consultation_info['title'],
                    'status': current_status,
                    'date_range': consultation_info['date_range'],
                    'published_date': published_date,
                    'consultation_period': consultation_period,
                    'theme': theme,
                    'content': content,
                    'pdf_content': pdf_content,
                    'related_links': related_links,
                    'image_url': image_url,
                    'scraped_date': datetime.now().isoformat(),
                    'status_history': [{
                        'status': current_status,
                        'date': datetime.now().isoformat()
                    }],
                    'last_status_check': datetime.now().isoformat()
                }
                
                self.stats['new_consultations'] += 1

            # Convert to Consultation object
            consultation = Consultation(**consultation_data)

            self.logger.info(f"Successfully {'updated' if is_update else 'extracted'} consultation: {consultation.title}")
            return consultation

        except Exception as e:
            self.logger.error(f"Error extracting consultation details from {url}: {e}")
            self.stats['errors'] += 1
            return None

    def extract_consultation_period(self, soup: BeautifulSoup) -> str:
        """Extract consultation period from page"""
        try:
            # Look for consultation period in headers
            for h_tag in soup.find_all(['h1', 'h2', 'h3', 'h4']):
                h_text = h_tag.get_text(strip=True)
                if 'consultation period' in h_text.lower():
                    period = re.sub(r'consultation period:\s*', '', h_text, flags=re.I)
                    return period.strip()

            # Look for patterns in text
            text = soup.get_text()
            patterns = [
                r'Consultation period:\s*([^<\n]+)',
                r'(\d{1,2}\s+\w+\s+to\s+\d{1,2}\s+\w+\s+\d{4})',
                r'(\d{1,2}\s+\w+\s+\d{4}\s*[-–]\s*\d{1,2}\s+\w+\s+\d{4})',
            ]

            for pattern in patterns:
                match = re.search(pattern, text, re.I)
                if match:
                    period = match.group(1).strip()
                    return re.sub(r'\s+', ' ', period)

        except Exception as e:
            self.logger.error(f"Error extracting consultation period: {e}")

        return "Unknown"

    def extract_theme(self, soup: BeautifulSoup) -> str:
        """Extract theme/policy topic from page"""
        try:
            content_text = soup.get_text().lower()

            themes = [
                ('taxation', 'Taxation'),
                ('banking and finance', 'Banking and finance'),
                ('business and industry', 'Business and industry'),
                ('consumers and community', 'Consumers and community'),
                ('superannuation', 'Superannuation'),
                ('economy', 'Economy'),
                ('housing', 'Housing'),
                ('budget', 'Budget'),
                ('competition', 'Competition'),
                ('regulation', 'Regulation'),
                ('financial services', 'Financial services'),
            ]

            for keyword, theme_name in themes:
                if keyword in content_text:
                    return theme_name

        except Exception as e:
            self.logger.error(f"Error extracting theme: {e}")

        return "General"

    def extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract main content from consultation page"""
        try:
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.decompose()

            # Find Treasury AU specific content containers
            content_containers = []

            # Look for Treasury AU specific content sections
            tsy_sections = soup.find_all('section', {'data-clp': 'html'})
            for section in tsy_sections:
                tsy_containers = section.find_all('div', class_='tsy-container')
                content_containers.extend(tsy_containers)

            # If no tsy-containers found, look for other Treasury content
            if not content_containers:
                content_containers = soup.find_all('div', {'data-cl-card-id': True})

            # Fallback to main content areas
            if not content_containers:
                main_elem = soup.find('main') or soup.find('div', id='main')
                if main_elem:
                    content_containers = [main_elem]
                else:
                    content_containers = [soup.find('body') or soup]

            self.logger.info(f"Found {len(content_containers)} content containers")

            all_content_parts = []
            processed_text = set()

            # Process each container
            for container in content_containers:
                if not container:
                    continue

                # Clean up unwanted elements within this container
                for unwanted in container.find_all(['nav', 'aside', 'footer', 'header']):
                    unwanted.decompose()

                # Extract content from this container in document order
                container_content = self._extract_container_content_in_order(container, processed_text)
                if container_content:
                    all_content_parts.extend(container_content)

            # Join all content with proper spacing
            full_content = '\n\n'.join(all_content_parts)

            # Clean up excessive whitespace but preserve structure
            full_content = re.sub(r'\n\s*\n\s*\n+', '\n\n', full_content)
            full_content = re.sub(r'[ \t]+', ' ', full_content)

            # Remove any remaining duplicated paragraphs
            full_content = self._remove_duplicate_paragraphs(full_content)

            self.logger.info(f"Extracted content length: {len(full_content)} characters")
            return full_content.strip()

        except Exception as e:
            self.logger.error(f"Error extracting main content: {e}")
            return ""

    def _extract_container_content_in_order(self, container, processed_text: set) -> List[str]:
        """Extract content from container in document order"""
        content_parts = []

        # Find all relevant elements in document order
        elements = container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol', 'div', 'li'])

        for elem in elements:
            # Skip elements that are nested inside other elements we're processing
            parent_elem = elem.find_parent(['ul', 'ol'])
            if parent_elem and parent_elem != container:
                continue  # This will be handled when we process the parent list

            text = elem.get_text(strip=True)
            if not text or text in processed_text or len(text) < 5:
                continue

            # Handle different element types
            if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                content_parts.append(f"## {text}")
                processed_text.add(text)

            elif elem.name == 'p':
                # Only add paragraphs that have substantial content
                if len(text) > 15:
                    content_parts.append(text)
                    processed_text.add(text)

            elif elem.name in ['ul', 'ol']:
                # Process the entire list
                list_items = []
                for li in elem.find_all('li'):
                    li_text = li.get_text(strip=True)
                    if li_text and li_text not in processed_text and len(li_text) > 3:
                        list_items.append(f"• {li_text}")
                        processed_text.add(li_text)

                if list_items:
                    content_parts.append('\n'.join(list_items))
                    processed_text.add(text)

            elif elem.name == 'div':
                # Only process divs that contain direct text and don't have other block elements
                if not elem.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol', 'div']):
                    if len(text) > 30:  # Only substantial text
                        content_parts.append(text)
                        processed_text.add(text)

        return content_parts

    def _remove_duplicate_paragraphs(self, content: str) -> str:
        """Remove duplicate paragraphs from content"""
        paragraphs = content.split('\n\n')
        unique_paragraphs = []
        seen = set()

        for para in paragraphs:
            para_clean = para.strip()
            if para_clean and para_clean not in seen:
                unique_paragraphs.append(para_clean)
                seen.add(para_clean)

        return '\n\n'.join(unique_paragraphs)

    def extract_table_text(self, table) -> str:
        """Extract text from HTML table preserving structure"""
        try:
            rows = table.find_all('tr')
            table_data = []

            for row in rows:
                cells = row.find_all(['td', 'th'])
                row_data = [cell.get_text(strip=True) for cell in cells]
                if any(row_data):
                    table_data.append(' | '.join(row_data))

            return '\n'.join(table_data)

        except Exception as e:
            self.logger.error(f"Error extracting table text: {e}")
            return ""

    def extract_related_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract related links from consultation page"""
        links = []

        try:
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href:
                    full_url = urljoin(BASE_URL, href)

                    # Filter out unwanted links
                    if not any(skip in full_url.lower() for skip in ['javascript:', 'mailto:', '#']):
                        if not any(ext in full_url.lower() for ext in ['.xlsx', '.csv', '.mp3', '.wav', '.mp4']):
                            links.append(full_url)

        except Exception as e:
            self.logger.error(f"Error extracting related links: {e}")

        return list(set(links))

    def extract_image_url(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract main image URL from consultation page"""
        try:
            main_content = soup.find('main') or soup.find('div', id='main')
            if main_content:
                img = main_content.find('img')
                if img and img.get('src'):
                    return urljoin(BASE_URL, img.get('src'))

        except Exception as e:
            self.logger.error(f"Error extracting image URL: {e}")

        return None

    def extract_published_date(self, soup: BeautifulSoup) -> str:
        """Extract published date from consultation page"""
        try:
            # Look for date elements
            date_elements = soup.find_all('time')
            for elem in date_elements:
                datetime_attr = elem.get('datetime')
                if datetime_attr:
                    return datetime_attr

            # Look for date patterns in text
            text = soup.get_text()
            date_patterns = [
                r'(\d{1,2}\s+\w+\s+\d{4})',
                r'(\d{4}-\d{2}-\d{2})',
            ]

            for pattern in date_patterns:
                match = re.search(pattern, text)
                if match:
                    return match.group(1)

        except Exception as e:
            self.logger.error(f"Error extracting published date: {e}")

        return "Unknown"

    def extract_pdf_content(self, soup: BeautifulSoup) -> str:
        """Extract text content from linked PDF files"""
        pdf_content = []

        try:
            # Find ALL PDF links using multiple methods
            pdf_links = []

            # Method 1: Direct PDF links ending with .pdf
            direct_pdf_links = soup.find_all('a', href=re.compile(r'\.pdf', re.I))
            pdf_links.extend(direct_pdf_links)

            # Method 2: Links containing .pdf anywhere in href
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                if '.pdf' in href.lower() and link not in pdf_links:
                    pdf_links.append(link)

            # Method 3: Find links that mention PDF in the text content
            for link in soup.find_all('a', href=True):
                link_text = link.get_text().lower()
                if 'pdf' in link_text and link not in pdf_links:
                    # Check if the href looks like it could be a PDF
                    href = link.get('href', '')
                    if any(domain in href for domain in ['googleapis.com', 'treasury.gov.au']) or '.pdf' in href:
                        pdf_links.append(link)

            # Method 4: Look in list items that contain PDF references
            for li in soup.find_all('li'):
                li_text = li.get_text().lower()
                if 'pdf' in li_text:
                    for link in li.find_all('a', href=True):
                        if link not in pdf_links:
                            pdf_links.append(link)

            # Remove duplicates based on href
            unique_urls = {}
            unique_links = []
            for link in pdf_links:
                href = link.get('href')
                if href and href not in unique_urls:
                    # Filter out clearly non-PDF links
                    if not any(skip in href.lower() for skip in ['mailto:', 'javascript:', '#']):
                        unique_urls[href] = link
                        unique_links.append(link)

            self.logger.info(f"Found {len(unique_links)} unique PDF links")

            # Log all found PDFs for verification
            for i, link in enumerate(unique_links, 1):
                href = link.get('href')
                link_text = link.get_text(strip=True)
                self.logger.info(f"PDF {i}: '{link_text}' -> {href}")

            # Download and extract each PDF
            for i, link in enumerate(unique_links, 1):
                href = link.get('href')
                if href:
                    # Handle relative and absolute URLs
                    if href.startswith('http'):
                        pdf_url = href
                    else:
                        pdf_url = urljoin(BASE_URL, href)

                    # Skip if it's clearly not a PDF
                    if not any(ext in pdf_url.lower() for ext in ['.pdf', 'pdf']):
                        self.logger.info(f"Skipping non-PDF link: {pdf_url}")
                        continue

                    self.logger.info(f"Processing PDF {i}/{len(unique_links)}: {pdf_url}")

                    # Get PDF description from link and surrounding context
                    description = self._get_pdf_description(link)

                    content = self.download_and_extract_pdf(pdf_url)
                    if content:
                        # Format for LLM analysis with clear separation
                        formatted_content = self._format_pdf_content_for_llm(description, content, i)
                        pdf_content.append(formatted_content)
                        self.logger.info(f"Successfully extracted PDF {i}: {len(content)} characters")
                    else:
                        self.logger.warning(f"Failed to extract content from PDF {i}: {pdf_url}")

                    time.sleep(1)  # Brief delay between PDF downloads

            # Combine all PDF content with clear separators
            if pdf_content:
                combined_content = "\n\n" + "="*80 + "\nPDF DOCUMENTS CONTENT\n" + "="*80 + "\n\n"
                combined_content += "\n\n".join(pdf_content)
                return combined_content
            else:
                self.logger.info("No PDF content extracted")
                return ""

        except Exception as e:
            self.logger.error(f"Error extracting PDF content: {e}")
            return ""

    def _get_pdf_description(self, link) -> str:
        """Get description for PDF from link text and context"""
        # Get link text
        link_text = link.get_text(strip=True)

        # Get parent context
        parent_text = ""
        if link.parent:
            parent_text = link.parent.get_text(strip=True)

        # Get preceding text context
        preceding_text = ""
        if link.previous_sibling:
            if hasattr(link.previous_sibling, 'strip'):
                preceding_text = link.previous_sibling.strip()

        # Build description from available context
        description_parts = []

        if link_text and len(link_text) > 3:
            # Clean up link text (remove file size info)
            clean_link_text = re.sub(r'pdf\s+\d+.*?kb', '', link_text, flags=re.I).strip()
            clean_link_text = re.sub(r'\|\s*docx.*', '', clean_link_text, flags=re.I).strip()
            if clean_link_text:
                description_parts.append(clean_link_text)

        if preceding_text and len(preceding_text) > 3:
            description_parts.append(preceding_text)

        # Use parent context if we don't have good description
        if not description_parts and parent_text:
            parent_clean = re.sub(r'pdf\s+\d+.*?kb.*', '', parent_text, flags=re.I).strip()
            if len(parent_clean) > 10 and len(parent_clean) < 100:
                description_parts.append(parent_clean)

        if description_parts:
            return " - ".join(description_parts[:2])  # Limit to avoid too long descriptions
        else:
            return "PDF Document"

    def _format_pdf_content_for_llm(self, description: str, content: str, pdf_number: int) -> str:
        """Format PDF content for LLM analysis"""
        formatted = f"--- PDF DOCUMENT {pdf_number}: {description} ---\n\n"

        # Clean and structure the content for better LLM processing
        # Split into paragraphs and clean up
        paragraphs = content.split('\n\n')
        clean_paragraphs = []

        for para in paragraphs:
            # Clean up the paragraph
            clean_para = re.sub(r'\s+', ' ', para.strip())

            # Skip very short paragraphs that are likely formatting artifacts
            if len(clean_para) > 30:
                clean_paragraphs.append(clean_para)

        # Join with proper spacing for LLM readability
        formatted += '\n\n'.join(clean_paragraphs)

        return formatted

    def download_and_extract_pdf(self, pdf_url: str) -> str:
        """Download and extract text from PDF"""
        try:
            headers = {
                'User-Agent': self.ua.random,
                'Accept': 'application/pdf,application/octet-stream,*/*',
                'Referer': BASE_URL,
            }

            response = self.session.get(pdf_url, timeout=PDF_TIMEOUT, headers=headers)
            response.raise_for_status()

            if len(response.content) < 1000:
                self.logger.warning(f"PDF seems too small: {len(response.content)} bytes")
                return ""

            self.logger.info(f"Downloaded PDF ({len(response.content)} bytes)")

            # Try pdfplumber first
            try:
                pdf_file = io.BytesIO(response.content)
                with pdfplumber.open(pdf_file) as pdf:
                    text_content = []
                    for i, page in enumerate(pdf.pages):
                        try:
                            text = page.extract_text()
                            if text:
                                text = re.sub(r'\s+', ' ', text.strip())
                                if len(text) > 10:
                                    text_content.append(text)
                        except Exception as e:
                            self.logger.warning(f"Error extracting page {i+1}: {e}")

                    if text_content:
                        full_text = '\n\n'.join(text_content)
                        self.logger.info(f"Extracted {len(full_text)} characters using pdfplumber")
                        return full_text

            except Exception as e:
                self.logger.warning(f"pdfplumber failed: {e}, trying PyPDF2")

            # Fallback to PyPDF2
            try:
                pdf_file = io.BytesIO(response.content)
                pdf_reader = PyPDF2.PdfReader(pdf_file)

                text_content = []
                for i, page in enumerate(pdf_reader.pages):
                    try:
                        text = page.extract_text()
                        if text:
                            text = re.sub(r'\s+', ' ', text.strip())
                            if len(text) > 10:
                                text_content.append(text)
                    except Exception as e:
                        self.logger.warning(f"Error extracting page {i+1} with PyPDF2: {e}")

                if text_content:
                    full_text = '\n\n'.join(text_content)
                    self.logger.info(f"Extracted {len(full_text)} characters using PyPDF2")
                    return full_text

            except Exception as e:
                self.logger.error(f"PyPDF2 also failed: {e}")

        except Exception as e:
            self.logger.error(f"Error downloading/extracting PDF {pdf_url}: {e}")

        return ""

    def scrape_consultations(self) -> List[Consultation]:
        """Main method to scrape all consultations"""
        self.logger.info("Starting Treasury AU consultations scraper")

        # Get first page
        soup = self.get_page_content(CONSULTATIONS_URL)
        if not soup:
            self.logger.error("Failed to load consultations page")
            return []

        # Extract pagination info
        total_pages = self.extract_pagination_info(soup)

        # Determine pages to scrape
        pages_to_scrape = min(MAX_PAGES, total_pages) if MAX_PAGES else total_pages
        self.logger.info(f"Will scrape {pages_to_scrape} pages")

        all_consultations = []

        # Scrape each page
        for page_num in range(pages_to_scrape):
            self.logger.info(f"Scraping page {page_num + 1}/{pages_to_scrape}")

            if page_num == 0:
                page_soup = soup
            else:
                page_url = f"{CONSULTATIONS_URL}?page={page_num}"
                page_soup = self.get_page_content(page_url)

            if not page_soup:
                self.logger.error(f"Failed to load page {page_num}")
                continue

            # Extract consultation links
            consultation_links = self.extract_consultation_links(page_soup)

            # Extract details for each consultation
            for consultation_info in consultation_links:
                try:
                    consultation = self.extract_consultation_details(consultation_info)
                    if consultation:
                        all_consultations.append(consultation)
                except Exception as e:
                    self.logger.error(f"Error processing consultation {consultation_info.get('title', 'Unknown')}: {e}")

                time.sleep(DELAY_BETWEEN_REQUESTS)

        self.logger.info(f"Scraping completed. Found {len(all_consultations)} consultations to save")
        return all_consultations

    def save_data(self, consultations: List[Consultation]):
        """FIXED: Save scraped data with proper merging of updates"""
        try:
            # Create a dictionary of new consultations by unique_id
            new_consultations_by_id = {c.unique_id: c.to_dict() for c in consultations}
            
            # Start with existing data
            updated_data = []
            updated_ids = set()
            
            # Update existing records with new data
            for existing_item in self.existing_data:
                unique_id = existing_item.get('unique_id')
                if unique_id in new_consultations_by_id:
                    # Use updated data
                    updated_data.append(new_consultations_by_id[unique_id])
                    updated_ids.add(unique_id)
                else:
                    # Keep existing data
                    updated_data.append(existing_item)
            
            # Add completely new consultations
            for unique_id, consultation_data in new_consultations_by_id.items():
                if unique_id not in updated_ids:
                    updated_data.append(consultation_data)

            # Save JSON
            with open(JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(updated_data, f, ensure_ascii=False, indent=2)

            # Save CSV
            if updated_data:
                df = pd.DataFrame(updated_data)
                df.to_csv(CSV_FILE, index=False, encoding='utf-8')

            self.logger.info(f"Saved {len(updated_data)} total consultations to {JSON_FILE} and {CSV_FILE}")
            
            # Log statistics
            self.logger.info("=== SCRAPING STATISTICS ===")
            self.logger.info(f"New consultations: {self.stats['new_consultations']}")
            self.logger.info(f"Updated consultations: {self.stats['updated_consultations']}")
            self.logger.info(f"Status changes detected: {self.stats['status_changes']}")
            self.logger.info(f"Skipped (no changes): {self.stats['skipped_no_changes']}")
            self.logger.info(f"Errors: {self.stats['errors']}")

        except Exception as e:
            self.logger.error(f"Error saving data: {e}")

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
            self.driver = None

        if self.session:
            self.session.close()

    def run(self):
        """Main run method"""
        try:
            self.logger.info("=== Starting Treasury AU Scraper with Status Tracking ===")

            consultations = self.scrape_consultations()
            self.logger.info(f"Scraping completed. Found {len(consultations)} consultations to process")

            if consultations:
                self.save_data(consultations)
                self.logger.info("Data saved successfully")
            else:
                self.logger.warning("No consultations found to update")

        except Exception as e:
            self.logger.error(f"Error in main run: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        finally:
            self.cleanup()

def main():
    """Main entry point"""
    scraper = TreasuryAUScraper()
    scraper.run()

if __name__ == "__main__":
    main()