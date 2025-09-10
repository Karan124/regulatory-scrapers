#!/usr/bin/env python3
"""
RBA Consultations Scraper - Production Version
Comprehensive scraper for RBA consultations with deduplication, PDF extraction, and stealth features.
"""

import os
import json
import logging
import hashlib
import requests
import time
import random
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path
import re
from typing import Dict, List, Optional, Set
import io

# Third-party imports
try:
    from bs4 import BeautifulSoup
    import PyPDF2
    import pdfplumber
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from fake_useragent import UserAgent
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install beautifulsoup4 PyPDF2 pdfplumber pandas requests fake-useragent openpyxl")
    exit(1)

class RBAConsultationsScraper:
    def __init__(self, data_dir: str = "data", log_level: str = "INFO"):
        """Initialize the scraper with configuration."""
        self.base_url = "https://www.rba.gov.au"
        self.consultations_url = f"{self.base_url}/publications/consultations/"
        self.data_dir = Path(data_dir)
        self.output_file = self.data_dir / "rba_consultations.json"
        self.log_file = self.data_dir / "scraping.log"
        
        # Create data directory
        self.data_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self.setup_logging(log_level)
        
        # Initialize session with stealth features
        self.session = self.create_stealth_session()
        
        # Track processed files to avoid reprocessing
        self.processed_files: Set[str] = set()
        
        # Load existing data for deduplication
        self.existing_data = self.load_existing_data()
        
        # Load processed files from existing data
        self._load_processed_files()
        
        self.logger.info("RBA Consultations Scraper initialized")

    def setup_logging(self, level: str):
        """Setup logging configuration."""
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def create_stealth_session(self) -> requests.Session:
        """Create a session with stealth features to avoid bot detection."""
        session = requests.Session()
        
        # Setup retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Setup realistic headers
        try:
            ua = UserAgent()
            user_agent = ua.random
        except:
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        return session

    def load_existing_data(self) -> Dict:
        """Load existing consultation data if available."""
        if self.output_file.exists():
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.logger.info(f"Loaded {len(data)} existing consultations")
                return data
            except json.JSONDecodeError:
                self.logger.warning("Existing data file corrupted, starting fresh")
                return {}
        return {}

    def _load_processed_files(self):
        """Load processed file hashes from existing data."""
        for consultation in self.existing_data.values():
            attachments = consultation.get('attachments', {})
            for pdf_item in attachments.get('pdf_text', []):
                if 'url' in pdf_item:
                    file_hash = hashlib.md5(pdf_item['url'].encode()).hexdigest()
                    self.processed_files.add(file_hash)
            for excel_item in attachments.get('excel_csv_text', []):
                if 'url' in excel_item:
                    file_hash = hashlib.md5(excel_item['url'].encode()).hexdigest()
                    self.processed_files.add(file_hash)

    def save_data(self, data: Dict):
        """Save consultation data to JSON file."""
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        self.logger.info(f"Data saved to {self.output_file}")

    def get_page_with_stealth(self, url: str, delay: tuple = (1, 3)) -> Optional[requests.Response]:
        """Fetch a page with stealth measures."""
        # Random delay between requests
        time.sleep(random.uniform(*delay))
        
        try:
            # First, visit the main page to establish session if this is a different page
            if url != self.consultations_url and not hasattr(self, '_session_established'):
                self.session.get(self.consultations_url, timeout=30)
                time.sleep(random.uniform(0.5, 1.5))
                self._session_established = True
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            return None

    def extract_consultations_list(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract consultation entries from the main page."""
        consultations = []
        
        # Find both open and closed consultations tables
        tables = soup.find_all('table', class_='table-linear')
        
        for table in tables:
            # Determine status from table caption
            caption = table.find('caption')
            status = "Open" if caption and "Open" in caption.get_text() else "Closed"
            
            # Extract rows
            tbody = table.find('tbody')
            if not tbody:
                continue
                
            rows = tbody.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    date_cell = cells[0].get_text(strip=True)
                    title_cell = cells[1]
                    
                    # Skip empty rows or placeholder text
                    if (not date_cell or date_cell == "&nbsp;" or 
                        "no open consultations" in title_cell.get_text().lower()):
                        continue
                    
                    # Extract link and title
                    link_elem = title_cell.find('a')
                    if link_elem:
                        title = link_elem.get_text(strip=True)
                        relative_url = link_elem.get('href', '')
                        full_url = urljoin(self.base_url, relative_url)
                        
                        consultations.append({
                            'title': title,
                            'url': full_url,
                            'status': status,
                            'published_date': date_cell,
                            'scraped_date': datetime.now().isoformat()
                        })
        
        return consultations

    def generate_consultation_id(self, consultation: Dict) -> str:
        """Generate a unique ID for a consultation based on URL and title."""
        title = consultation.get('title') or consultation.get('headline', '')
        url = consultation.get('url', '')
        content = f"{url}{title}"
        return hashlib.md5(content.encode()).hexdigest()

    def extract_page_content(self, url: str) -> Dict:
        """Extract content from a consultation page."""
        response = self.get_page_with_stealth(url)
        if not response:
            return {'error': 'Failed to fetch page'}
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract main content
        content_div = soup.find('div', {'id': 'content'}) or soup.find('main') or soup
        
        # Extract all required fields
        theme = self.extract_theme(soup)
        image_url = self.extract_image(soup)
        page_text = self.extract_clean_text(content_div)
        embedded_links = self.extract_embedded_links(content_div, url)
        attachments = self.extract_attachments(soup, url)
        
        return {
            'theme': theme,
            'image_url': image_url,
            'page_text': page_text,
            'embedded_links': embedded_links,
            'attachments': attachments
        }

    def extract_theme(self, soup: BeautifulSoup) -> str:
        """Extract theme/category from the page."""
        # Look for breadcrumbs first
        breadcrumb = soup.find('nav', class_='breadcrumb') or soup.find('ol', class_='breadcrumb')
        if breadcrumb:
            links = breadcrumb.find_all('a')
            if len(links) > 1:
                return links[-2].get_text(strip=True)
        
        # Look for section headers
        section_headers = soup.find_all(['h1', 'h2'], class_=re.compile(r'section|category|theme'))
        if section_headers:
            return section_headers[0].get_text(strip=True)
        
        # Extract from URL path
        canonical = soup.find('link', {'rel': 'canonical'})
        if canonical:
            href = canonical.get('href', '')
        else:
            href = str(soup)
        
        # Theme mapping based on URL patterns
        if 'payments' in href.lower():
            return 'Payments and Infrastructure'
        elif 'monetary-policy' in href.lower():
            return 'Monetary Policy'
        elif 'financial-stability' in href.lower():
            return 'Financial Stability'
        elif 'banking' in href.lower():
            return 'Banking'
        else:
            return 'General'

    def extract_image(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract associated image URL."""
        # Look for images in order of preference
        img_selectors = [
            'img.featured-image',
            'img.summary-icon',
            'figure img',
            '.summary-icon img',
            '.at-a-glance-box img',
            '.box-note img',
            'article img'
        ]
        
        for selector in img_selectors:
            img = soup.select_one(selector)
            if img and img.get('src'):
                src = img.get('src')
                if src.startswith('http'):
                    return src
                return urljoin(self.base_url, src)
        
        return None

    def extract_clean_text(self, content_div) -> str:
        """Extract clean text content from the page."""
        if not content_div:
            return ""
        
        # Remove script, style, nav, and other non-content elements
        for element in content_div.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            element.decompose()
        
        # Extract text and clean it
        text = content_div.get_text(separator='\n', strip=True)
        
        # Clean up whitespace and empty lines
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return '\n'.join(lines)

    def extract_embedded_links(self, content_div, base_url: str) -> List[str]:
        """Extract embedded links from content paragraphs and lists."""
        links = []
        
        if not content_div:
            return links
        
        # Find links in content elements only (p, ul, li, div with content, section)
        content_elements = content_div.find_all(['p', 'ul', 'li', 'div', 'section', 'article'])
        
        for element in content_elements:
            for link in element.find_all('a', href=True):
                href = link.get('href')
                if href:
                    # Skip non-content links
                    skip_patterns = [
                        'javascript:', 'mailto:', 'tel:', '#', 
                        'twitter.com', 'facebook.com', 'linkedin.com', 'youtube.com',
                        '/about/', '/contact/', '/sitemap', '/search',
                        'instagram.com', 'tiktok.com'
                    ]
                    
                    if any(skip in href.lower() for skip in skip_patterns):
                        continue
                    
                    full_url = urljoin(base_url, href)
                    if full_url not in links and full_url != base_url:
                        links.append(full_url)
        
        return links

    def extract_attachments(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Extract and process all attachments (PDFs, Excel, CSV)."""
        attachments = {
            'pdf_text': [],
            'excel_csv_text': []
        }
        
        # Find all attachment links
        attachment_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            if any(ext in href for ext in ['.pdf', '.xlsx', '.xls', '.csv']):
                full_url = urljoin(base_url, link.get('href'))
                title = link.get_text(strip=True)
                attachment_links.append((full_url, title, href))
        
        # Process each attachment
        for url, title, href in attachment_links:
            file_hash = hashlib.md5(url.encode()).hexdigest()
            
            # Skip if already processed
            if file_hash in self.processed_files:
                self.logger.info(f"Skipping already processed file: {title}")
                continue
            
            self.logger.info(f"Processing attachment: {title}")
            
            try:
                if '.pdf' in href:
                    text = self.extract_pdf_text(url)
                    if text:
                        attachments['pdf_text'].append({
                            'title': title,
                            'url': url,
                            'text': text
                        })
                        self.processed_files.add(file_hash)
                        
                elif any(ext in href for ext in ['.xlsx', '.xls', '.csv']):
                    text = self.extract_excel_csv_text(url)
                    if text:
                        attachments['excel_csv_text'].append({
                            'title': title,
                            'url': url,
                            'text': text
                        })
                        self.processed_files.add(file_hash)
                
            except Exception as e:
                self.logger.error(f"Failed to process attachment {url}: {e}")
        
        return attachments

    def extract_pdf_text(self, url: str) -> Optional[str]:
        """Extract text from PDF files with full content and table support."""
        response = self.get_page_with_stealth(url, delay=(2, 4))
        if not response:
            return None
        
        try:
            pdf_file = io.BytesIO(response.content)
            text_parts = []
            
            # Try pdfplumber first (better for tables and formatting)
            try:
                with pdfplumber.open(pdf_file) as pdf:
                    total_pages = len(pdf.pages)
                    self.logger.info(f"Processing PDF with {total_pages} pages")
                    
                    for page_num, page in enumerate(pdf.pages, 1):
                        # Extract text
                        text = page.extract_text()
                        if text:
                            text_parts.append(f"[PAGE {page_num}]\n{text.strip()}")
                        
                        # Extract tables
                        tables = page.extract_tables()
                        for table_num, table in enumerate(tables, 1):
                            if table:
                                # Convert table to text format
                                table_text = []
                                for row in table:
                                    if row:
                                        clean_row = [str(cell).strip() if cell else '' for cell in row]
                                        table_text.append('\t'.join(clean_row))
                                
                                if table_text:
                                    text_parts.append(f"[TABLE {table_num} - PAGE {page_num}]\n" + 
                                                    '\n'.join(table_text) + "\n[/TABLE]")
                
                if text_parts:
                    result = '\n\n'.join(text_parts)
                    self.logger.info(f"Extracted {len(result)} characters from PDF using pdfplumber")
                    return result
                    
            except Exception as e:
                self.logger.warning(f"pdfplumber failed for {url}: {e}, trying PyPDF2")
                
            # Fallback to PyPDF2
            pdf_file.seek(0)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            text_parts = []
            
            for page_num, page in enumerate(pdf_reader.pages, 1):
                text = page.extract_text()
                if text:
                    text_parts.append(f"[PAGE {page_num}]\n{text.strip()}")
            
            if text_parts:
                result = '\n\n'.join(text_parts)
                self.logger.info(f"Extracted {len(result)} characters from PDF using PyPDF2")
                return result
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to extract PDF text from {url}: {e}")
            return None

    def extract_excel_csv_text(self, url: str) -> Optional[str]:
        """Extract text from Excel and CSV files."""
        response = self.get_page_with_stealth(url, delay=(2, 4))
        if not response:
            return None
        
        try:
            if url.lower().endswith('.csv'):
                # Handle CSV files
                try:
                    # Try UTF-8 first
                    content = response.content.decode('utf-8')
                except UnicodeDecodeError:
                    # Fallback to other encodings
                    try:
                        content = response.content.decode('latin1')
                    except UnicodeDecodeError:
                        content = response.content.decode('cp1252', errors='ignore')
                
                df = pd.read_csv(io.StringIO(content))
                result = df.to_string(index=False)
                self.logger.info(f"Extracted CSV with {len(df)} rows, {len(df.columns)} columns")
                return result
                
            else:
                # Handle Excel files
                excel_file = io.BytesIO(response.content)
                
                # Read all sheets
                sheet_dict = pd.read_excel(excel_file, sheet_name=None)
                
                if len(sheet_dict) == 1:
                    # Single sheet
                    df = list(sheet_dict.values())[0]
                    result = df.to_string(index=False)
                    self.logger.info(f"Extracted Excel with {len(df)} rows, {len(df.columns)} columns")
                    return result
                else:
                    # Multiple sheets
                    text_parts = []
                    for sheet_name, df in sheet_dict.items():
                        text_parts.append(f"[SHEET: {sheet_name}]")
                        text_parts.append(df.to_string(index=False))
                        text_parts.append("")  # Blank line between sheets
                    
                    result = '\n'.join(text_parts)
                    total_rows = sum(len(df) for df in sheet_dict.values())
                    self.logger.info(f"Extracted Excel with {len(sheet_dict)} sheets, {total_rows} total rows")
                    return result
                
        except Exception as e:
            self.logger.error(f"Failed to extract Excel/CSV text from {url}: {e}")
            return None

    def check_status_changes(self, new_consultations: List[Dict]) -> List[Dict]:
        """Check for status changes in existing consultations and identify new ones."""
        updated_consultations = []
        
        for consultation in new_consultations:
            consultation_id = self.generate_consultation_id(consultation)
            
            if consultation_id in self.existing_data:
                existing = self.existing_data[consultation_id]
                
                # Check for status change
                if existing.get('status') != consultation['status']:
                    self.logger.info(f"Status change detected for '{consultation['title']}': "
                                   f"{existing.get('status')} → {consultation['status']}")
                    
                    # Mark for re-scraping by adding status change flag
                    consultation['_status_changed'] = True
                    consultation['status_changed_date'] = datetime.now().isoformat()
                    updated_consultations.append(consultation)
                else:
                    # No change, keep existing data
                    updated_consultations.append(existing)
            else:
                # New consultation
                self.logger.info(f"New consultation found: {consultation['title']}")
                consultation['_is_new'] = True
                updated_consultations.append(consultation)
        
        return updated_consultations

    def run(self):
        """Main scraping method."""
        self.logger.info("Starting RBA consultations scraping")
        
        # Fetch main consultations page
        response = self.get_page_with_stealth(self.consultations_url)
        if not response:
            self.logger.error("Failed to fetch main consultations page")
            return
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract consultation list
        consultations_list = self.extract_consultations_list(soup)
        self.logger.info(f"Found {len(consultations_list)} consultations")
        
        if not consultations_list:
            self.logger.warning("No consultations found on the page")
            return
        
        # Check for status changes and identify new consultations
        updated_consultations = self.check_status_changes(consultations_list)
        
        # Process each consultation
        final_data = {}
        new_count = 0
        updated_count = 0
        
        for i, consultation in enumerate(updated_consultations, 1):
            consultation_id = self.generate_consultation_id(consultation)
            
            title_or_headline = consultation.get('title') or consultation.get('headline', 'Unknown')
            self.logger.info(f"Processing consultation {i}/{len(updated_consultations)}: {title_or_headline}")
            
            # Check if we need to scrape content
            needs_scraping = (
                consultation.get('_is_new', False) or 
                consultation.get('_status_changed', False) or
                consultation_id not in self.existing_data or
                not self.existing_data.get(consultation_id, {}).get('page_text')
            )
            
            if needs_scraping:
                # Extract full content
                content_data = self.extract_page_content(consultation['url'])
                
                if 'error' in content_data:
                    self.logger.error(f"Failed to extract content for: {consultation.get('title', consultation.get('headline', 'Unknown'))}")
                    # Use existing data if available
                    if consultation_id in self.existing_data:
                        final_consultation = self.existing_data[consultation_id]
                        # Update status if changed
                        if consultation.get('_status_changed'):
                            final_consultation['status'] = consultation['status']
                            final_consultation['status_changed_date'] = consultation['status_changed_date']
                    else:
                        # Create minimal entry
                        final_consultation = consultation.copy()
                        title_field = consultation.get('title') or consultation.get('headline', 'Unknown')
                        final_consultation['headline'] = title_field
                        if 'title' in final_consultation:
                            final_consultation.pop('title')
                        final_consultation.update(content_data)  # Include error
                else:
                    # Successful extraction
                    consultation.update(content_data)
                    title_field = consultation.get('title') or consultation.get('headline', 'Unknown')
                    consultation['headline'] = title_field
                    if 'title' in consultation:
                        consultation.pop('title')
                    
                    # Clean up temporary flags
                    consultation.pop('_is_new', None)
                    consultation.pop('_status_changed', None)
                    
                    final_consultation = consultation
                    
                    if consultation_id not in self.existing_data:
                        new_count += 1
                    else:
                        updated_count += 1
                
                self.logger.info(f"Processed content for: {final_consultation.get('headline', 'Unknown')}")
            else:
                # Use existing data (already has 'headline' field)
                final_consultation = self.existing_data[consultation_id]
                self.logger.info(f"Using existing data for: {final_consultation.get('headline', 'Unknown')}")
            
            final_data[consultation_id] = final_consultation
        
        # Save the updated data
        self.save_data(final_data)
        
        self.logger.info("="*60)
        self.logger.info("SCRAPING COMPLETED")
        self.logger.info("="*60)
        self.logger.info(f"Total consultations: {len(final_data)}")
        self.logger.info(f"New consultations: {new_count}")
        self.logger.info(f"Updated consultations: {updated_count}")
        self.logger.info(f"Data saved to: {self.output_file}")
        self.logger.info(f"Log saved to: {self.log_file}")

def main():
    """Main entry point."""
    scraper = RBAConsultationsScraper()
    scraper.run()

if __name__ == "__main__":
    main()