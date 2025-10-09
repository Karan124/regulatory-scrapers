"""
Federal Reserve Press Release Scraper
A production-ready scraper for extracting all Federal Reserve press releases
with comprehensive content extraction from PDFs, Excel files, and linked pages.

Usage:
    python3 press_releases_scrape.py                    # Scrape all pages
    python3 press_releases_scrape.py --incremental      # Last 3 pages only
    python3 press_releases_scrape.py --max-pages 2      # First 2 pages
    python3 press_releases_scrape.py --debug            # Verbose output
"""

import os
import json
import time
import hashlib
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import re
import io

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options

from bs4 import BeautifulSoup
import PyPDF2
import pdfplumber
from PIL import Image
import pytesseract
import openpyxl
import pandas as pd

# Configuration
BASE_URL = "https://www.federalreserve.gov"
INDEX_URL = f"{BASE_URL}/newsevents/pressreleases.htm"
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "fed_press_releases.json"
MAX_PAGES_INCREMENTAL = 3
DOWNLOAD_DIR = OUTPUT_DIR / "temp_downloads"

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)
DOWNLOAD_DIR.mkdir(exist_ok=True)


class FedReserveScraper:
    """Main scraper class for Federal Reserve press releases."""
    
    def __init__(self, max_pages: Optional[int] = None, debug: bool = False):
        self.max_pages = max_pages
        self.debug = debug
        self.session = requests.Session()
        self.driver = None
        self.existing_data = self._load_existing_data()
        self.existing_ids = {item['id'] for item in self.existing_data}
        self.scraped_date = datetime.utcnow().isoformat() + 'Z'
        
    def _load_existing_data(self) -> List[Dict]:
        """Load existing scraped data to avoid duplicates."""
        if OUTPUT_FILE.exists():
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load existing data: {e}")
        return []
    
    def _setup_driver(self):
        """Set up Selenium WebDriver with stealth options."""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        prefs = {
            "download.default_directory": str(DOWNLOAD_DIR.absolute()),
            "download.prompt_for_download": False,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        
        # Hide automation flags
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def _setup_session(self):
        """Set up requests session with browser-like headers."""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        try:
            self.session.get(BASE_URL)
            time.sleep(2)
        except Exception as e:
            print(f"Warning: Could not establish session: {e}")
    
    def scrape_index_page(self) -> List[Dict]:
        """Scrape the index page to get all press release links."""
        self._setup_driver()
        self._setup_session()
        
        print(f"Loading index page: {INDEX_URL}")
        self.driver.get(INDEX_URL)
        time.sleep(3)
        
        all_releases = []
        page_count = 0
        
        while True:
            page_count += 1
            print(f"\nScraping page {page_count}...")
            
            # Parse current page
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            releases = self._parse_index_page(soup)
            
            # Filter out already scraped releases
            new_releases = [r for r in releases if r['id'] not in self.existing_ids]
            print(f"Found {len(releases)} releases, {len(new_releases)} are new")
            
            all_releases.extend(new_releases)
            
            # Check max pages limit
            if self.max_pages and page_count >= self.max_pages:
                print(f"Reached max pages limit: {self.max_pages}")
                break
            
            # Try to navigate to next page
            try:
                # Find pagination Next button
                next_buttons = self.driver.find_elements(By.CSS_SELECTOR, "li.pagination-next")
                
                if not next_buttons:
                    print("No pagination controls found")
                    break
                
                next_li = next_buttons[0]
                
                # Check if disabled
                if 'disabled' in next_li.get_attribute('class'):
                    print("Reached last page (Next button disabled)")
                    break
                
                # Get the link and click
                next_link = next_li.find_element(By.TAG_NAME, "a")
                
                # Scroll into view
                self.driver.execute_script("arguments[0].scrollIntoView(true);", next_link)
                time.sleep(1)
                
                # Click using JavaScript to avoid interception
                self.driver.execute_script("arguments[0].click();", next_link)
                
                print("Navigating to next page...")
                time.sleep(4)  # Wait for page load
                
            except Exception as e:
                print(f"No more pages available: {e}")
                break
        
        self.driver.quit()
        return all_releases
    
    def _parse_index_page(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse a single index page to extract press release metadata."""
        releases = []
        rows = soup.select('div.row.ng-scope')
        
        for row in rows:
            try:
                date_elem = row.select_one('time.itemDate')
                if not date_elem:
                    continue
                    
                date_str = date_elem.get('datetime', '').strip()
                published_date = self._parse_date(date_str)
                
                link_elem = row.select_one('a[href*="/newsevents/pressreleases/"]')
                if not link_elem:
                    continue
                    
                href = link_elem.get('href', '').strip()
                url = urljoin(BASE_URL, href)
                headline = link_elem.get_text(strip=True)
                
                press_id = href.split('/')[-1].replace('.htm', '')
                
                theme_elem = row.select_one('em.ng-binding')
                theme = theme_elem.get_text(strip=True) if theme_elem else None
                
                releases.append({
                    'id': press_id,
                    'url': url,
                    'headline': headline,
                    'theme': theme,
                    'published_date': published_date,
                })
            except Exception as e:
                print(f"Error parsing row: {e}")
                continue
        
        return releases
    
    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format."""
        try:
            dt = datetime.strptime(date_str, '%m/%d/%Y')
            return dt.strftime('%Y-%m-%d')
        except:
            return date_str
    
    def scrape_press_release(self, metadata: Dict) -> Dict:
        """Scrape a single press release page and extract all content."""
        print(f"\nScraping: {metadata['headline']}")
        
        try:
            response = self.session.get(metadata['url'], timeout=30)
            response.raise_for_status()
            time.sleep(1)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract main content
            main_text = self._extract_main_text(soup)
            if self.debug:
                print(f"  Main text: {len(main_text)} characters")
            
            # Extract linked pages - THIS IS THE KEY PART
            linked_pages = self._extract_linked_pages(soup, metadata['url'])
            print(f"  ✓ Linked pages: {len(linked_pages)}")
            if self.debug and linked_pages:
                for lp in linked_pages:
                    print(f"    - {lp['url']} ({len(lp['text'])} chars)")
            
            # Extract attachments
            attachments = self._extract_attachments(soup, metadata['url'])
            total_att = len(attachments['pdfs']) + len(attachments['excels']) + len(attachments['csvs'])
            print(f"  ✓ Attachments: {total_att} files")
            
            # Extract image
            image_url = self._extract_image(soup, metadata['url'])
            
            return {
                'id': metadata['id'],
                'url': metadata['url'],
                'headline': metadata['headline'],
                'theme': metadata['theme'],
                'published_date': metadata['published_date'],
                'scraped_date': self.scraped_date,
                'content': {
                    'main_page_text': main_text,
                    'linked_pages': linked_pages,
                },
                'attachments': attachments,
                'image_url': image_url,
            }
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None
    
    def _extract_main_text(self, soup: BeautifulSoup) -> str:
        """Extract main text content from press release page."""
        text_parts = []
        
        # Strategy 1: Look for the main article content area (#article)
        article = soup.select_one('#article')
        
        if article:
            # Clone to avoid modifying original
            article_copy = BeautifulSoup(str(article), 'html.parser')
            
            # Remove unwanted navigation and supplementary elements
            for tag in article_copy.select('script, style, nav, .share, .panel-related, .panel-attachments, .breadcrumb, #t3_nav'):
                tag.decompose()
            
            text = article_copy.get_text(separator='\n', strip=True)
            return self._clean_text(text)
        
        # Strategy 2: For statement/linked pages - extract from content container
        content_div = soup.select_one('#content[role="main"]')
        
        if content_div:
            content_copy = BeautifulSoup(str(content_div), 'html.parser')
            
            # Remove all navigation, headers, and boilerplate
            for tag in content_copy.select('script, style, nav, .breadcrumb, .page-header, #t3_nav, .lastUpdate, #lastUpdate'):
                tag.decompose()
            
            # Extract the heading section
            heading = content_copy.select_one('.heading')
            if heading:
                # Get date
                date_elem = heading.select_one('.article__time')
                if date_elem:
                    text_parts.append(date_elem.get_text(strip=True))
                
                # Get title
                title_elem = heading.select_one('h3, h2, h1')
                if title_elem:
                    text_parts.append(title_elem.get_text(strip=True))
            
            # Extract the main body content
            body_paragraphs = content_copy.select('.col-xs-12 p')
            for p in body_paragraphs:
                para_text = p.get_text(strip=True)
                if para_text:
                    text_parts.append(para_text)
            
            # If we got structured content, return it
            if text_parts:
                full_text = '\n\n'.join(text_parts)
                return self._clean_text(full_text)
            
            # Otherwise get all remaining text
            text = content_copy.get_text(separator='\n', strip=True)
            return self._clean_text(text)
        
        # Strategy 3: Fallback to body but remove common boilerplate
        body = soup.select_one('body')
        if body:
            body_copy = BeautifulSoup(str(body), 'html.parser')
            
            # Remove all navigation, headers, footers, and boilerplate
            for tag in body_copy.select('script, style, nav, header, footer, .skip-link, .breadcrumb, .navbar, .menu, #header, #footer, .lastUpdate'):
                tag.decompose()
            
            text = body_copy.get_text(separator='\n', strip=True)
            return self._clean_text(text)
        
        return ""
    
    def _extract_linked_pages(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """
        Extract text from secondary linked press release pages.
        This is critical for getting statement pages and related content.
        """
        linked_pages = []
        seen_urls = set()
        
        # Find ALL links on the page
        all_links = soup.find_all('a', href=True)
        
        if self.debug:
            print(f"  DEBUG: Scanning {len(all_links)} links for linked pages")
        
        for link in all_links:
            href = link.get('href', '').strip()
            if not href:
                continue
            
            # Convert to absolute URL
            full_url = urljoin(BASE_URL, href)
            
            # Must be a press release page
            if '/newsevents/pressreleases/' not in full_url:
                continue
            
            # Must end with .htm
            if not full_url.endswith('.htm'):
                continue
            
            # Skip the main page itself
            if full_url == base_url:
                continue
            
            # Skip duplicates
            if full_url in seen_urls:
                continue
            
            seen_urls.add(full_url)
            
            # This is a candidate linked page
            if self.debug:
                link_text = link.get_text(strip=True)
                print(f"  DEBUG: Found linked page: {full_url} ('{link_text}')")
            
            # Fetch and extract the page
            page_data = self._fetch_linked_page(full_url)
            if page_data:
                linked_pages.append(page_data)
        
        return linked_pages
    
    def _fetch_linked_page(self, url: str) -> Optional[Dict]:
        """Fetch and extract text from a linked page."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            time.sleep(1)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = self._extract_main_text(soup)
            
            # Must have meaningful content
            if page_text and len(page_text) > 50:
                if self.debug:
                    print(f"    ✓ Extracted {len(page_text)} characters")
                return {
                    'url': url,
                    'text': page_text,
                }
            else:
                if self.debug:
                    print(f"    ✗ Insufficient content ({len(page_text)} chars)")
        except Exception as e:
            if self.debug:
                print(f"    ✗ Error: {e}")
        
        return None
    
    def _extract_attachments(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Extract and process all attachments (PDFs, Excel, CSV)."""
        attachments = {
            'pdfs': [],
            'excels': [],
            'csvs': [],
        }
        
        file_links = soup.select('a[href]')
        processed_urls = set()
        
        for link in file_links:
            href = link.get('href', '')
            if not href:
                continue
            
            full_url = urljoin(BASE_URL, href)
            
            if full_url in processed_urls:
                continue
            
            # Process by file type
            if full_url.endswith('.pdf'):
                processed_urls.add(full_url)
                pdf_data = self._extract_pdf(full_url)
                if pdf_data:
                    attachments['pdfs'].append(pdf_data)
            
            elif full_url.endswith(('.xlsx', '.xls')):
                processed_urls.add(full_url)
                excel_data = self._extract_excel(full_url)
                if excel_data:
                    attachments['excels'].append(excel_data)
            
            elif full_url.endswith('.csv'):
                processed_urls.add(full_url)
                csv_data = self._extract_csv(full_url)
                if csv_data:
                    attachments['csvs'].append(csv_data)
        
        return attachments
    
    def _extract_pdf(self, url: str) -> Optional[Dict]:
        """Extract text from PDF including OCR for images."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            file_name = url.split('/')[-1]
            
            extracted_text = []
            
            # Try pdfplumber first (better for tables)
            try:
                with pdfplumber.open(pdf_file) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            extracted_text.append(text)
                        
                        # Extract tables
                        tables = page.extract_tables()
                        for table in tables:
                            table_text = self._format_table(table)
                            extracted_text.append(table_text)
            except:
                pass
            
            # Fallback to PyPDF2
            if not extracted_text:
                pdf_file.seek(0)
                try:
                    reader = PyPDF2.PdfReader(pdf_file)
                    for page in reader.pages:
                        text = page.extract_text()
                        if text:
                            extracted_text.append(text)
                except:
                    pass
            
            # OCR attempt (requires tesseract)
            try:
                pdf_file.seek(0)
                with pdfplumber.open(pdf_file) as pdf:
                    for page_num, page in enumerate(pdf.pages):
                        img = page.to_image(resolution=300)
                        pil_img = img.original
                        ocr_text = pytesseract.image_to_string(pil_img)
                        if ocr_text.strip():
                            extracted_text.append(f"[OCR Page {page_num + 1}]\n{ocr_text}")
            except:
                pass  # OCR not available
            
            full_text = '\n\n'.join(extracted_text)
            full_text = self._clean_text(full_text)
            
            return {
                'file_name': file_name,
                'url': url,
                'extracted_text': full_text,
            }
        except Exception as e:
            if self.debug:
                print(f"    Error extracting PDF {url}: {e}")
            return None
    
    def _extract_excel(self, url: str) -> Optional[Dict]:
        """Extract text from Excel files."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            file_name = url.split('/')[-1]
            excel_file = io.BytesIO(response.content)
            
            extracted_text = []
            xl = pd.ExcelFile(excel_file)
            
            for sheet_name in xl.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                sheet_text = f"[Sheet: {sheet_name}]\n{df.to_string(index=False)}"
                extracted_text.append(sheet_text)
            
            full_text = '\n\n'.join(extracted_text)
            full_text = self._clean_text(full_text)
            
            return {
                'file_name': file_name,
                'url': url,
                'extracted_text': full_text,
            }
        except Exception as e:
            if self.debug:
                print(f"    Error extracting Excel {url}: {e}")
            return None
    
    def _extract_csv(self, url: str) -> Optional[Dict]:
        """Extract text from CSV files."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            file_name = url.split('/')[-1]
            df = pd.read_csv(io.StringIO(response.text))
            csv_text = df.to_string(index=False)
            csv_text = self._clean_text(csv_text)
            
            return {
                'file_name': file_name,
                'url': url,
                'extracted_text': csv_text,
            }
        except Exception as e:
            if self.debug:
                print(f"    Error extracting CSV {url}: {e}")
            return None
    
    def _format_table(self, table: List[List]) -> str:
        """Format extracted table data as text."""
        if not table:
            return ""
        
        lines = []
        for row in table:
            line = ' | '.join([str(cell) if cell else '' for cell in row])
            lines.append(line)
        
        return '\n'.join(lines)
    
    def _extract_image(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract associated image URL if available."""
        article = soup.select_one('#article')
        if article:
            img = article.select_one('img[src]')
            if img:
                img_url = img.get('src')
                return urljoin(BASE_URL, img_url)
        return None
    
    def _clean_text(self, text: str) -> str:
        """Clean text by removing extra whitespace and boilerplate."""
        # Remove common boilerplate phrases
        boilerplate_phrases = [
            r'Skip to main content',
            r'An official website of the United States Government',
            r"Here's how you know",
            r'Official websites use \.gov',
            r'A \.gov website belongs to an official government organization in the United States\.',
            r'Secure \.gov websites use HTTPS',
            r'A lock \( Lock Locked padlock icon \) or https:// means you\'ve safely connected to the \.gov website\.',
            r'Share sensitive information only on official, secure websites\.',
            r'Back to Home',
            r'Board of Governors of the Federal Reserve System',
            r'Stay Connected',
            r'Federal Reserve Facebook Page',
            r'Federal Reserve Instagram Page',
            r'Federal Reserve YouTube Page',
            r'Federal Reserve Flickr Page',
            r'Federal Reserve LinkedIn Page',
            r'Federal Reserve Threads Page',
            r'Federal Reserve X Page',
            r'Federal Reserve Bluesky Page',
            r'Subscribe to RSS',
            r'Subscribe to Email',
            r'Recent Postings',
            r'Calendar',
            r'Publications',
            r'Site Map',
            r'A-Z index',
            r'Careers',
            r'FAQs',
            r'Videos',
            r'Contact',
            r'Search Submit Search Button',
            r'Advanced Toggle Dropdown Menu',
            r'Main Menu Toggle Button',
            r'Sections',
            r'Search Toggle Button',
            r'Home News & Events Press Releases',
            r'Last Update:.*\d{4}',
        ]
        
        for phrase in boilerplate_phrases:
            text = re.sub(phrase, '', text, flags=re.IGNORECASE)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Split into lines and clean
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line and len(line) > 1]
        
        # Remove duplicate consecutive lines
        cleaned_lines = []
        prev_line = None
        for line in lines:
            if line != prev_line:
                cleaned_lines.append(line)
                prev_line = line
        
        return '\n'.join(cleaned_lines)
    
    def save_results(self, new_releases: List[Dict]):
        """Save scraped data to JSON file."""
        all_data = self.existing_data + new_releases
        all_data.sort(key=lambda x: x.get('published_date', ''), reverse=True)
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print(f"Saved {len(all_data)} total releases to {OUTPUT_FILE}")
        print(f"Added {len(new_releases)} new releases in this run")
        print(f"{'='*60}")
    
    def run(self):
        """Main execution method."""
        print("=" * 60)
        print("Federal Reserve Press Release Scraper")
        print("=" * 60)
        
        # Scrape index
        releases_metadata = self.scrape_index_page()
        
        if not releases_metadata:
            print("\nNo new releases found")
            return
        
        print(f"\n{'='*60}")
        print(f"Found {len(releases_metadata)} new press releases to scrape")
        print(f"{'='*60}")
        
        # Scrape each release
        scraped_releases = []
        for i, metadata in enumerate(releases_metadata, 1):
            print(f"\n[{i}/{len(releases_metadata)}]", end=' ')
            release_data = self.scrape_press_release(metadata)
            
            if release_data:
                scraped_releases.append(release_data)
            
            time.sleep(2)  # Rate limiting
        
        # Save results
        if scraped_releases:
            self.save_results(scraped_releases)
        
        print("\n" + "=" * 60)
        print("Scraping completed successfully!")
        print("=" * 60)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Scrape Federal Reserve press releases',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 press_releases_scrape.py                    # Scrape all pages
  python3 press_releases_scrape.py --incremental      # Last 3 pages only
  python3 press_releases_scrape.py --max-pages 2      # First 2 pages
  python3 press_releases_scrape.py --debug            # Verbose output
        """
    )
    parser.add_argument(
        '--incremental',
        action='store_true',
        help=f'Run in incremental mode (scrape only last {MAX_PAGES_INCREMENTAL} pages)'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=None,
        help='Maximum number of pages to scrape (overrides --incremental)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode for verbose output'
    )
    
    args = parser.parse_args()
    
    max_pages = args.max_pages
    if max_pages is None and args.incremental:
        max_pages = MAX_PAGES_INCREMENTAL
    
    scraper = FedReserveScraper(max_pages=max_pages, debug=args.debug)
    scraper.run()


if __name__ == "__main__":
    main()