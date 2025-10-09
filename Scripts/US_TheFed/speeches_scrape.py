"""
Federal Reserve Speeches Scraper
Scrapes all speeches with full content extraction for LLM analysis.

Usage:
    python3 fed_speeches_scraper.py                # Scrape all new speeches
    python3 fed_speeches_scraper.py --debug        # Verbose output
    python3 fed_speeches_scraper.py --max-pages 5  # Limit pages (testing)
"""

import os
import json
import time
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin
import re
import io
import hashlib

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
import pandas as pd

# Configuration
BASE_URL = "https://www.federalreserve.gov"
SPEECHES_URL = f"{BASE_URL}/newsevents/speeches.htm"
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "fed_speeches.json"

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)


class FedSpeechesScraper:
    """Main scraper class for Federal Reserve Speeches."""
    
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
        
        # Visit homepage to establish session
        try:
            self.session.get(BASE_URL)
            time.sleep(2)
        except Exception as e:
            print(f"Warning: Could not establish session: {e}")
    
    def scrape_index_pages(self) -> List[Dict]:
        """Scrape all speeches index pages using Selenium for pagination."""
        self._setup_driver()
        self._setup_session()
        
        print(f"Loading speeches index page: {SPEECHES_URL}")
        self.driver.get(SPEECHES_URL)
        time.sleep(5)  # Wait for Angular to load
        
        all_speeches = []
        page_count = 0
        
        while True:
            page_count += 1
            print(f"\nScraping page {page_count}...")
            
            # Wait for content to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "angularEvents"))
                )
            except TimeoutException:
                print("Timeout waiting for content")
                break
            
            # Parse current page
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            speeches = self._parse_index_page(soup)
            
            # Filter out already scraped speeches
            new_speeches = [s for s in speeches if s['id'] not in self.existing_ids]
            print(f"Found {len(speeches)} speeches, {len(new_speeches)} are new")
            
            all_speeches.extend(new_speeches)
            
            # Check max pages limit
            if self.max_pages and page_count >= self.max_pages:
                print(f"Reached max pages limit: {self.max_pages}")
                break
            
            # Try to navigate to next page
            try:
                # Find Next button in pagination
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
                
                # Click using JavaScript
                self.driver.execute_script("arguments[0].click();", next_link)
                
                print("Navigating to next page...")
                time.sleep(4)  # Wait for page load
                
            except Exception as e:
                print(f"No more pages available: {e}")
                break
        
        self.driver.quit()
        return all_speeches
    
    def _parse_index_page(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse speeches from the index page."""
        speeches = []
        
        # Find all speech rows
        rows = soup.select('div.angularEvents div.row.ng-scope')
        
        for row in rows:
            try:
                # Extract date
                date_elem = row.select_one('time.itemDate')
                date_str = date_elem.get('datetime', '') if date_elem else ''
                published_date = self._parse_date(date_str)
                
                # Extract title and URL
                title_elem = row.select_one('p.itemTitle a')
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                href = title_elem.get('href', '')
                url = urljoin(BASE_URL, href)
                
                # Extract speaker
                speaker_elem = row.select_one('p.news__speaker')
                speaker = speaker_elem.get_text(strip=True) if speaker_elem else ""
                
                # Extract role from speaker string (if present)
                role = self._extract_role(speaker)
                
                # Extract location
                location_elem = row.select_one('p.result__location')
                location = location_elem.get_text(strip=True) if location_elem else ""
                
                # Generate unique ID from URL
                speech_id = self._generate_speech_id(url)
                
                speeches.append({
                    'id': speech_id,
                    'url': url,
                    'title': title,
                    'speaker': speaker,
                    'role': role,
                    'date': published_date,
                    'location': location,
                })
                
            except Exception as e:
                if self.debug:
                    print(f"  Error parsing row: {e}")
                continue
        
        return speeches
    
    def _generate_speech_id(self, url: str) -> str:
        """Generate speech ID from URL (e.g., powell20250615a)."""
        # Extract filename from URL
        parts = url.rstrip('/').split('/')
        filename = parts[-1]
        # Remove .htm extension
        speech_id = filename.replace('.htm', '')
        return speech_id
    
    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format."""
        if not date_str:
            return str(datetime.now().year)
        
        try:
            # Format: MM/DD/YYYY
            dt = datetime.strptime(date_str.strip(), '%m/%d/%Y')
            return dt.strftime('%Y-%m-%d')
        except:
            return date_str
    
    def _extract_role(self, speaker_str: str) -> str:
        """Extract role from speaker string."""
        roles = [
            'Chair', 'Vice Chair', 'Governor', 'President',
            'Vice President', 'Board Member'
        ]
        for role in roles:
            if role in speaker_str:
                return role
        return "Federal Reserve Official"
    
    def scrape_speech(self, metadata: Dict) -> Dict:
        """Scrape a single speech page."""
        print(f"\nScraping: {metadata['title'][:60]}...")
        print(f"  Speaker: {metadata['speaker']}")
        
        try:
            # Check if URL points to a PDF
            if metadata['url'].endswith('.pdf'):
                return self._scrape_pdf_speech(metadata)
            else:
                return self._scrape_html_speech(metadata)
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None
    
    def _scrape_pdf_speech(self, metadata: Dict) -> Dict:
        """Scrape a speech that is directly a PDF."""
        pdf_data = self._extract_pdf(metadata['url'])
        
        if pdf_data:
            print(f"  ✓ PDF extracted: {len(pdf_data['extracted_text'])} characters")
            
            return {
                'id': metadata['id'],
                'url': metadata['url'],
                'title': metadata['title'],
                'speaker': metadata['speaker'],
                'role': metadata['role'],
                'date': metadata['date'],
                'scraped_date': self.scraped_date,
                'content': {
                    'main_text': pdf_data['extracted_text'],
                    'attachments': {
                        'pdfs': [pdf_data],
                    }
                }
            }
        return None
    
    def _scrape_html_speech(self, metadata: Dict) -> Dict:
        """Scrape a speech from an HTML page."""
        response = self.session.get(metadata['url'], timeout=30)
        response.raise_for_status()
        time.sleep(1)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract main content
        main_text = self._extract_main_text(soup)
        if self.debug:
            print(f"  Main text: {len(main_text)} characters")
        
        # Extract attachments (PDFs, Excel, CSV)
        attachments = self._extract_attachments(soup, metadata['url'])
        total_att = len(attachments['pdfs']) + len(attachments.get('excels', [])) + len(attachments.get('csvs', []))
        if total_att > 0:
            print(f"  ✓ Attachments: {total_att} files")
        
        return {
            'id': metadata['id'],
            'url': metadata['url'],
            'title': metadata['title'],
            'speaker': metadata['speaker'],
            'role': metadata['role'],
            'date': metadata['date'],
            'scraped_date': self.scraped_date,
            'content': {
                'main_text': main_text,
                'attachments': attachments,
            }
        }
    
    def _extract_main_text(self, soup: BeautifulSoup) -> str:
        """Extract main text content from speech page."""
        text_parts = []
        
        # Find the article container
        article = soup.select_one('#article')
        
        if article:
            article_copy = BeautifulSoup(str(article), 'html.parser')
            
            # Remove unwanted elements
            for tag in article_copy.select('script, style, nav, .breadcrumb, .share, .panel-attachments, .hidden'):
                tag.decompose()
            
            # Extract title
            title = article_copy.select_one('h3.title')
            if title:
                text_parts.append(title.get_text(separator=' ', strip=True))
            
            # Extract speaker and location
            speaker = article_copy.select_one('p.speaker')
            if speaker:
                text_parts.append(speaker.get_text(strip=True))
            
            location = article_copy.select_one('p.location')
            if location:
                text_parts.append(location.get_text(strip=True))
            
            # Extract all paragraphs
            paragraphs = article_copy.select('p')
            for p in paragraphs:
                para_text = p.get_text(strip=True)
                if para_text and len(para_text) > 20:
                    # Skip if it's a speaker or location (already extracted)
                    if p.get('class') and ('speaker' in p.get('class') or 'location' in p.get('class')):
                        continue
                    text_parts.append(para_text)
            
            # Extract footnotes
            footnotes = article_copy.select('hr + p')
            if footnotes:
                text_parts.append("\n--- Footnotes ---")
                for fn in footnotes:
                    text_parts.append(fn.get_text(strip=True))
            
            full_text = '\n\n'.join(text_parts)
            return self._clean_text(full_text)
        
        return ""
    
    def _extract_attachments(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Extract and process all attachments (PDFs, Excel, CSV) from article content only."""
        attachments = {
            'pdfs': [],
            'excels': [],
            'csvs': [],
        }
        
        # Find the article container only (avoid footer/navigation links)
        article = soup.select_one('#article')
        if not article:
            return attachments
        
        # Remove navigation/footer sections before processing
        article_copy = BeautifulSoup(str(article), 'html.parser')
        for unwanted in article_copy.select('nav, footer, .breadcrumb, .share, .stay-connected, [role="navigation"]'):
            unwanted.decompose()
        
        file_links = article_copy.select('a[href]')
        processed_urls = set()
        
        for link in file_links:
            href = link.get('href', '')
            if not href:
                continue
            
            full_url = urljoin(BASE_URL, href)
            
            # Skip if already processed
            if full_url in processed_urls:
                continue
            
            # CRITICAL: Only process actual file attachments from speech content
            # Skip any links that don't point to files
            if not any(full_url.endswith(ext) for ext in ['.pdf', '.xlsx', '.xls', '.csv']):
                continue
            
            # Additional filter: must be from /newsevents/speech/ path (speech-specific files)
            # or /files/ directory (common attachment location)
            if '/newsevents/speech/' not in full_url and '/files/' not in full_url:
                if self.debug:
                    print(f"      Skipping non-speech file: {full_url}")
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
        """Extract text from PDF including tables and OCR."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            file_name = url.split('/')[-1]
            
            extracted_text = []
            
            # Try pdfplumber (best for tables)
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
            
            # OCR attempt if text extraction failed
            if not extracted_text:
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
                    pass
            
            full_text = '\n\n'.join(extracted_text)
            full_text = self._clean_text(full_text)
            
            if self.debug:
                print(f"    PDF: {file_name} ({len(full_text)} chars)")
            
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
            
            if self.debug:
                print(f"    Excel: {file_name} ({len(full_text)} chars)")
            
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
            
            if self.debug:
                print(f"    CSV: {file_name} ({len(csv_text)} chars)")
            
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
    
    def _clean_text(self, text: str) -> str:
        """Clean text by removing extra whitespace and boilerplate."""
        # Remove common boilerplate phrases
        boilerplate_phrases = [
            r'Skip to main content',
            r'An official website of the United States Government',
            r"Here's how you know",
            r'Official websites use \.gov',
            r'Secure \.gov websites use HTTPS',
            r'Board of Governors of the Federal Reserve System',
            r'Stay Connected',
            r'Subscribe to RSS',
            r'Subscribe to Email',
            r'Last Update:.*\d{4}',
            r'Return to text',
            r'Back to Top',
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
    
    def save_results(self, new_speeches: List[Dict]):
        """Save scraped data to JSON file."""
        all_data = self.existing_data + new_speeches
        all_data.sort(key=lambda x: x.get('date', ''), reverse=True)
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print(f"Saved {len(all_data)} total speeches to {OUTPUT_FILE}")
        print(f"Added {len(new_speeches)} new speeches in this run")
        print(f"{'='*60}")
    
    def run(self):
        """Main execution method."""
        print("=" * 60)
        print("Federal Reserve Speeches Scraper")
        print("=" * 60)
        
        # Scrape index pages
        speeches_metadata = self.scrape_index_pages()
        
        if not speeches_metadata:
            print("\nNo new speeches found")
            return
        
        print(f"\n{'='*60}")
        print(f"Found {len(speeches_metadata)} new speeches")
        print(f"{'='*60}")
        
        # Scrape each speech
        scraped_speeches = []
        for i, metadata in enumerate(speeches_metadata, 1):
            print(f"\n[{i}/{len(speeches_metadata)}]", end=' ')
            speech_data = self.scrape_speech(metadata)
            
            if speech_data:
                scraped_speeches.append(speech_data)
            
            time.sleep(2)  # Rate limiting
        
        # Save results
        if scraped_speeches:
            self.save_results(scraped_speeches)
        
        print("\n" + "=" * 60)
        print("Scraping completed successfully!")
        print("=" * 60)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Scrape Federal Reserve Speeches',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 fed_speeches_scraper.py                # Scrape all new speeches
  python3 fed_speeches_scraper.py --debug        # Verbose output
  python3 fed_speeches_scraper.py --max-pages 5  # Limit pages (testing)
        """
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=None,
        help='Maximum number of pages to scrape'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode for verbose output'
    )
    
    args = parser.parse_args()
    
    scraper = FedSpeechesScraper(max_pages=args.max_pages, debug=args.debug)
    scraper.run()


if __name__ == "__main__":
    main()