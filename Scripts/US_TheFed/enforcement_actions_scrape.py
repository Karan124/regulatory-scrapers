"""
Federal Reserve Enforcement Actions Scraper
Scrapes all enforcement actions for LLM analysis.

Usage:
    python3 enforcement_actions_scrape.py                # Scrape all pages
    python3 enforcement_actions_scrape.py --max-pages 5  # First 5 pages
    python3 enforcement_actions_scrape.py --debug        # Verbose output
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
INDEX_URL = f"{BASE_URL}/supervisionreg/enforcementactions.htm"
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "fed_enforcement_actions.json"

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)


class EnforcementActionsScraper:
    """Main scraper class for Enforcement Actions."""
    
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
        """Scrape all enforcement action index pages."""
        self._setup_driver()
        self._setup_session()
        
        print(f"Loading index page: {INDEX_URL}")
        self.driver.get(INDEX_URL)
        time.sleep(4)  # Wait for JavaScript to load
        
        all_actions = []
        page_count = 0
        
        while True:
            page_count += 1
            print(f"\nScraping page {page_count}...")
            
            # Parse current page
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            actions = self._parse_index_page(soup)
            
            # Filter out already scraped actions
            new_actions = [a for a in actions if a['id'] not in self.existing_ids]
            print(f"Found {len(actions)} actions, {len(new_actions)} are new")
            
            all_actions.extend(new_actions)
            
            # Check max pages limit
            if self.max_pages and page_count >= self.max_pages:
                print(f"Reached max pages limit: {self.max_pages}")
                break
            
            # Try to navigate to next page
            try:
                # Find Next button
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
        return all_actions
    
    def _parse_index_page(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse enforcement actions from index page table."""
        actions = []
        
        # Find the table
        table = soup.select_one('table.pubtables')
        if not table:
            return actions
        
        # Find all tbody elements (each represents one action)
        tbody_elements = table.select('tbody')
        
        for tbody in tbody_elements:
            try:
                row = tbody.select_one('tr')
                if not row:
                    continue
                
                # Extract effective date
                effective_date_elem = row.select_one('td[headers*="effective"]')
                effective_date = effective_date_elem.get_text(strip=True) if effective_date_elem else ""
                published_date = self._parse_date(effective_date)
                
                # Extract party information
                org_elem = row.select_one('td[headers*="organization"]')
                individual_elem = row.select_one('td[headers*="individual"]')
                
                organization = ""
                individual = ""
                institution = ""
                
                if org_elem:
                    organization = org_elem.get_text(strip=True)
                
                if individual_elem:
                    # Individual name is in <b> tag
                    ind_name = individual_elem.select_one('b')
                    individual = ind_name.get_text(strip=True) if ind_name else ""
                    
                    # Institution is in <div>
                    inst_div = individual_elem.select_one('div')
                    institution = inst_div.get_text(strip=True) if inst_div else ""
                
                # Construct headline
                if organization:
                    headline = organization
                elif individual:
                    headline = f"{individual} ({institution})" if institution else individual
                else:
                    headline = "Enforcement Action"
                
                # Extract action type and link
                action_elem = row.select_one('td[headers="action"]')
                if not action_elem:
                    continue
                
                action_type_text = action_elem.get_text(strip=True)
                link_elem = action_elem.select_one('a')
                
                if not link_elem:
                    continue
                
                href = link_elem.get('href', '')
                url = urljoin(BASE_URL, href)
                link_text = link_elem.get_text(strip=True)
                
                # Determine type
                if 'Press Release' in link_text:
                    action_type = "Press Release"
                elif 'Letter' in link_text or url.endswith('.pdf'):
                    action_type = "Letter (PDF)"
                else:
                    action_type = "Document"
                
                # Generate unique ID
                action_id = self._generate_action_id(url, published_date, headline)
                
                actions.append({
                    'id': action_id,
                    'url': url,
                    'headline': headline,
                    'theme': 'Enforcement & Supervision',
                    'type': action_type,
                    'published_date': published_date,
                    'action_category': action_type_text.split('\n')[0].strip(),  # e.g., "Prohibition from Banking"
                })
                
            except Exception as e:
                if self.debug:
                    print(f"  Error parsing row: {e}")
                continue
        
        return actions
    
    def _generate_action_id(self, url: str, date: str, headline: str) -> str:
        """Generate a unique action ID."""
        # Create hash from URL + date + headline
        unique_string = f"{url}|{date}|{headline}"
        hash_obj = hashlib.md5(unique_string.encode())
        hash_str = hash_obj.hexdigest()[:8]
        
        # Format: EA-YYYY-MM-DD-HASH
        date_parts = date.split('-') if '-' in date else [date, '01', '01']
        return f"EA-{date_parts[0]}-{date_parts[1] if len(date_parts) > 1 else '01'}-{date_parts[2] if len(date_parts) > 2 else '01'}-{hash_str}"
    
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
    
    def scrape_enforcement_action(self, metadata: Dict) -> Dict:
        """Scrape a single enforcement action."""
        print(f"\nScraping: {metadata['headline'][:60]}...")
        print(f"  Type: {metadata['type']}")
        
        try:
            # If it's a PDF letter, extract directly
            if metadata['type'] == 'Letter (PDF)' or metadata['url'].endswith('.pdf'):
                return self._scrape_pdf_letter(metadata)
            else:
                # It's a press release page
                return self._scrape_press_release(metadata)
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None
    
    def _scrape_pdf_letter(self, metadata: Dict) -> Dict:
        """Scrape a direct PDF letter."""
        pdf_data = self._extract_pdf(metadata['url'])
        
        if pdf_data:
            print(f"  ✓ PDF extracted: {len(pdf_data['extracted_text'])} characters")
            
            return {
                'id': metadata['id'],
                'url': metadata['url'],
                'headline': metadata['headline'],
                'theme': metadata['theme'],
                'type': metadata['type'],
                'published_date': metadata['published_date'],
                'scraped_date': self.scraped_date,
                'action_category': metadata.get('action_category', ''),
                'attachments': {
                    'pdfs': [pdf_data],
                    'excels': [],
                    'csvs': [],
                }
            }
        return None
    
    def _scrape_press_release(self, metadata: Dict) -> Dict:
        """Scrape a press release page."""
        response = self.session.get(metadata['url'], timeout=30)
        response.raise_for_status()
        time.sleep(1)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract main content
        main_text = self._extract_main_text(soup)
        if self.debug:
            print(f"  Main text: {len(main_text)} characters")
        
        # Extract linked pages
        linked_pages = self._extract_linked_pages(soup, metadata['url'])
        print(f"  ✓ Linked pages: {len(linked_pages)}")
        
        # Extract attachments
        attachments = self._extract_attachments(soup, metadata['url'])
        total_att = len(attachments['pdfs']) + len(attachments['excels']) + len(attachments['csvs'])
        print(f"  ✓ Attachments: {total_att} files")
        
        return {
            'id': metadata['id'],
            'url': metadata['url'],
            'headline': metadata['headline'],
            'theme': metadata['theme'],
            'type': metadata['type'],
            'published_date': metadata['published_date'],
            'scraped_date': self.scraped_date,
            'action_category': metadata.get('action_category', ''),
            'content': {
                'main_page_text': main_text,
                'linked_pages': linked_pages,
            },
            'attachments': attachments,
        }
    
    def _extract_main_text(self, soup: BeautifulSoup) -> str:
        """Extract main text content from page."""
        text_parts = []
        
        # Strategy 1: Look for #article div
        article = soup.select_one('#article')
        
        if article:
            article_copy = BeautifulSoup(str(article), 'html.parser')
            
            # Remove unwanted elements
            for tag in article_copy.select('script, style, nav, .breadcrumb, .share, .panel-related, .panel-attachments'):
                tag.decompose()
            
            text = article_copy.get_text(separator='\n', strip=True)
            return self._clean_text(text)
        
        # Strategy 2: Look for main content container
        content_div = soup.select_one('#content[role="main"]')
        
        if content_div:
            content_copy = BeautifulSoup(str(content_div), 'html.parser')
            
            for tag in content_copy.select('script, style, nav, .breadcrumb, .page-header'):
                tag.decompose()
            
            text = content_copy.get_text(separator='\n', strip=True)
            return self._clean_text(text)
        
        return ""
    
    def _extract_linked_pages(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract text from linked pages, excluding social media and navigation."""
        linked_pages = []
        seen_urls = set()
        
        # Social media domains to exclude
        excluded_domains = [
            'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
            'youtube.com', 'flickr.com', 'threads.net', 'bsky.app', 'x.com'
        ]
        
        # Navigation and generic pages to exclude (path patterns)
        excluded_paths = [
            '/default.htm',
            '/feeds/',
            '/subscribe.htm',
            '/recentpostings.htm',
            '/newsevents/calendar.htm',
            '/publications.htm',
            '/sitemap.htm',
            '/azindex.htm',
            '/careers.htm',
            '/faqs.htm',
            '/videos.htm',
            '/aboutthefed/contact-us',
            '/aboutthefed.htm',
            '/aboutthefed/the-fed-explained.htm',
        ]
        
        # Only look for links within the main article content area
        article = soup.select_one('#article')
        if not article:
            # Fallback to main content if article div not found
            article = soup.select_one('#content[role="main"]')
        
        if not article:
            return linked_pages
        
        # Find all links within the article content
        all_links = article.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '').strip()
            if not href:
                continue
            
            full_url = urljoin(BASE_URL, href)
            
            # Skip external/social media links
            if any(domain in full_url for domain in excluded_domains):
                continue
            
            # Must be Federal Reserve page
            if not full_url.startswith(BASE_URL):
                continue
            
            # Skip main page itself
            if full_url == base_url:
                continue
            
            # Skip excluded navigation/generic pages
            if any(excluded_path in full_url for excluded_path in excluded_paths):
                continue
            
            # Must end with .htm
            if not full_url.endswith('.htm'):
                continue
            
            # Skip duplicates
            if full_url in seen_urls:
                continue
            
            # Additional filtering: only include links that appear to be related content
            # Check if the link is in a paragraph or list item (likely related content)
            # rather than navigation elements
            parent = link.parent
            if parent and parent.name in ['p', 'li', 'td', 'div']:
                # Check if parent is not a navigation element
                parent_classes = parent.get('class', [])
                parent_id = parent.get('id', '')
                
                # Skip if parent is clearly navigation
                nav_indicators = ['nav', 'menu', 'breadcrumb', 'footer', 'header', 'sidebar']
                if any(indicator in str(parent_classes).lower() or indicator in parent_id.lower() 
                       for indicator in nav_indicators):
                    continue
                
                seen_urls.add(full_url)
                page_data = self._fetch_linked_page(full_url)
                if page_data:
                    linked_pages.append(page_data)
        
        return linked_pages
    
    def _fetch_linked_page(self, url: str) -> Optional[Dict]:
        """Fetch and extract text from a linked page."""
        try:
            if self.debug:
                print(f"    → Fetching: {url}")
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            time.sleep(1)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = self._extract_main_text(soup)
            
            if page_text and len(page_text) > 50:
                if self.debug:
                    print(f"      ✓ {len(page_text)} characters")
                return {
                    'url': url,
                    'text': page_text,
                }
        except Exception as e:
            if self.debug:
                print(f"      ✗ Error: {e}")
        
        return None
    
    def _extract_attachments(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Extract and process all attachments."""
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
        """Extract text from PDF including tables and OCR."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            file_name = url.split('/')[-1]
            
            extracted_text = []
            
            # Try pdfplumber
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
            
            # OCR attempt
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
        """Format table as text."""
        if not table:
            return ""
        
        lines = []
        for row in table:
            line = ' | '.join([str(cell) if cell else '' for cell in row])
            lines.append(line)
        
        return '\n'.join(lines)
    
    def _clean_text(self, text: str) -> str:
        """Clean text by removing whitespace and boilerplate."""
        boilerplate_phrases = [
            r'Skip to main content',
            r'An official website of the United States Government',
            r"Here's how you know",
            r'Official websites use \.gov',
            r'Secure \.gov websites use HTTPS',
            r'Board of Governors of the Federal Reserve System',
            r'Stay Connected',
            r'Federal Reserve .* Page',
            r'Subscribe to RSS',
            r'Subscribe to Email',
            r'Last Update:.*\d{4}',
        ]
        
        for phrase in boilerplate_phrases:
            text = re.sub(phrase, '', text, flags=re.IGNORECASE)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Split and clean lines
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line and len(line) > 1]
        
        # Remove duplicates
        cleaned_lines = []
        prev_line = None
        for line in lines:
            if line != prev_line:
                cleaned_lines.append(line)
                prev_line = line
        
        return '\n'.join(cleaned_lines)
    
    def save_results(self, new_actions: List[Dict]):
        """Save scraped data to JSON file."""
        all_data = self.existing_data + new_actions
        all_data.sort(key=lambda x: x.get('published_date', ''), reverse=True)
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print(f"Saved {len(all_data)} total actions to {OUTPUT_FILE}")
        print(f"Added {len(new_actions)} new actions in this run")
        print(f"{'='*60}")
    
    def run(self):
        """Main execution method."""
        print("=" * 60)
        print("Federal Reserve Enforcement Actions Scraper")
        print("=" * 60)
        
        # Scrape index pages
        actions_metadata = self.scrape_index_pages()
        
        if not actions_metadata:
            print("\nNo new enforcement actions found")
            return
        
        print(f"\n{'='*60}")
        print(f"Found {len(actions_metadata)} new enforcement actions")
        print(f"{'='*60}")
        
        # Scrape each action
        scraped_actions = []
        for i, metadata in enumerate(actions_metadata, 1):
            print(f"\n[{i}/{len(actions_metadata)}]", end=' ')
            action_data = self.scrape_enforcement_action(metadata)
            
            if action_data:
                scraped_actions.append(action_data)
            
            time.sleep(2)  # Rate limiting
        
        # Save results
        if scraped_actions:
            self.save_results(scraped_actions)
        
        print("\n" + "=" * 60)
        print("Scraping completed successfully!")
        print("=" * 60)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Scrape Federal Reserve Enforcement Actions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 enforcement_actions_scrape.py                # Scrape all pages
  python3 enforcement_actions_scrape.py --max-pages 5  # First 5 pages
  python3 enforcement_actions_scrape.py --debug        # Verbose output
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
    
    scraper = EnforcementActionsScraper(max_pages=args.max_pages, debug=args.debug)
    scraper.run()


if __name__ == "__main__":
    main()