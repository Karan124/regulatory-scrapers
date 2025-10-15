"""
MAS Enforcement Actions Scraper - Production Grade
===================================================
Scrapes enforcement actions from the Monetary Authority of Singapore.
Designed for LLM consumption with full text extraction and deduplication.


Installation:
-------------
pip install playwright beautifulsoup4 requests pandas openpyxl pdfplumber PyPDF2
playwright install chromium

Usage:
------
python mas_enforcement_scrape.py

Author: Production-Grade Scraper
Version: 1.0
Date: 2025-10-15
"""

import json
import logging
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
from io import BytesIO, StringIO
import re

# Core dependencies
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
import requests
import pandas as pd

# PDF processing libraries
try:
    import pdfplumber
    PDF_PDFPLUMBER = True
except ImportError:
    PDF_PDFPLUMBER = False

try:
    import PyPDF2
    PDF_PYPDF2 = True
except ImportError:
    PDF_PYPDF2 = False


# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_PAGES = 1  # Number of pages to scrape (1, 2, 3... N)
BASE_URL = "https://www.mas.gov.sg"
ENFORCEMENT_URL = f"{BASE_URL}/regulation/enforcement/enforcement-actions"
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "mas_enforcement_actions.json"

# Create output directory
DATA_DIR.mkdir(exist_ok=True)

# Configure logging (console only, no file)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# ============================================================================
# MAIN SCRAPER CLASS
# ============================================================================

class MASEnforcementScraper:
    """
    Production-grade MAS Enforcement Actions scraper with:
    - Playwright for JavaScript-rendered pages
    - Anti-bot protection (stealth mode, random delays)
    - Deduplication (URL-based)
    - In-memory attachment processing (no file saving)
    - Incremental saves
    - Table-based extraction from index page
    """
    
    def __init__(self):
        self.scraped_urls: Set[str] = set()
        self.existing_data: List[Dict] = []
        self.playwright = None
        self.browser = None
        self.context = None
        self.load_existing_data()
    
    # ========================================================================
    # DATA PERSISTENCE
    # ========================================================================
    
    def load_existing_data(self):
        """Load existing scraped enforcement actions for deduplication"""
        if OUTPUT_FILE.exists():
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    self.existing_data = json.load(f)
                    # Build set of scraped URLs
                    self.scraped_urls = {
                        item.get('url') for item in self.existing_data 
                        if item.get('url')
                    }
                logger.info(f"✓ Loaded {len(self.existing_data)} existing enforcement actions")
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                self.existing_data = []
        else:
            logger.info("No existing data found. Starting fresh.")
    
    def save_data(self):
        """Save all scraped data to JSON file"""
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.existing_data, f, ensure_ascii=False, indent=2)
            logger.info(f"✓ Saved {len(self.existing_data)} enforcement actions to {OUTPUT_FILE}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    # ========================================================================
    # BROWSER MANAGEMENT (PLAYWRIGHT WITH STEALTH)
    # ========================================================================
    
    def init_browser(self):
        """Initialize Playwright browser with anti-bot stealth settings"""
        logger.info("Initializing Playwright browser...")
        self.playwright = sync_playwright().start()
        
        # Launch Chromium with stealth flags
        self.browser = self.playwright.chromium.launch(
            headless=True,  # Set to False for debugging
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        )
        
        # Create context with realistic browser fingerprint
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='Asia/Singapore',
        )
        
        # Inject stealth JavaScript to hide automation
        self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)
        
        logger.info("✓ Browser initialized")
    
    def close_browser(self):
        """Cleanup browser resources"""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("✓ Browser closed")
    
    # ========================================================================
    # PAGE FETCHING
    # ========================================================================
    
    def random_delay(self, min_sec: float = 2.0, max_sec: float = 5.0):
        """Random delay to mimic human behavior"""
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)
    
    def fetch_page_with_playwright(self, url: str, wait_for_selectors: List[str] = None) -> Optional[str]:
        """
        Fetch page content using Playwright.
        Waits for JavaScript to render, tries multiple selectors.
        Returns HTML as string.
        """
        page = None
        try:
            page = self.context.new_page()
            
            logger.debug(f"Loading: {url}")
            page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Wait for dynamic content with multiple selector options
            if wait_for_selectors:
                selector_found = False
                for selector in wait_for_selectors:
                    try:
                        page.wait_for_selector(selector, timeout=10000)
                        logger.debug(f"✓ Found: {selector}")
                        selector_found = True
                        break
                    except PlaywrightTimeout:
                        continue
                
                if not selector_found:
                    logger.debug("No selectors matched, proceeding anyway")
            
            # Extra wait for remaining JS
            time.sleep(2)
            
            # Get page HTML
            content = page.content()
            page.close()
            
            self.random_delay(1.5, 3.0)
            return content
            
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            if page:
                page.close()
            return None
    
    # ========================================================================
    # LISTING PAGE EXTRACTION (TABLE-BASED)
    # ========================================================================
    
    def extract_enforcement_actions_from_table(self, html_content: str) -> List[Dict[str, str]]:
        """
        Extract enforcement action metadata from table rows.
        Returns list of dicts with: url, issue_date, person_company, action_type, title
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        actions = []
        
        # Find the enforcement actions table
        table = soup.find('table')
        if not table:
            logger.error("No table found on page")
            return actions
        
        tbody = table.find('tbody')
        if not tbody:
            logger.error("No tbody found in table")
            return actions
        
        # Extract each row
        rows = tbody.find_all('tr')
        logger.info(f"Found {len(rows)} table rows")
        
        for row in rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue
                
                # Cell 0: Issue Date
                issue_date = cells[0].get_text(strip=True)
                
                # Cell 1: Person/Company
                person_company = cells[1].get_text(strip=True)
                
                # Cell 2: Action Type
                action_type = cells[2].get_text(strip=True)
                
                # Cell 3: Title and URL
                link = cells[3].find('a')
                if not link or not link.get('href'):
                    continue
                
                url = urljoin(BASE_URL, link['href'])
                
                # Skip if already scraped (deduplication)
                if url in self.scraped_urls:
                    logger.debug(f"⊗ Skip (already scraped): {url}")
                    continue
                
                title = link.get_text(strip=True)
                
                actions.append({
                    'url': url,
                    'issue_date': issue_date,
                    'person_company': person_company,
                    'action_type': action_type,
                    'title': title
                })
                
            except Exception as e:
                logger.error(f"Error extracting row data: {e}")
                continue
        
        return actions
    
    def scrape_enforcement_listing(self, page_num: int = 1) -> List[Dict]:
        """Scrape enforcement actions from a single listing page"""
        # Note: The enforcement actions page uses JavaScript for pagination
        # We'll need to check the actual URL pattern when pagination is clicked
        # For now, assuming it follows similar pattern to news/publications
        url = ENFORCEMENT_URL if page_num == 1 else f"{ENFORCEMENT_URL}?page={page_num}"
        
        logger.info(f"📄 Scraping listing page {page_num}: {url}")
        
        # Wait for table to load
        html = self.fetch_page_with_playwright(
            url,
            wait_for_selectors=['table', '#MasXbeEnforcementActionListingTable']
        )
        
        if not html:
            logger.error(f"Failed to fetch page {page_num}")
            return []
        
        actions = self.extract_enforcement_actions_from_table(html)
        logger.info(f"✓ Found {len(actions)} new enforcement actions on page {page_num}")
        
        return actions
    
    # ========================================================================
    # TEXT CLEANING
    # ========================================================================
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text for LLM consumption"""
        if not text:
            return ""
        
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove page numbers and footers
        text = re.sub(r'Page \d+ of \d+', '', text)
        text = re.sub(r'\d+\s*\|\s*Page', '', text)
        
        # Reduce multiple newlines
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        return text.strip()
    
    # ========================================================================
    # ATTACHMENT PROCESSING (IN-MEMORY ONLY - NO FILE SAVING)
    # ========================================================================
    
    def extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes (no file saving)"""
        text_parts = []
        
        # Method 1: pdfplumber (best for tables)
        if PDF_PDFPLUMBER:
            try:
                with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            text_parts.append(text)
            except Exception as e:
                logger.debug(f"pdfplumber failed: {e}")
        
        # Method 2: PyPDF2 (fallback)
        if not text_parts and PDF_PYPDF2:
            try:
                reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        text_parts.append(text)
            except Exception as e:
                logger.debug(f"PyPDF2 failed: {e}")
        
        return self.clean_text("\n\n".join(text_parts))
    
    def download_and_extract_attachment(self, url: str) -> Dict[str, str]:
        """
        Download and extract text from attachment.
        CRITICAL: All processing in memory - NO file saving to disk.
        """
        result = {
            'url': url,
            'filename': Path(urlparse(url).path).name,
            'text': ""
        }
        
        try:
            # Download to memory
            time.sleep(random.uniform(0.5, 1.5))
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"Download failed {url}: HTTP {response.status_code}")
                return result
            
            # Get extension
            ext = Path(result['filename']).suffix.lower()
            
            # Process by type - ALL IN MEMORY
            if ext == '.pdf':
                result['text'] = self.extract_pdf_text(response.content)
            
            elif ext in ['.xlsx', '.xls']:
                try:
                    df_dict = pd.read_excel(BytesIO(response.content), sheet_name=None)
                    parts = []
                    for sheet_name, df in df_dict.items():
                        parts.append(f"Sheet: {sheet_name}")
                        parts.append(df.to_string())
                    result['text'] = self.clean_text("\n\n".join(parts))
                except Exception as e:
                    logger.error(f"Excel processing failed: {e}")
            
            elif ext == '.csv':
                try:
                    csv_text = response.content.decode('utf-8', errors='ignore')
                    df = pd.read_csv(StringIO(csv_text))
                    result['text'] = self.clean_text(df.to_string())
                except Exception as e:
                    logger.error(f"CSV processing failed: {e}")
            
            elif ext in ['.txt', '.doc', '.docx']:
                try:
                    result['text'] = self.clean_text(
                        response.content.decode('utf-8', errors='ignore')
                    )
                except Exception as e:
                    logger.error(f"Text extraction failed: {e}")
            
            if result['text']:
                logger.info(f"✓ Extracted {len(result['text'])} chars from {result['filename']}")
            
        except Exception as e:
            logger.error(f"Attachment processing error {url}: {e}")
        
        return result
    
    # ========================================================================
    # ENFORCEMENT ACTION CONTENT EXTRACTION
    # ========================================================================
    
    def extract_enforcement_content(self, url: str, html: str, action_meta: Dict) -> Dict:
        """
        Extract complete enforcement action content from HTML.
        Uses same structure as news/publications pages.
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'url': url,
            'scraped_date': datetime.now().isoformat(),
            'issue_date': action_meta.get('issue_date', ''),
            'person_company': action_meta.get('person_company', ''),
            'action_type': action_meta.get('action_type', ''),
            'title': "",
            'published_date': "",
            'main_text': "",
            'footnotes': [],
            'attachments': [],
            'internal_links': [],
            'related_links': []
        }
        
        try:
            # ============================================================
            # 1. TITLE
            # ============================================================
            title_elem = soup.find('h1', class_='mas-text-h1')
            if title_elem:
                data['title'] = title_elem.get_text(strip=True)
            
            # ============================================================
            # 2. PUBLISHED DATE (from mas-ancillaries)
            # ============================================================
            ancillaries = soup.find('div', class_='mas-ancillaries')
            if ancillaries:
                for span in ancillaries.find_all('span'):
                    text = span.get_text(strip=True)
                    if 'Published Date:' in text:
                        data['published_date'] = text.replace('Published Date:', '').strip()
                        break
            
            # ============================================================
            # 3. MAIN CONTENT TEXT
            # ============================================================
            # Look for <div class="_mas-typeset ...">
            content_div = soup.find('div', class_='_mas-typeset')
            if not content_div:
                # Fallback to mas-rte-content
                content_div = soup.find('div', class_='mas-rte-content')
            
            if content_div:
                # Remove unwanted elements
                for unwanted in content_div(['script', 'style', 'nav', 'header', 'footer']):
                    unwanted.decompose()
                
                # Extract text from semantic elements
                text_parts = []
                for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'td', 'th']):
                    text = elem.get_text(strip=True)
                    if text and len(text) > 10:  # Filter noise
                        text_parts.append(text)
                
                data['main_text'] = self.clean_text("\n\n".join(text_parts))
                
                # ========================================================
                # 4. INTERNAL LINKS (from content only)
                # ========================================================
                content_links = []
                for link in content_div.find_all('a', href=True):
                    href = link['href']
                    full_url = urljoin(BASE_URL, href)
                    
                    # Only internal MAS links, exclude files
                    if full_url.startswith(BASE_URL):
                        is_file = any(
                            ext in href.lower() 
                            for ext in ['.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx']
                        )
                        if not is_file:
                            content_links.append(full_url)
                
                data['internal_links'] = list(set(content_links))
                
                # ========================================================
                # 5. INLINE ATTACHMENTS
                # ========================================================
                for link in content_div.find_all('a', href=True):
                    href = link['href']
                    if any(ext in href.lower() for ext in ['.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx']):
                        full_url = urljoin(BASE_URL, href)
                        # Avoid duplicates
                        if not any(att['url'] == full_url for att in data['attachments']):
                            logger.info(f"📎 Inline attachment: {full_url}")
                            att = self.download_and_extract_attachment(full_url)
                            if att.get('text'):
                                data['attachments'].append(att)
            else:
                logger.warning(f"⚠ No content div found for {url}")
            
            # ============================================================
            # 6. FOOTNOTES (from shadow DOM)
            # ============================================================
            footnotes = []
            
            # Method 1: Look for rendered footnotes in the DOM
            for ol in soup.find_all('ol', id='footnote-list'):
                for li in ol.find_all('li'):
                    span = li.find('span', class_='footnote-item-content')
                    if span:
                        text = span.get_text(strip=True)
                        if text and text not in footnotes:
                            footnotes.append(text)
            
            # Method 2: Try parsing shadow DOM template if Method 1 fails
            if not footnotes:
                for footnote_group in soup.find_all('mas-footnote-group'):
                    template = footnote_group.find('template')
                    if template:
                        # Get all content inside template tag
                        template_content = ''.join(str(child) for child in template.children)
                        shadow_soup = BeautifulSoup(template_content, 'html.parser')
                        
                        footnote_list = shadow_soup.find('ol', id='footnote-list')
                        if footnote_list:
                            for li in footnote_list.find_all('li'):
                                span = li.find('span', class_='footnote-item-content')
                                if span:
                                    text = span.get_text(strip=True)
                                    if text and text not in footnotes:
                                        footnotes.append(text)
            
            # Method 3: Fallback - look for text after "***" pattern
            if not footnotes:
                for p in soup.find_all('p'):
                    if '***' in p.get_text():
                        next_elem = p.find_next_sibling()
                        while next_elem:
                            if next_elem.name == 'mas-footnote-group':
                                text = next_elem.get_text(strip=True)
                                text = re.sub(r'^\[\d+\]\s*', '', text)
                                if text and len(text) > 10:
                                    footnotes.append(text)
                                break
                            next_elem = next_elem.find_next_sibling()
            
            if footnotes:
                data['footnotes'] = footnotes
                logger.info(f"✓ Extracted {len(footnotes)} footnotes")
            
            # ============================================================
            # 7. RESOURCES SECTION & ADDITIONAL DOCUMENTS ATTACHMENTS
            # ============================================================
            # Look for all attachment sections
            attachment_sections = []
            
            # Pattern 1: "Resources" section
            for section in soup.find_all('div', class_='mas-section'):
                header = section.find('h2', class_='mas-section__title')
                if header and ('resource' in header.get_text().lower() or 'document' in header.get_text().lower()):
                    attachment_sections.append(section)
            
            # Pattern 2: Sections with _mas-typeset containing links
            for section in soup.find_all('div', class_='_mas-typeset'):
                # Check if it has a header about documents
                header = section.find('h2', class_='mas-section__title')
                if header and 'document' in header.get_text().lower():
                    attachment_sections.append(section)
            
            # Extract attachments from all found sections
            for section in attachment_sections:
                for link in section.find_all('a', href=True):
                    href = link.get('href')
                    if href and any(ext in href.lower() for ext in ['.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx']):
                        full_url = urljoin(BASE_URL, href)
                        if not any(att['url'] == full_url for att in data['attachments']):
                            logger.info(f"📎 Section attachment: {full_url}")
                            att = self.download_and_extract_attachment(full_url)
                            if att.get('text'):
                                data['attachments'].append(att)
            
            # ============================================================
            # 8. RELATED LINKS
            # ============================================================
            related = []
            
            # Check for "Related:" paragraphs
            for elem in soup.find_all(['p', 'div']):
                if elem.get_text(strip=True).lower().startswith('related:'):
                    for link in elem.find_all('a', href=True):
                        full_url = urljoin(BASE_URL, link['href'])
                        if full_url.startswith(BASE_URL):
                            related.append({
                                'url': full_url,
                                'text': link.get_text(strip=True)
                            })
            
            # Check "Related News/Actions" section
            related_section = soup.find('div', id='related-news-listing')
            if related_section:
                for card in related_section.find_all('article', class_='mas-search-card'):
                    link = card.find('a', class_='mas-link')
                    if link and link.get('href'):
                        full_url = urljoin(BASE_URL, link['href'])
                        title_elem = link.find('span', class_='mas-link__text')
                        title = title_elem.get_text(strip=True) if title_elem else link.get_text(strip=True)
                        related.append({'url': full_url, 'text': title})
            
            if related:
                data['related_links'] = related
            
            # ============================================================
            # LOG SUMMARY
            # ============================================================
            logger.info(
                f"✓ Title: {bool(data['title'])}, "
                f"Text: {len(data['main_text'])} chars, "
                f"Footnotes: {len(data['footnotes'])}, "
                f"Attachments: {len(data['attachments'])}, "
                f"Links: {len(data['internal_links'])}, "
                f"Related: {len(data['related_links'])}"
            )
            
        except Exception as e:
            logger.error(f"Extraction error for {url}: {e}", exc_info=True)
        
        return data
    
    def scrape_enforcement_action(self, action_meta: Dict) -> Optional[Dict]:
        """Scrape full content from a single enforcement action page"""
        url = action_meta['url']
        logger.info(f"⚖️  Scraping: {url}")
        
        # Wait for article structure
        selectors = ['._mas-typeset', '.mas-layout__main--banner', '.mas-text-h1']
        html = self.fetch_page_with_playwright(url, wait_for_selectors=selectors)
        
        if not html:
            logger.error(f"Failed to fetch enforcement action: {url}")
            return None
        
        data = self.extract_enforcement_content(url, html, action_meta)
        
        # Merge metadata from listing if not found in page
        if not data['title']:
            data['title'] = action_meta.get('title', '')
        
        return data
    
    # ========================================================================
    # MAIN WORKFLOW
    # ========================================================================
    
    def run(self):
        """
        Main scraping workflow:
        1. Init browser
        2. Loop through pages (1 to MAX_PAGES)
        3. Extract enforcement action rows from table
        4. Scrape each enforcement action
        5. Save incrementally
        6. Close browser
        """
        logger.info("=" * 70)
        logger.info("🚀 MAS ENFORCEMENT ACTIONS SCRAPER - STARTING")
        logger.info("=" * 70)
        logger.info(f"Max pages: {MAX_PAGES}")
        logger.info(f"Output: {OUTPUT_FILE}")
        logger.info(f"Existing actions: {len(self.existing_data)}")
        logger.info("=" * 70)
        
        try:
            self.init_browser()
            new_count = 0
            
            # Loop through pages
            for page_num in range(1, MAX_PAGES + 1):
                action_links = self.scrape_enforcement_listing(page_num)
                
                if not action_links:
                    logger.warning(f"No new enforcement actions on page {page_num}, stopping")
                    break
                
                # Scrape each enforcement action
                for idx, meta in enumerate(action_links, 1):
                    try:
                        logger.info(f"[Page {page_num}, Action {idx}/{len(action_links)}]")
                        data = self.scrape_enforcement_action(meta)
                        
                        if data:
                            self.existing_data.append(data)
                            self.scraped_urls.add(data['url'])
                            new_count += 1
                            
                            # Save every 5 actions
                            if new_count % 5 == 0:
                                self.save_data()
                                logger.info(f"💾 Checkpoint: {new_count} actions")
                        
                    except Exception as e:
                        logger.error(f"Error scraping {meta.get('url')}: {e}")
                        continue
            
            # Final save
            self.save_data()
            
            logger.info("=" * 70)
            logger.info("✅ SCRAPING COMPLETED")
            logger.info(f"New enforcement actions: {new_count}")
            logger.info(f"Total enforcement actions: {len(self.existing_data)}")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"❌ Fatal error: {e}", exc_info=True)
            raise
        finally:
            self.close_browser()


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Entry point"""
    scraper = MASEnforcementScraper()
    scraper.run()


if __name__ == "__main__":
    main()