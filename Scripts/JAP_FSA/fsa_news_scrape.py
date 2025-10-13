"""
Japan Financial Services Agency (FSA) News & Publications Scraper
Production-grade script for scraping FSA news, publications, and related content

Required dependencies:
    pip install playwright beautifulsoup4 PyPDF2 pdfplumber pytesseract pdf2image pandas openpyxl aiohttp lxml
    playwright install chromium

For OCR (required for chart extraction):
    - Ubuntu/Debian: sudo apt-get install tesseract-ocr
    - macOS: brew install tesseract
    - Windows: Download from https://github.com/UB-Mannheim/tesseract/wiki
    
For image processing (required for pdf2image):
    - Ubuntu/Debian: sudo apt-get install poppler-utils
    - macOS: brew install poppler
    - Windows: Download poppler binaries and add to PATH
"""

import asyncio
import json
import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import random

import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser
import PyPDF2
import pytesseract
from pdf2image import convert_from_bytes
import io
import pandas as pd

# Configuration
START_DATE = datetime(2025, 9, 1)  # Configurable start date
BASE_URL = "https://www.fsa.go.jp"
INDEX_URL = f"{BASE_URL}/en/recent.html"
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "fsa_news.json"
LOG_FILE = OUTPUT_DIR / "fsa_scraper.log"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed output
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FSAScraper:
    """Main scraper class for FSA news and publications"""
    
    def __init__(self, start_date: datetime = START_DATE):
        self.start_date = start_date
        self.session = None
        self.browser = None
        self.context = None
        self.scraped_urls: Set[str] = set()
        self.existing_data: List[Dict] = []
        self.pdf_hashes: Set[str] = set()
        
        # Browser headers for stealth
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,ja;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
    
    async def initialize(self):
        """Initialize browser and load existing data"""
        logger.info("Initializing FSA scraper...")
        
        # Load existing data for deduplication
        if OUTPUT_FILE.exists():
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    self.existing_data = json.load(f)
                    self.scraped_urls = {item['url'] for item in self.existing_data}
                    logger.info(f"Loaded {len(self.existing_data)} existing records")
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                self.existing_data = []
        
        # Initialize Playwright browser with stealth
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        )
        
        self.context = await self.browser.new_context(
            user_agent=self.headers['User-Agent'],
            viewport={'width': 1920, 'height': 1080},
            extra_http_headers=self.headers
        )
        
        # Additional stealth measures
        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'ja']});
        """)
        
        logger.info("Browser initialized successfully")
    
    async def random_delay(self, min_seconds: float = 1.0, max_seconds: float = 3.0):
        """Random delay to avoid detection"""
        delay = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(delay)
    
    async def fetch_index_page(self) -> List[Dict]:
        """Fetch and parse the index page for all publications"""
        logger.info(f"Fetching index page: {INDEX_URL}")
        
        page = await self.context.new_page()
        
        try:
            # Visit homepage first to establish session
            await page.goto(BASE_URL, wait_until='networkidle', timeout=30000)
            await self.random_delay(1, 2)
            
            # Now visit the index page
            await page.goto(INDEX_URL, wait_until='networkidle', timeout=30000)
            await self.random_delay(2, 4)
            
            # Aggressive scrolling to trigger lazy loading
            logger.info("Scrolling to load lazy content...")
            previous_height = 0
            scroll_attempts = 0
            max_scroll_attempts = 15
            
            while scroll_attempts < max_scroll_attempts:
                # Get current scroll height
                current_height = await page.evaluate("document.body.scrollHeight")
                
                if current_height == previous_height:
                    scroll_attempts += 1
                else:
                    scroll_attempts = 0
                
                # Scroll down in chunks
                await page.evaluate(f"window.scrollTo(0, {current_height})")
                await asyncio.sleep(1.5)
                
                # Scroll up and down to trigger observers
                await page.evaluate(f"window.scrollTo(0, {current_height - 500})")
                await asyncio.sleep(0.5)
                await page.evaluate(f"window.scrollTo(0, {current_height})")
                await asyncio.sleep(1)
                
                previous_height = current_height
                
                # Check if we've reached our target date
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                if self._has_reached_target_date(soup):
                    logger.info("Reached target start date, stopping scroll")
                    break
            
            logger.info(f"Completed scrolling after checking {scroll_attempts} times")
            
            # Final content extraction
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            items = self._parse_index_page(soup)
            
            # Log date range found
            if items:
                dates = [item['published_date'] for item in items]
                logger.info(f"Found {len(items)} items from {min(dates)} to {max(dates)}")
            else:
                logger.warning("No items found on index page")
            
            return items
            
        except Exception as e:
            logger.error(f"Error fetching index page: {e}")
            return []
        finally:
            await page.close()
    
    def _has_reached_target_date(self, soup: BeautifulSoup) -> bool:
        """Check if we've scrolled to items before our start date"""
        main_div = soup.find('div', id='main')
        if not main_div:
            return False
        
        # Find all month headers (h2 tags)
        month_headers = main_div.find_all('h2')
        if not month_headers:
            return False
        
        # Check the last month header
        last_month_text = month_headers[-1].get_text(strip=True)
        try:
            # Parse month like "July 2025"
            last_date = datetime.strptime(last_month_text, '%B %Y')
            return last_date < self.start_date
        except ValueError:
            # Try alternative format like "July 2025" without nbsp
            try:
                last_month_text = last_month_text.replace('\xa0', ' ')
                last_date = datetime.strptime(last_month_text, '%B %Y')
                return last_date < self.start_date
            except ValueError:
                return False
    
    def _parse_index_page(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse the index page HTML to extract publication metadata"""
        items = []
        main_div = soup.find('div', id='main')
        
        if not main_div:
            logger.warning("Could not find main div on index page")
            return items
        
        # Find the inner div which contains the actual content
        inner_div = main_div.find('div', class_='inner')
        if not inner_div:
            inner_div = main_div
        
        current_month = None
        current_day = None
        
        # Iterate through all h2 (months), h3 (days), and ul (items) elements
        for element in inner_div.find_all(['h2', 'h3', 'ul']):
            if element.name == 'h2':
                # Month header like "October 2025" or "October 2025"
                month_text = element.get_text(strip=True).replace('\xa0', ' ').replace('\u3000', ' ')
                try:
                    current_month = datetime.strptime(month_text, '%B %Y')
                    logger.debug(f"Parsed month: {current_month.strftime('%B %Y')}")
                except ValueError:
                    # Try alternative parsing
                    try:
                        # Sometimes there might be extra spaces or formatting
                        month_text = ' '.join(month_text.split())
                        current_month = datetime.strptime(month_text, '%B %Y')
                        logger.debug(f"Parsed month (alt): {current_month.strftime('%B %Y')}")
                    except ValueError:
                        logger.warning(f"Could not parse month: {month_text}")
                        continue
            
            elif element.name == 'h3':
                # Day header like "October 10" or "October ９" (Japanese numerals possible)
                day_text = element.get_text(strip=True).replace('\xa0', ' ').replace('\u3000', ' ')
                
                # Extract just the numeric part (handle both Arabic and potential Japanese numerals)
                # Japanese full-width numbers: ０-９
                day_text_normalized = day_text
                # Convert Japanese full-width numbers to ASCII
                for i in range(10):
                    day_text_normalized = day_text_normalized.replace(chr(0xFF10 + i), str(i))
                
                try:
                    if current_month:
                        # Extract all digits
                        day_num = ''.join(filter(str.isdigit, day_text_normalized))
                        if day_num:
                            day_value = int(day_num)
                            current_day = current_month.replace(day=day_value)
                            logger.debug(f"Parsed day: {current_day.strftime('%Y-%m-%d')}")
                        else:
                            logger.warning(f"No digits found in day text: {day_text}")
                            continue
                    else:
                        logger.warning(f"No current month set when parsing day: {day_text}")
                        continue
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse day: {day_text} (normalized: {day_text_normalized}) - {e}")
                    continue
            
            elif element.name == 'ul' and current_day:
                # Skip if date is before our start date
                if current_day < self.start_date:
                    logger.debug(f"Skipping date {current_day.strftime('%Y-%m-%d')} (before start date)")
                    continue
                
                # Extract items from list
                for li in element.find_all('li', recursive=False):
                    item = self._extract_item_details(li, current_day)
                    if item:
                        items.append(item)
                        logger.debug(f"Added item: {item['headline'][:50]}... on {item['published_date']}")
        
        return items
    
    def _extract_item_details(self, li_element, pub_date: datetime) -> Optional[Dict]:
        """Extract details from a single publication item"""
        try:
            link_elem = li_element.find('a', href=True)
            
            if not link_elem:
                return None
            
            url = urljoin(BASE_URL, link_elem.get('href', ''))
            title = link_elem.get_text(strip=True)
            
            # Determine type based on title or context
            item_type = self._determine_type(title)
            
            return {
                'url': url,
                'headline': title,
                'type': item_type,
                'published_date': pub_date.strftime('%Y-%m-%d'),
            }
            
        except Exception as e:
            logger.error(f"Error extracting item details: {e}")
            return None
    
    def _determine_type(self, title: str) -> str:
        """Determine publication type from title"""
        title_lower = title.lower()
        
        if 'press conference' in title_lower:
            return 'Press Conference'
        elif 'speech' in title_lower or 'remarks' in title_lower:
            return 'Speech'
        elif 'publication' in title_lower or 'report' in title_lower:
            return 'Publication'
        elif 'fsa weekly review' in title_lower:
            return 'Weekly Review'
        elif 'meeting' in title_lower or 'council' in title_lower:
            return 'Meeting'
        elif 'news' in title_lower or 'announcement' in title_lower:
            return 'News'
        elif 'analytical notes' in title_lower:
            return 'Analytical Notes'
        else:
            return 'General'
    
    async def scrape_article(self, item: Dict) -> Optional[Dict]:
        """Scrape full content from an article page"""
        url = item['url']
        
        # Check if already scraped
        if url in self.scraped_urls:
            logger.info(f"Skipping already scraped URL: {url}")
            return None
        
        logger.info(f"Scraping article: {url}")
        
        # Check if URL is a PDF
        if url.lower().endswith('.pdf'):
            return await self._handle_pdf_direct(item, url)
        
        page = await self.context.new_page()
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=30000)
            await self.random_delay(1, 2)
            
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract main content
            main_content = self._extract_main_content(soup)
            
            # Find and process attachments
            attachments = await self._extract_attachments(soup, url, page)
            
            # Find related links (excluding social media and navigation)
            related_links = self._extract_related_links(soup, url)
            
            # Generate unique ID
            item_id = self._generate_id(url, item['published_date'])
            
            result = {
                'id': item_id,
                'headline': item['headline'],
                'published_date': item['published_date'],
                'scraped_date': datetime.now(timezone.utc).isoformat(),
                'type': item['type'],
                'url': url,
                'content_text': main_content,
                'attachments': attachments,
                'related_links': related_links
            }
            
            self.scraped_urls.add(url)
            return result
            
        except Exception as e:
            logger.error(f"Error scraping article {url}: {e}")
            return None
        finally:
            await page.close()
    
    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract main text content from article page, including tables"""
        main_div = soup.find('div', id='main')
        if not main_div:
            return ""
        
        # Find the inner div which contains actual content
        inner_div = main_div.find('div', class_='inner')
        if not inner_div:
            inner_div = main_div
        
        # Remove script, style, and unwanted elements
        for tag in inner_div.find_all(['script', 'style', 'nav', 'header', 'footer', 'iframe']):
            tag.decompose()
        
        # Remove share buttons and navigation
        for tag in inner_div.find_all(['p', 'div'], class_=['share-button', 'navihidden']):
            tag.decompose()
        
        # Extract text from content elements
        text_parts = []
        
        # Extract title
        h1 = inner_div.find('h1')
        if h1:
            text_parts.append(h1.get_text(strip=True))
        
        # Extract paragraphs, lists, and headers
        for elem in inner_div.find_all(['p', 'li', 'dd', 'h2', 'h3', 'h4']):
            text = elem.get_text(strip=True)
            # Filter out very short fragments and navigation text
            if text and len(text) > 10 and not text.startswith('Contact'):
                text_parts.append(text)
        
        # Extract tables with proper structure
        tables = inner_div.find_all('table')
        for table in tables:
            table_text = self._extract_table_content(table)
            if table_text:
                text_parts.append(f"\n[TABLE]\n{table_text}\n[/TABLE]\n")
        
        # Clean and join
        full_text = '\n\n'.join(text_parts)
        full_text = re.sub(r'\s+', ' ', full_text)
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        
        return full_text.strip()
    
    def _extract_table_content(self, table) -> str:
        """Extract structured content from HTML table"""
        try:
            rows = []
            
            # Process table headers
            headers = []
            for th in table.find_all('th'):
                header_text = th.get_text(strip=True)
                if header_text:
                    headers.append(header_text)
            
            if headers:
                rows.append(' | '.join(headers))
                rows.append('-' * (len(' | '.join(headers))))
            
            # Process table rows
            for tr in table.find_all('tr'):
                cells = []
                
                # Get both th and td cells
                for cell in tr.find_all(['th', 'td']):
                    cell_text = cell.get_text(strip=True)
                    if cell_text:
                        cells.append(cell_text)
                
                if cells and not (len(cells) == 1 and cells[0] in [h for h in headers]):
                    rows.append(' | '.join(cells))
            
            return '\n'.join(rows) if rows else ''
            
        except Exception as e:
            logger.warning(f"Error extracting table content: {e}")
            return ''
    
    def _extract_related_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract related links from main content, excluding navigation/social media"""
        links = []
        excluded_domains = ['facebook.com', 'linkedin.com', 'twitter.com', 'x.com', 
                          'instagram.com', 'youtube.com', 'platform.twitter.com']
        
        # Only look for links within the main content area
        main_div = soup.find('div', id='main')
        if not main_div:
            return links
        
        inner_div = main_div.find('div', class_='inner')
        if not inner_div:
            return links
        
        excluded_patterns = [
            'skip to', 'contact', 'japanese', 'new window', 'icon',
            'pdf', 'excel', 'csv', 'download'
        ]
        
        for link in inner_div.find_all('a', href=True):
            href = link.get('href')
            text = link.get_text(strip=True)
            
            # Skip empty or very short text
            if not text or len(text) < 15:
                continue
            
            # Skip if text matches excluded patterns
            text_lower = text.lower()
            if any(pattern in text_lower for pattern in excluded_patterns):
                continue
            
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            
            # Skip anchor-only links
            if href.startswith('#'):
                continue
            
            # Skip social media
            if any(domain in parsed.netloc for domain in excluded_domains):
                continue
            
            # Skip file downloads (we handle them in attachments)
            if href.lower().endswith(('.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx')):
                continue
            
            # Skip if same as base URL
            base_parsed = urlparse(base_url)
            if parsed.netloc == base_parsed.netloc and parsed.path == base_parsed.path:
                continue
            
            # Add valid link
            if full_url not in links:
                links.append(full_url)
        
        return links[:10]  # Limit to 10 most relevant
    
    async def _extract_attachments(self, soup: BeautifulSoup, base_url: str, page: Page) -> List[Dict]:
        """Extract and process all attachments (PDFs, Excel, CSV)"""
        attachments = []
        
        # Find all links to files
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            full_url = urljoin(base_url, href)
            
            file_type = None
            if full_url.lower().endswith('.pdf'):
                file_type = 'pdf'
            elif full_url.lower().endswith(('.xlsx', '.xls')):
                file_type = 'xlsx'
            elif full_url.lower().endswith('.csv'):
                file_type = 'csv'
            
            if file_type:
                file_data = await self._process_file(full_url, file_type, page)
                if file_data:
                    attachments.append(file_data)
        
        return attachments
    
    async def _process_file(self, url: str, file_type: str, page: Page) -> Optional[Dict]:
        """Process a file (PDF, Excel, or CSV) and extract content"""
        try:
            # Check if already processed
            url_hash = hashlib.md5(url.encode()).hexdigest()
            if url_hash in self.pdf_hashes:
                logger.info(f"Skipping duplicate file: {url}")
                return None
            
            logger.info(f"Processing {file_type.upper()}: {url}")
            
            # Download file
            response = await page.request.get(url)
            if response.status != 200:
                logger.warning(f"Failed to download file {url}: {response.status}")
                return None
            
            file_bytes = await response.body()
            
            # Extract text based on file type
            text = ""
            if file_type == 'pdf':
                text = self._extract_pdf_text(file_bytes)
            elif file_type == 'xlsx':
                text = self._extract_excel_text(file_bytes)
            elif file_type == 'csv':
                text = self._extract_csv_text(file_bytes)
            
            if text:
                self.pdf_hashes.add(url_hash)
                return {
                    'file_name': url.split('/')[-1],
                    'file_type': file_type,
                    'file_url': url,
                    'text_extracted': text
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing file {url}: {e}")
            return None
    
    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes, including charts and images with OCR"""
        text_parts = []
        
        try:
            pdf_file = io.BytesIO(pdf_bytes)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            logger.info(f"Processing PDF with {len(pdf_reader.pages)} pages")
            
            for page_num, page in enumerate(pdf_reader.pages):
                try:
                    # First, try standard text extraction
                    page_text = page.extract_text()
                    
                    # Also extract text from images/charts on the page using OCR
                    # This ensures we capture text from charts, graphs, and embedded images
                    ocr_text = self._ocr_pdf_page(pdf_bytes, page_num)
                    
                    # Combine both sources
                    combined_text = ""
                    
                    if page_text and page_text.strip():
                        combined_text += page_text
                    
                    if ocr_text and ocr_text.strip():
                        # If we have OCR text, add it
                        # OCR might duplicate some text, but it also captures chart labels/data
                        if combined_text:
                            combined_text += "\n\n[OCR_EXTRACTED_TEXT]\n" + ocr_text
                        else:
                            combined_text = ocr_text
                    
                    if combined_text:
                        text_parts.append(f"--- Page {page_num + 1} ---\n{combined_text}")
                    else:
                        logger.warning(f"No text found on page {page_num + 1}")
                        
                except Exception as e:
                    logger.warning(f"Error extracting page {page_num + 1}: {e}")
            
            # Try alternative PDF libraries if PyPDF2 extraction is poor
            if not text_parts or sum(len(t) for t in text_parts) < 100:
                logger.info("PyPDF2 extraction yielded minimal text, trying pdfplumber")
                alternative_text = self._extract_pdf_with_pdfplumber(pdf_bytes)
                if alternative_text:
                    return alternative_text
            
            # Clean and combine text
            full_text = '\n\n'.join(text_parts)
            full_text = re.sub(r'\s+', ' ', full_text)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            
            return full_text.strip()
            
        except Exception as e:
            logger.error(f"Error in PDF text extraction: {e}")
            # Last resort: full OCR
            try:
                return self._full_ocr_pdf(pdf_bytes)
            except Exception as ocr_error:
                logger.error(f"OCR fallback also failed: {ocr_error}")
                return ""
    
    def _extract_pdf_with_pdfplumber(self, pdf_bytes: bytes) -> str:
        """Extract PDF text using pdfplumber (better for tables and structured content)"""
        try:
            import pdfplumber
            
            text_parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    # Extract text
                    page_text = page.extract_text()
                    
                    # Extract tables separately for better structure
                    tables = page.extract_tables()
                    
                    combined = f"--- Page {page_num + 1} ---\n"
                    
                    if page_text:
                        combined += page_text + "\n"
                    
                    # Add tables in structured format
                    if tables:
                        for table_num, table in enumerate(tables):
                            combined += f"\n[TABLE {table_num + 1}]\n"
                            for row in table:
                                if row:
                                    row_text = ' | '.join([str(cell) if cell else '' for cell in row])
                                    combined += row_text + "\n"
                            combined += "[/TABLE]\n"
                    
                    text_parts.append(combined)
            
            return '\n\n'.join(text_parts)
            
        except ImportError:
            logger.warning("pdfplumber not installed, skipping alternative extraction")
            return ""
        except Exception as e:
            logger.warning(f"pdfplumber extraction failed: {e}")
            return ""
    
    def _ocr_pdf_page(self, pdf_bytes: bytes, page_num: int) -> str:
        """Perform OCR on a PDF page to extract text from images and charts"""
        try:
            # Convert PDF page to image at higher DPI for better OCR accuracy
            images = convert_from_bytes(
                pdf_bytes, 
                first_page=page_num + 1, 
                last_page=page_num + 1,
                dpi=300  # Higher DPI for better text recognition in charts
            )
            
            if images:
                # Use Tesseract with custom config for better chart/table recognition
                custom_config = r'--oem 3 --psm 6'  # PSM 6: Assume uniform block of text
                text = pytesseract.image_to_string(images[0], config=custom_config)
                return text.strip()
                
        except Exception as e:
            logger.warning(f"OCR failed for page {page_num + 1}: {e}")
        
        return ""
    
    def _full_ocr_pdf(self, pdf_bytes: bytes) -> str:
        """Perform full OCR on entire PDF as last resort"""
        try:
            logger.info("Attempting full OCR extraction as fallback")
            images = convert_from_bytes(pdf_bytes, dpi=300)
            
            text_parts = []
            for page_num, image in enumerate(images):
                custom_config = r'--oem 3 --psm 6'
                page_text = pytesseract.image_to_string(image, config=custom_config)
                if page_text.strip():
                    text_parts.append(f"--- Page {page_num + 1} ---\n{page_text}")
            
            return '\n\n'.join(text_parts)
            
        except Exception as e:
            logger.error(f"Full OCR failed: {e}")
            return ""
    
    def _extract_excel_text(self, excel_bytes: bytes) -> str:
        """Extract text from Excel bytes"""
        try:
            df_dict = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=None)
            
            text_parts = []
            for sheet_name, sheet_df in df_dict.items():
                text_parts.append(f"Sheet: {sheet_name}\n")
                text_parts.append(sheet_df.to_string())
            
            return '\n\n'.join(text_parts)
            
        except Exception as e:
            logger.error(f"Error extracting Excel text: {e}")
            return ""
    
    def _extract_csv_text(self, csv_bytes: bytes) -> str:
        """Extract text from CSV bytes"""
        try:
            csv_text = csv_bytes.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_text))
            return df.to_string()
            
        except Exception as e:
            logger.error(f"Error extracting CSV text: {e}")
            return ""
    
    async def _handle_pdf_direct(self, item: Dict, url: str) -> Optional[Dict]:
        """Handle items that link directly to PDFs"""
        logger.info(f"Handling direct PDF link: {url}")
        
        page = await self.context.new_page()
        
        try:
            pdf_data = await self._process_file(url, 'pdf', page)
            
            if not pdf_data:
                return None
            
            item_id = self._generate_id(url, item['published_date'])
            
            result = {
                'id': item_id,
                'headline': item['headline'],
                'published_date': item['published_date'],
                'scraped_date': datetime.now(timezone.utc).isoformat(),
                'type': item['type'],
                'url': url,
                'content_text': pdf_data['text_extracted'],
                'attachments': [pdf_data],
                'related_links': []
            }
            
            self.scraped_urls.add(url)
            return result
            
        finally:
            await page.close()
    
    def _generate_id(self, url: str, date: str) -> str:
        """Generate unique ID for an item"""
        date_part = date.replace('-', '')
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        return f"fsa{date_part}_{url_hash}"
    
    def _save_results(self, new_items: List[Dict]):
        """Save results to JSON file"""
        # Combine with existing data
        all_data = self.existing_data + new_items
        
        # Sort by date (newest first)
        all_data.sort(key=lambda x: x['published_date'], reverse=True)
        
        # Save to file
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(all_data)} total records to {OUTPUT_FILE}")
        logger.info(f"Added {len(new_items)} new records in this run")
    
    async def run(self):
        """Main execution method"""
        try:
            await self.initialize()
            
            # Fetch index page
            items = await self.fetch_index_page()
            
            if not items:
                logger.warning("No items found on index page")
                return
            
            # Filter out already scraped items
            new_items = [item for item in items if item['url'] not in self.scraped_urls]
            logger.info(f"Found {len(new_items)} new items to scrape")
            
            # Scrape each article
            results = []
            for i, item in enumerate(new_items, 1):
                logger.info(f"Processing item {i}/{len(new_items)}")
                
                result = await self.scrape_article(item)
                if result:
                    results.append(result)
                
                # Random delay between requests
                await self.random_delay(2, 5)
            
            # Save results
            if results:
                self._save_results(results)
                logger.info(f"Successfully scraped {len(results)} new articles")
            else:
                logger.info("No new articles were scraped")
            
        except Exception as e:
            logger.error(f"Error in main execution: {e}", exc_info=True)
        
        finally:
            # Cleanup
            if self.browser:
                await self.browser.close()
            
            logger.info("Scraper execution completed")


async def main():
    """Entry point for the scraper"""
    logger.info("=" * 80)
    logger.info("FSA Japan News Scraper - Starting")
    logger.info(f"Start date filter: {START_DATE.strftime('%Y-%m-%d')}")
    logger.info("=" * 80)
    
    scraper = FSAScraper(start_date=START_DATE)
    await scraper.run()
    
    logger.info("=" * 80)
    logger.info("FSA Japan News Scraper - Completed")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())