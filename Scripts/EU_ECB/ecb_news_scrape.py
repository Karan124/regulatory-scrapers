"""
ECB News & Publications Scraper
Production-grade script for scraping European Central Bank news, publications, speeches, etc.
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
START_DATE = datetime(2025, 10, 1)  # Configurable start date
BASE_URL = "https://www.bankingsupervision.europa.eu"
INDEX_URL = f"{BASE_URL}/press/pubbydate/html/index.en.html"
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "ecb_news.json"
LOG_FILE = OUTPUT_DIR / "ecb_scraper.log"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ECBScraper:
    """Main scraper class for ECB news and publications"""
    
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
            'Accept-Language': 'en-US,en;q=0.9',
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
        logger.info("Initializing scraper...")
        
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
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
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
            max_scroll_attempts = 20
            
            while scroll_attempts < max_scroll_attempts:
                # Get current scroll height
                current_height = await page.evaluate("document.body.scrollHeight")
                
                if current_height == previous_height:
                    # No new content loaded, try a few more times
                    scroll_attempts += 1
                else:
                    scroll_attempts = 0  # Reset if new content appeared
                
                # Scroll down in chunks
                await page.evaluate(f"window.scrollTo(0, {current_height})")
                await asyncio.sleep(1.5)
                
                # Also try scrolling up and down to trigger observers
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
                dates = [item['date_published'] for item in items]
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
        dl_wrapper = soup.find('div', class_='dl-wrapper')
        if not dl_wrapper:
            return False
        
        # Find all date elements
        date_elements = dl_wrapper.find_all('dt')
        if not date_elements:
            return False
        
        # Check the last (oldest) date on the page
        last_date_text = date_elements[-1].get_text(strip=True)
        try:
            last_date = datetime.strptime(last_date_text, '%d %B %Y')
            # If we've found dates before our start date, we're done
            return last_date < self.start_date
        except ValueError:
            return False
    
    def _parse_index_page(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse the index page HTML to extract publication metadata"""
        items = []
        dl_wrapper = soup.find('div', class_='dl-wrapper')
        
        if not dl_wrapper:
            logger.warning("Could not find dl-wrapper on index page")
            return items
        
        current_date = None
        
        for element in dl_wrapper.find_all(['dt', 'dd']):
            if element.name == 'dt':
                # Date header
                date_text = element.get_text(strip=True)
                try:
                    current_date = datetime.strptime(date_text, '%d %B %Y')
                except ValueError:
                    logger.warning(f"Could not parse date: {date_text}")
                    continue
            
            elif element.name == 'dd' and current_date:
                # Skip if date is before our start date
                if current_date < self.start_date:
                    continue
                
                # Extract item details
                item = self._extract_item_details(element, current_date)
                if item:
                    items.append(item)
        
        return items
    
    def _extract_item_details(self, dd_element, pub_date: datetime) -> Optional[Dict]:
        """Extract details from a single publication item"""
        try:
            category_elem = dd_element.find('div', class_='category')
            title_elem = dd_element.find('div', class_='title')
            
            if not title_elem:
                return None
            
            link_elem = title_elem.find('a')
            if not link_elem:
                return None
            
            url = urljoin(BASE_URL, link_elem.get('href', ''))
            title = link_elem.get_text(strip=True)
            category = category_elem.get_text(strip=True) if category_elem else "Unknown"
            
            # Extract authors if available
            authors = []
            authors_elem = dd_element.find('div', class_='authors')
            if authors_elem:
                author_items = authors_elem.find_all('li')
                authors = [li.get_text(strip=True) for li in author_items]
            
            # Extract subtitle from accordion if available
            subtitle = None
            accordion = dd_element.find('div', class_='accordion')
            if accordion:
                subtitle_elem = accordion.find('dd')
                if subtitle_elem:
                    subtitle = subtitle_elem.get_text(strip=True)
            
            return {
                'url': url,
                'title': title,
                'type': category,
                'date_published': pub_date.strftime('%Y-%m-%d'),
                'authors': authors,
                'subtitle': subtitle,
            }
            
        except Exception as e:
            logger.error(f"Error extracting item details: {e}")
            return None
    
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
            
            # Extract theme/topics
            themes = self._extract_themes(soup)
            
            # Find and process attachments
            attachments = await self._extract_attachments(soup, url, page)
            
            # Find secondary links (excluding social media)
            secondary_links = self._extract_secondary_links(soup, url)
            
            # Generate unique ID
            item_id = self._generate_id(url, item['date_published'])
            
            result = {
                'id': item_id,
                'url': url,
                'title': item['title'],
                'type': item['type'],
                'date_published': item['date_published'],
                'scraped_date': datetime.now(timezone.utc).isoformat(),
                'theme': themes,
                'authors': item.get('authors', []),
                'subtitle': item.get('subtitle'),
                'content': {
                    'main_text': main_content,
                    'attachments': attachments,
                    'secondary_links': secondary_links
                }
            }
            
            self.scraped_urls.add(url)
            return result
            
        except Exception as e:
            logger.error(f"Error scraping article {url}: {e}")
            return None
        finally:
            await page.close()
    
    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract main text content from article page"""
        main_elem = soup.find('main')
        if not main_elem:
            return ""
        
        # Remove script, style, and navigation elements
        for tag in main_elem.find_all(['script', 'style', 'nav', 'header', 'footer']):
            tag.decompose()
        
        # Extract text from paragraphs and other content elements
        text_parts = []
        for elem in main_elem.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'li']):
            text = elem.get_text(strip=True)
            if text and len(text) > 10:  # Filter out very short fragments
                text_parts.append(text)
        
        return '\n\n'.join(text_parts)
    
    def _extract_themes(self, soup: BeautifulSoup) -> List[str]:
        """Extract themes/topics from the page"""
        themes = []
        topics_div = soup.find('div', class_='related-topics')
        
        if topics_div:
            topic_links = topics_div.find_all('a', class_='taxonomy-tag')
            themes = [link.get_text(strip=True) for link in topic_links]
        
        return themes
    
    def _extract_secondary_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract secondary links from main content only, excluding navigation/UI elements"""
        links = []
        excluded_domains = ['facebook.com', 'linkedin.com', 'twitter.com', 'x.com', 'instagram.com']
        
        # Only look for links within the main content area
        main_elem = soup.find('main')
        if not main_elem:
            return links
        
        # Find the actual content section (usually within divs with class 'section')
        content_sections = main_elem.find_all('div', class_='section')
        if not content_sections:
            # Fallback to paragraphs if no section divs found
            content_sections = [main_elem]
        
        excluded_patterns = [
            'skip to', 'skip', 'language', 'search', 'menu', 'navigation',
            'contact', 'subscribe', 'cookie', 'disclaimer', 'copyright',
            'български', 'čeština', 'deutsch', 'español', 'français', 'italiano',
            'latviešu', 'lietuvių', 'magyar', 'malti', 'nederlands', 'polski',
            'português', 'română', 'slovenčina', 'slovenščina', 'suomi', 'svenska',
            'ελληνικά', 'eesti keel', 'hrvatski', 'dansk', 'íslenska'
        ]
        
        for section in content_sections:
            for link in section.find_all('a', href=True):
                href = link.get('href')
                text = link.get_text(strip=True)
                
                # Skip empty or very short text
                if not text or len(text) < 10:
                    continue
                
                # Skip if text matches excluded patterns (case-insensitive)
                text_lower = text.lower()
                if any(pattern in text_lower for pattern in excluded_patterns):
                    continue
                
                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
                
                # Skip anchor-only links (page navigation)
                if href.startswith('#'):
                    continue
                
                # Skip if just a language variant of the same page
                if href.endswith(('.bg.html', '.cs.html', '.de.html', '.es.html', 
                                 '.fr.html', '.it.html', '.pt.html', '.el.html',
                                 '.et.html', '.lv.html', '.lt.html', '.hu.html',
                                 '.mt.html', '.nl.html', '.pl.html', '.ro.html',
                                 '.sk.html', '.sl.html', '.fi.html', '.sv.html',
                                 '.hr.html', '.da.html', '.is.html')):
                    continue
                
                # Skip social media
                if any(domain in parsed.netloc for domain in excluded_domains):
                    continue
                
                # Skip if same as base URL (removing fragments)
                base_parsed = urlparse(base_url)
                if parsed.netloc == base_parsed.netloc and parsed.path == base_parsed.path:
                    continue
                
                # Only include links that appear to be to documents, reports, or related content
                # These usually have descriptive text and point to actual content
                if (len(text) > 15 and 
                    not text.lower().startswith(('download', 'pdf', 'excel'))):
                    links.append({
                        'url': full_url,
                        'text': text
                    })
        
        # Remove duplicates while preserving order
        seen_urls = set()
        unique_links = []
        for link in links:
            if link['url'] not in seen_urls:
                seen_urls.add(link['url'])
                unique_links.append(link)
        
        return unique_links[:10]  # Limit to 10 most relevant links
    
    async def _extract_attachments(self, soup: BeautifulSoup, base_url: str, page: Page) -> Dict:
        """Extract and process all attachments (PDFs, Excel, CSV)"""
        attachments = {
            'pdfs': [],
            'excels': [],
            'csvs': []
        }
        
        # Find all links to files
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            full_url = urljoin(base_url, href)
            
            if full_url.lower().endswith('.pdf'):
                pdf_data = await self._process_pdf(full_url, page)
                if pdf_data:
                    attachments['pdfs'].append(pdf_data)
            
            elif full_url.lower().endswith(('.xlsx', '.xls')):
                excel_data = await self._process_excel(full_url)
                if excel_data:
                    attachments['excels'].append(excel_data)
            
            elif full_url.lower().endswith('.csv'):
                csv_data = await self._process_csv(full_url)
                if csv_data:
                    attachments['csvs'].append(csv_data)
        
        return attachments
    
    async def _process_pdf(self, url: str, page: Page) -> Optional[Dict]:
        """Download and extract text from PDF"""
        try:
            # Check if already processed (deduplication)
            url_hash = hashlib.md5(url.encode()).hexdigest()
            if url_hash in self.pdf_hashes:
                logger.info(f"Skipping duplicate PDF: {url}")
                return None
            
            logger.info(f"Processing PDF: {url}")
            
            # Download PDF
            response = await page.request.get(url)
            if response.status != 200:
                logger.warning(f"Failed to download PDF {url}: {response.status}")
                return None
            
            pdf_bytes = await response.body()
            
            # Extract text
            text = self._extract_pdf_text(pdf_bytes)
            
            if text:
                self.pdf_hashes.add(url_hash)
                return {
                    'file_name': url.split('/')[-1],
                    'url': url,
                    'extracted_text': text
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing PDF {url}: {e}")
            return None
    
    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes, with OCR fallback"""
        text_parts = []
        
        try:
            # Try standard text extraction first
            pdf_file = io.BytesIO(pdf_bytes)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            for page_num, page in enumerate(pdf_reader.pages):
                try:
                    page_text = page.extract_text()
                    if page_text and page_text.strip():
                        text_parts.append(page_text)
                    else:
                        # Try OCR if no text found
                        ocr_text = self._ocr_pdf_page(pdf_bytes, page_num)
                        if ocr_text:
                            text_parts.append(ocr_text)
                except Exception as e:
                    logger.warning(f"Error extracting page {page_num}: {e}")
            
            # Clean and normalize text
            full_text = '\n\n'.join(text_parts)
            full_text = re.sub(r'\s+', ' ', full_text)  # Remove excessive whitespace
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)  # Limit newlines
            
            return full_text.strip()
            
        except Exception as e:
            logger.error(f"Error in PDF text extraction: {e}")
            return ""
    
    def _ocr_pdf_page(self, pdf_bytes: bytes, page_num: int) -> str:
        """Perform OCR on a PDF page"""
        try:
            images = convert_from_bytes(pdf_bytes, first_page=page_num+1, last_page=page_num+1)
            if images:
                text = pytesseract.image_to_string(images[0])
                return text
        except Exception as e:
            logger.warning(f"OCR failed for page {page_num}: {e}")
        return ""
    
    async def _process_excel(self, url: str) -> Optional[Dict]:
        """Download and extract content from Excel file"""
        try:
            logger.info(f"Processing Excel: {url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers) as response:
                    if response.status != 200:
                        return None
                    
                    excel_bytes = await response.read()
            
            # Read Excel file
            df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=None)
            
            # Convert all sheets to text
            text_parts = []
            for sheet_name, sheet_df in df.items():
                text_parts.append(f"Sheet: {sheet_name}\n")
                text_parts.append(sheet_df.to_string())
            
            return {
                'file_name': url.split('/')[-1],
                'url': url,
                'extracted_text': '\n\n'.join(text_parts)
            }
            
        except Exception as e:
            logger.error(f"Error processing Excel {url}: {e}")
            return None
    
    async def _process_csv(self, url: str) -> Optional[Dict]:
        """Download and extract content from CSV file"""
        try:
            logger.info(f"Processing CSV: {url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers) as response:
                    if response.status != 200:
                        return None
                    
                    csv_text = await response.text()
            
            # Parse CSV
            df = pd.read_csv(io.StringIO(csv_text))
            
            return {
                'file_name': url.split('/')[-1],
                'url': url,
                'extracted_text': df.to_string()
            }
            
        except Exception as e:
            logger.error(f"Error processing CSV {url}: {e}")
            return None
    
    async def _handle_pdf_direct(self, item: Dict, url: str) -> Optional[Dict]:
        """Handle items that link directly to PDFs"""
        logger.info(f"Handling direct PDF link: {url}")
        
        page = await self.context.new_page()
        
        try:
            pdf_data = await self._process_pdf(url, page)
            
            if not pdf_data:
                return None
            
            item_id = self._generate_id(url, item['date_published'])
            
            result = {
                'id': item_id,
                'url': url,
                'title': item['title'],
                'type': item['type'],
                'date_published': item['date_published'],
                'scraped_date': datetime.now(timezone.utc).isoformat(),
                'theme': [],
                'authors': item.get('authors', []),
                'subtitle': item.get('subtitle'),
                'content': {
                    'main_text': pdf_data['extracted_text'],
                    'attachments': {'pdfs': [pdf_data], 'excels': [], 'csvs': []},
                    'secondary_links': []
                }
            }
            
            self.scraped_urls.add(url)
            return result
            
        finally:
            await page.close()
    
    def _generate_id(self, url: str, date: str) -> str:
        """Generate unique ID for an item"""
        # Extract relevant parts and create hash
        url_part = url.split('/')[-1].replace('.en.html', '').replace('.pdf', '')
        date_part = date.replace('-', '')
        
        # Create a short hash from the URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        
        return f"ecb{date_part}_{url_hash}"
    
    def _save_results(self, new_items: List[Dict]):
        """Save results to JSON file"""
        # Combine with existing data
        all_data = self.existing_data + new_items
        
        # Sort by date (newest first)
        all_data.sort(key=lambda x: x['date_published'], reverse=True)
        
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
    logger.info("ECB News Scraper - Starting")
    logger.info(f"Start date filter: {START_DATE.strftime('%Y-%m-%d')}")
    logger.info("=" * 80)
    
    scraper = ECBScraper(start_date=START_DATE)
    await scraper.run()
    
    logger.info("=" * 80)
    logger.info("ECB News Scraper - Completed")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())