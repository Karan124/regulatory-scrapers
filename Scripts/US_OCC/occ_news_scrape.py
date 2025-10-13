"""
OCC Newsroom Scraper - Production Ready
========================================
Scrapes news articles from the Office of the Comptroller of the Currency (OCC)
with full attachment extraction, table parsing, and LLM-ready output.

Requirements Implemented:
- ✓ Scrapes all news feeds with pagination (stf parameter)
- ✓ Extracts: headline, date, type, themes, full text, attachments, URL
- ✓ Handles JavaScript-rendered Angular content (Playwright)
- ✓ Extracts PDF, Excel, CSV attachment text
- ✓ Extracts HTML tables in structured format
- ✓ Anti-bot measures: realistic headers, stealth mode, rate limiting
- ✓ Deduplication: tracks existing URLs, appends only new articles
- ✓ Output: Clean JSON suitable for LLM processing
- ✓ Error handling and logging throughout

Author: Claude
Date: October 2025
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin
from io import BytesIO

# Third-party imports
from playwright.async_api import async_playwright, Page, Browser
from bs4 import BeautifulSoup
import aiohttp
import PyPDF2
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('occ_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OCCNewsScraper:
    """
    Scraper for OCC Newsroom using Playwright for JavaScript rendering.
    Handles pagination, attachment extraction, and produces LLM-ready output.
    """
    
    BASE_URL = "https://www.occ.treas.gov"
    NEWSROOM_URL = f"{BASE_URL}/news-events/newsroom/"
    
    def __init__(self, output_dir: str = "data", rate_limit: float = 2.0):
        """
        Initialize the OCC scraper.
        
        Args:
            output_dir: Directory to save output JSON (default: "data")
            rate_limit: Delay between requests in seconds (default: 2.0)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.output_file = self.output_dir / "occ_news.json"
        self.rate_limit = rate_limit
        
        # Browser and session objects
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Tracking
        self.existing_urls: Set[str] = set()
        self.scraped_count = 0
        
    async def __aenter__(self):
        """Async context manager entry - initializes browser and HTTP session."""
        try:
            # Start Playwright
            self.playwright = await async_playwright().start()
            
            # Launch browser with stealth settings to avoid detection
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security'
                ]
            )
            
            # Create browser context with realistic settings
            context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                locale='en-US',
                timezone_id='America/New_York'
            )
            
            # Create page
            self.page = await context.new_page()
            
            # Setup HTTP session for binary downloads (PDFs, Excel files)
            timeout = aiohttp.ClientTimeout(total=60, connect=30)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/pdf,application/vnd.ms-excel,text/csv,*/*'
            }
            self.http_session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers
            )
            
            logger.info("Browser and HTTP session initialized successfully")
            return self
            
        except Exception as e:
            logger.error(f"Failed to initialize scraper: {e}")
            raise
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup resources."""
        try:
            if self.http_session:
                await self.http_session.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            
    def load_existing_data(self) -> List[Dict]:
        """
        Load existing scraped data from JSON file to avoid duplicates.
        
        Returns:
            List of existing article dictionaries
        """
        if self.output_file.exists():
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.existing_urls = {item['url'] for item in data if 'url' in item}
                    logger.info(f"Loaded {len(data)} existing articles ({len(self.existing_urls)} unique URLs)")
                    return data
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in existing file: {e}")
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
        return []
        
    def save_data(self, articles: List[Dict]) -> None:
        """
        Save scraped articles to JSON file.
        
        Args:
            articles: List of article dictionaries to save
        """
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(articles, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(articles)} articles to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            raise
            
    async def fetch_binary(self, url: str, max_retries: int = 3) -> Optional[bytes]:
        """
        Fetch binary content (PDFs, Excel, CSV files) via HTTP with retry logic.
        
        Args:
            url: URL of the binary file
            max_retries: Maximum number of retry attempts
            
        Returns:
            Binary content as bytes, or None if failed
        """
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(self.rate_limit)
                async with self.http_session.get(url) as response:
                    if response.status == 200:
                        return await response.read()
                    else:
                        logger.warning(f"Status {response.status} for {url}, attempt {attempt + 1}")
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {url}, attempt {attempt + 1}")
            except Exception as e:
                logger.error(f"Error fetching binary {url}: {e}")
                
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                
        return None
        
    def extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """
        Extract complete text from PDF bytes.
        
        Args:
            pdf_bytes: Raw PDF file bytes
            
        Returns:
            Extracted and cleaned text content
        """
        try:
            pdf_file = BytesIO(pdf_bytes)
            reader = PyPDF2.PdfReader(pdf_file)
            text_parts = []
            
            for page_num, page in enumerate(reader.pages):
                try:
                    text = page.extract_text()
                    if text:
                        text_parts.append(text)
                except Exception as e:
                    logger.warning(f"Error extracting page {page_num}: {e}")
                    
            # Combine and clean text
            full_text = "\n".join(text_parts)
            # Remove excessive whitespace
            full_text = re.sub(r'\s+', ' ', full_text)
            # Remove control characters
            full_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', full_text)
            full_text = full_text.strip()
            
            return full_text
            
        except Exception as e:
            logger.error(f"Error extracting PDF text: {e}")
            return ""
            
    def extract_excel_text(self, excel_bytes: bytes) -> str:
        """
        Extract text from Excel file (XLSX, XLS).
        
        Args:
            excel_bytes: Raw Excel file bytes
            
        Returns:
            Text representation of all sheets and cells
        """
        try:
            # Read all sheets
            excel_file = BytesIO(excel_bytes)
            df_dict = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')
            text_parts = []
            
            for sheet_name, df in df_dict.items():
                text_parts.append(f"\n[Sheet: {sheet_name}]")
                
                # Convert DataFrame to text representation
                for col in df.columns:
                    col_name = str(col)
                    if col_name and col_name != 'Unnamed':
                        text_parts.append(f"{col_name}:")
                    
                    # Get all non-null values
                    values = df[col].dropna().astype(str).tolist()
                    text_parts.extend(values)
                    
            return "\n".join(text_parts)
            
        except Exception as e:
            logger.error(f"Error extracting Excel text: {e}")
            return ""
            
    def extract_csv_text(self, csv_bytes: bytes) -> str:
        """
        Extract text from CSV file.
        
        Args:
            csv_bytes: Raw CSV file bytes
            
        Returns:
            Text representation of CSV data
        """
        try:
            csv_file = BytesIO(csv_bytes)
            df = pd.read_csv(csv_file)
            text_parts = []
            
            for col in df.columns:
                col_name = str(col)
                text_parts.append(f"{col_name}:")
                
                # Get all non-null values
                values = df[col].dropna().astype(str).tolist()
                text_parts.extend(values)
                
            return "\n".join(text_parts)
            
        except Exception as e:
            logger.error(f"Error extracting CSV text: {e}")
            return ""
    
    def extract_tables_as_text(self, soup: BeautifulSoup) -> str:
        """
        Extract all HTML tables and convert to structured text format.
        
        Args:
            soup: BeautifulSoup object containing the HTML
            
        Returns:
            Formatted text representation of all tables
        """
        table_texts = []
        tables = soup.find_all('table')
        
        for idx, table in enumerate(tables, 1):
            try:
                rows_data = []
                headers = []
                
                # Find all rows
                all_rows = table.find_all('tr')
                
                if not all_rows:
                    continue
                
                # Try to extract headers from first row
                first_row = all_rows[0]
                header_cells = first_row.find_all(['th', 'td'])
                
                # Check if first row is a header
                has_header = bool(first_row.find_all('th'))
                
                if has_header:
                    for cell in header_cells:
                        cell_text = cell.get_text(separator=' ', strip=True)
                        # Remove footnote superscripts
                        cell_text = re.sub(r'\[\d+\]', '', cell_text)
                        cell_text = re.sub(r'\d+$', '', cell_text).strip()
                        headers.append(cell_text)
                    data_rows = all_rows[1:]
                else:
                    data_rows = all_rows
                
                # Extract data rows
                for tr in data_rows:
                    row_data = []
                    for cell in tr.find_all(['td', 'th']):
                        cell_text = cell.get_text(separator=' ', strip=True)
                        # Clean up whitespace and footnotes
                        cell_text = re.sub(r'\s+', ' ', cell_text)
                        cell_text = re.sub(r'\[\d+\]', '', cell_text).strip()
                        row_data.append(cell_text)
                    
                    if row_data and any(row_data):  # Only add non-empty rows
                        rows_data.append(row_data)
                
                # Format table as text
                if headers or rows_data:
                    table_text = f"\n[Table {idx}]\n"
                    
                    if headers:
                        table_text += "Headers: " + " | ".join(headers) + "\n"
                    
                    for row in rows_data:
                        table_text += " | ".join(row) + "\n"
                    
                    table_texts.append(table_text)
                    
            except Exception as e:
                logger.warning(f"Error extracting table {idx}: {e}")
                continue
        
        return "\n".join(table_texts)
            
    async def extract_attachments_from_html(self, html_content: str, base_url: str) -> str:
        """
        Extract and parse all attachments (PDF, Excel, CSV) from article HTML.
        
        Args:
            html_content: HTML content of the article page
            base_url: Base URL for resolving relative links
            
        Returns:
            Combined text from all attachments
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        attachment_texts = []
        processed_urls = set()  # Avoid duplicate downloads
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link['href'].strip()
            
            # Skip navigation and social media links
            skip_patterns = [
                'javascript:', 'mailto:', '#',
                'facebook', 'twitter', 'linkedin', 'youtube',
                'instagram', 'sharethis'
            ]
            if any(pattern in href.lower() for pattern in skip_patterns):
                continue
            
            # Build full URL
            full_url = urljoin(base_url, href)
            
            # Skip if already processed
            if full_url in processed_urls:
                continue
                
            # Check file extension and process accordingly
            file_ext = href.lower().split('?')[0].split('#')[0]  # Remove query params
            
            if file_ext.endswith('.pdf'):
                logger.info(f"  Extracting PDF: {href.split('/')[-1]}")
                pdf_bytes = await self.fetch_binary(full_url)
                if pdf_bytes:
                    text = self.extract_pdf_text(pdf_bytes)
                    if text:
                        attachment_texts.append(f"[PDF Attachment: {href.split('/')[-1]}]\n{text}")
                        processed_urls.add(full_url)
                        
            elif file_ext.endswith(('.xlsx', '.xls')):
                logger.info(f"  Extracting Excel: {href.split('/')[-1]}")
                excel_bytes = await self.fetch_binary(full_url)
                if excel_bytes:
                    text = self.extract_excel_text(excel_bytes)
                    if text:
                        attachment_texts.append(f"[Excel Attachment: {href.split('/')[-1]}]\n{text}")
                        processed_urls.add(full_url)
                        
            elif file_ext.endswith('.csv'):
                logger.info(f"  Extracting CSV: {href.split('/')[-1]}")
                csv_bytes = await self.fetch_binary(full_url)
                if csv_bytes:
                    text = self.extract_csv_text(csv_bytes)
                    if text:
                        attachment_texts.append(f"[CSV Attachment: {href.split('/')[-1]}]\n{text}")
                        processed_urls.add(full_url)
                        
        return "\n\n".join(attachment_texts)
        
    def parse_date(self, date_str: str) -> str:
        """
        Parse date string to ISO format (YYYY-MM-DD).
        
        Args:
            date_str: Date string in format "Month DD, YYYY"
            
        Returns:
            ISO formatted date string
        """
        try:
            # Handle "October 9, 2025" format
            date_obj = datetime.strptime(date_str.strip(), "%B %d, %Y")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            # If parsing fails, return original string
            logger.warning(f"Could not parse date: {date_str}")
            return date_str
            
    async def scrape_article(self, url: str) -> Optional[Dict]:
        """
        Scrape an individual article page with all content and attachments.
        
        Args:
            url: Full URL of the article
            
        Returns:
            Dictionary containing article data, or None if failed/duplicate
        """
        # Skip if already scraped
        if url in self.existing_urls:
            logger.debug(f"Skipping duplicate: {url}")
            return None
            
        try:
            # Navigate to article page and wait for content
            await self.page.goto(url, wait_until='networkidle', timeout=60000)
            await asyncio.sleep(self.rate_limit)
            
            # Get page content
            html_content = await self.page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract headline
            h1 = soup.find('h1')
            headline = h1.get_text(strip=True) if h1 else "No Title"
            
            # Extract metadata (date and type)
            category_span = soup.find('span', class_='category')
            date_span = soup.find('span', class_='date')
            
            article_type = category_span.get_text(strip=True) if category_span else "Unknown"
            pub_date = self.parse_date(date_span.get_text(strip=True)) if date_span else ""
            
            # Extract main article text
            content_section = soup.find('section', class_='occgov-issuance-content')
            if not content_section:
                content_section = soup.find('section', class_='occgov-section__content-subsection')
                
            article_text = ""
            table_text = ""
            
            if content_section:
                # Remove noise elements
                for tag in content_section.find_all(['script', 'style', 'nav']):
                    tag.decompose()
                
                # Extract tables separately for structured formatting
                table_text = self.extract_tables_as_text(content_section)
                    
                # Extract main text
                article_text = content_section.get_text(separator='\n', strip=True)
                article_text = re.sub(r'\n\s*\n+', '\n', article_text)  # Remove blank lines
                
            else:
                # Fallback: try to get main content
                main = soup.find('main')
                if main:
                    table_text = self.extract_tables_as_text(main)
                    article_text = main.get_text(separator='\n', strip=True)
                    article_text = re.sub(r'\n\s*\n+', '\n', article_text)
            
            # Combine article text with structured table data
            full_article_text = article_text
            if table_text:
                full_article_text += "\n\n=== TABLES ===\n" + table_text
                
            # Extract topics/themes
            topics = []
            
            # Try primary topics section
            topics_section = soup.find('section', class_='occgov-section__content--topics')
            if topics_section:
                topic_links = topics_section.find_all('a', class_='topic-link')
                topics = [link.get_text(strip=True) for link in topic_links]
            
            # Fallback: check for resulttopics (from index pages)
            if not topics:
                topic_list = soup.find('ul', class_='resulttopics')
                if topic_list:
                    topic_items = topic_list.find_all('li')
                    topics = [li.get_text(strip=True) for li in topic_items]
                
            # Extract all attachments
            logger.info(f"  Checking for attachments...")
            attachment_text = await self.extract_attachments_from_html(html_content, url)
            
            # Build article data object
            article_data = {
                "headline": headline,
                "published_date": pub_date,
                "type": article_type,
                "themes": topics,
                "article_text": full_article_text,
                "attachment_text": attachment_text,
                "url": url,
                "scraped_date": datetime.now(timezone.utc).isoformat()
            }
            
            self.scraped_count += 1
            logger.info(f"✓ Scraped article #{self.scraped_count}: {headline[:60]}...")
            
            return article_data
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout scraping article: {url}")
            return None
        except Exception as e:
            logger.error(f"Error scraping article {url}: {e}")
            return None
            
    async def scrape_index_page(self, page_num: int) -> List[str]:
        """
        Scrape article URLs from a newsroom index page.
        
        Args:
            page_num: Page number (1-indexed)
            
        Returns:
            List of article URLs found on the page
        """
        # Build URL with pagination parameter
        if page_num == 1:
            url = self.NEWSROOM_URL
        else:
            stf = (page_num - 1) * 10  # Start from parameter
            url = f"{self.NEWSROOM_URL}?q=&nr=&topic=&dte=0&stf={stf}&rpp=10"
            
        logger.info(f"\nScraping index page {page_num}: {url}")
        
        try:
            # Navigate to index page
            await self.page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Wait for Angular content to load
            await self.page.wait_for_selector('.news-results', timeout=30000)
            
            # Additional wait for Angular rendering to complete
            await asyncio.sleep(3)
            
            # Extract all article links
            article_links = await self.page.query_selector_all('.news-results .focus-title')
            
            article_urls = []
            for link in article_links:
                href = await link.get_attribute('href')
                if href:
                    # Convert to absolute URL
                    full_url = urljoin(self.BASE_URL, href)
                    article_urls.append(full_url)
                    
            logger.info(f"Found {len(article_urls)} articles on page {page_num}")
            return article_urls
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout loading index page {page_num}")
            return []
        except Exception as e:
            logger.error(f"Error scraping index page {page_num}: {e}")
            return []
        
    async def scrape_all(self, max_pages: Optional[int] = None) -> List[Dict]:
        """
        Scrape all pages of the OCC newsroom.
        
        Args:
            max_pages: Maximum number of pages to scrape (None = all pages)
            
        Returns:
            List of all article dictionaries
        """
        # Load existing data for deduplication
        all_articles = self.load_existing_data()
        initial_count = len(all_articles)
        
        page_num = 1
        consecutive_empty = 0
        
        logger.info(f"\n{'='*60}")
        logger.info("Starting OCC Newsroom scrape")
        logger.info(f"Max pages: {max_pages if max_pages else 'ALL'}")
        logger.info(f"Rate limit: {self.rate_limit}s")
        logger.info(f"{'='*60}\n")
        
        while True:
            # Check if we've reached the page limit
            if max_pages and page_num > max_pages:
                logger.info(f"Reached max pages limit: {max_pages}")
                break
                
            # Scrape index page to get article URLs
            article_urls = await self.scrape_index_page(page_num)
            
            # Check if we found any articles
            if not article_urls:
                consecutive_empty += 1
                logger.warning(f"No articles found on page {page_num} (empty count: {consecutive_empty})")
                
                if consecutive_empty >= 2:
                    logger.info("Two consecutive empty pages - assuming end of results")
                    break
            else:
                consecutive_empty = 0
                
            # Scrape each article on this page
            for article_url in article_urls:
                article_data = await self.scrape_article(article_url)
                
                if article_data:
                    all_articles.append(article_data)
                    
                    # Incremental save every 10 articles
                    if len(all_articles) % 10 == 0:
                        self.save_data(all_articles)
                        logger.info(f"Progress saved: {len(all_articles)} total articles")
                        
            # Move to next page
            page_num += 1
            
            # Rate limiting between pages
            await asyncio.sleep(self.rate_limit)
            
        # Final save
        self.save_data(all_articles)
        
        new_articles = len(all_articles) - initial_count
        
        logger.info(f"\n{'='*60}")
        logger.info("Scraping Complete!")
        logger.info(f"Total articles in dataset: {len(all_articles)}")
        logger.info(f"New articles scraped: {new_articles}")
        logger.info(f"Output file: {self.output_file}")
        logger.info(f"{'='*60}\n")
        
        return all_articles


async def main():
    """Main execution function."""
    try:
        async with OCCNewsScraper(output_dir="data", rate_limit=2.0) as scraper:
            # For testing: limit to 3 pages
            # For production: remove max_pages parameter to scrape all ~557 pages
            articles = await scraper.scrape_all(max_pages=2)
            
            print(f"\n✓ Successfully scraped {len(articles)} total articles")
            print(f"✓ Output saved to: {scraper.output_file}")
            
    except KeyboardInterrupt:
        logger.info("\nScraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())