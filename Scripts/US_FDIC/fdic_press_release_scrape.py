"""
FDIC Press Release Scraper
Production-ready script for scraping FDIC press releases with LLM-friendly output.
"""

import asyncio
import aiohttp
import hashlib
import json
import logging
import re
import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import time

# Third-party imports
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import PyPDF2
import pytesseract
from pdf2image import convert_from_bytes
from PIL import Image

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fdic_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FDICScraper:
    """Scraper for FDIC press releases with anti-bot measures and incremental updates."""
    
    BASE_URL = "https://www.fdic.gov"
    PRESS_RELEASES_URL = f"{BASE_URL}/news/press-releases"
    OUTPUT_FILE = Path("data/fdic_press_releases.json")
    
    # Social media domains to exclude
    SOCIAL_DOMAINS = {
        'facebook.com', 'twitter.com', 'x.com', 'linkedin.com', 
        'youtube.com', 'instagram.com', 'tiktok.com'
    }
    
    # Maximum pages to scrape (None = all pages)
    MAX_PAGES = 1  # Set to a number like 5 to limit scrapings
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.existing_urls: Set[str] = set()
        self.processed_pdfs: Set[str] = set()
        self.rate_limit_delay = 2  # seconds between requests
        self.max_retries = 3
        
        # Browser headers for stealth
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
        
    async def __aenter__(self):
        """Async context manager entry."""
        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=5)
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=timeout,
            connector=connector
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    def load_existing_data(self) -> List[Dict]:
        """Load existing press releases to avoid re-scraping."""
        if not self.OUTPUT_FILE.exists():
            logger.info("No existing data file found. Starting fresh.")
            return []
        
        try:
            with open(self.OUTPUT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.existing_urls = {item['url'] for item in data}
                logger.info(f"Loaded {len(data)} existing press releases.")
                return data
        except Exception as e:
            logger.error(f"Error loading existing data: {e}")
            return []
    
    def save_data(self, data: List[Dict]):
        """Save press releases to JSON file."""
        self.OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(data)} press releases to {self.OUTPUT_FILE}")
    
    async def fetch_with_retry(self, url: str, use_playwright: bool = False) -> Optional[str]:
        """Fetch URL with retry logic and exponential backoff."""
        for attempt in range(self.max_retries):
            try:
                if use_playwright:
                    return await self._fetch_with_playwright(url)
                else:
                    return await self._fetch_with_session(url)
            except Exception as e:
                wait_time = (2 ** attempt) * self.rate_limit_delay
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {url} after {self.max_retries} attempts")
                    return None
    
    async def _fetch_with_session(self, url: str) -> str:
        """Fetch URL using aiohttp session."""
        async with self.session.get(url) as response:
            if response.status == 403:
                logger.warning(f"403 Forbidden for {url}, switching to Playwright")
                return await self._fetch_with_playwright(url)
            
            response.raise_for_status()
            return await response.text()
    
    async def _fetch_with_playwright(self, url: str) -> str:
        """Fetch URL using Playwright for stealth."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=self.headers['User-Agent'],
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()
            
            # Visit root domain first for session cookies
            if not url.startswith(self.BASE_URL):
                full_url = urljoin(self.BASE_URL, url)
            else:
                full_url = url
                
            if full_url != self.BASE_URL:
                await page.goto(self.BASE_URL, wait_until='domcontentloaded')
                await asyncio.sleep(1)
            
            await page.goto(full_url, wait_until='domcontentloaded')
            content = await page.content()
            
            await browser.close()
            return content
    
    async def get_total_pages(self) -> int:
        """Determine total number of pages from pagination."""
        html = await self.fetch_with_retry(self.PRESS_RELEASES_URL)
        if not html:
            return 1
        
        soup = BeautifulSoup(html, 'html.parser')
        pagination = soup.find('nav', {'aria-label': 'Pagination'})
        
        if not pagination:
            return 1
        
        # Find the last page number
        last_page_link = pagination.find('a', {'aria-label': lambda x: x and 'Last page' in x})
        if last_page_link:
            last_page_text = last_page_link.get_text(strip=True)
            try:
                return int(last_page_text)
            except ValueError:
                pass
        
        # Fallback: find all page links
        page_links = pagination.find_all('a', {'aria-label': re.compile(r'Page \d+')})
        if page_links:
            pages = [int(re.search(r'\d+', link['aria-label']).group()) for link in page_links]
            return max(pages)
        
        return 1
    
    async def scrape_index_page(self, page_num: int) -> List[Dict]:
        """Scrape a single index page for press release links."""
        url = f"{self.PRESS_RELEASES_URL}?pg={page_num}" if page_num > 1 else self.PRESS_RELEASES_URL
        
        html = await self.fetch_with_retry(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        articles = soup.find_all('article', class_='node--news')
        
        press_releases = []
        for article in articles:
            try:
                date_elem = article.find('time', {'itemprop': 'datePublished'})
                title_elem = article.find('a', rel='bookmark')
                
                if not date_elem or not title_elem:
                    continue
                
                url = urljoin(self.BASE_URL, title_elem['href'])
                
                # Skip if already processed
                if url in self.existing_urls:
                    logger.debug(f"Skipping existing URL: {url}")
                    continue
                
                press_releases.append({
                    'headline': title_elem.get_text(strip=True),
                    'published_date': date_elem['datetime'],
                    'url': url
                })
            except Exception as e:
                logger.error(f"Error parsing article on page {page_num}: {e}")
        
        logger.info(f"Page {page_num}: Found {len(press_releases)} new press releases")
        return press_releases
    
    async def scrape_article(self, basic_info: Dict) -> Optional[Dict]:
        """Scrape full article content including attachments."""
        url = basic_info['url']
        
        logger.info(f"Scraping article: {basic_info['headline']}")
        
        html = await self.fetch_with_retry(url)
        if not html:
            return None
        
        await asyncio.sleep(self.rate_limit_delay)
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract article content
        article = soup.find('article', class_='node--news')
        if not article:
            logger.warning(f"No article content found for {url}")
            return None
        
        # Get article text including tables
        article_text = self._extract_article_text(article)
        
        # Get type/category
        joint_release = article.find('div', class_='news-joint-release')
        release_type = "Joint Release" if joint_release else "Press Release"
        
        # Extract attachments
        attachment_text = await self._extract_attachments(article, url)
        
        # Extract related links
        related_links = self._extract_related_links(article)
        
        result = {
            'headline': basic_info['headline'],
            'published_date': basic_info['published_date'],
            'type': release_type,
            'article_text': article_text,
            'attachment_text': attachment_text,
            'related_links': related_links,
            'url': url,
            'scraped_date': datetime.now(timezone.utc).isoformat()
        }
        
        return result
    
    def _extract_article_text(self, article: BeautifulSoup) -> str:
        """Extract clean text from article including tables."""
        body = article.find('div', class_='field--name-body')
        if not body:
            return ""
        
        # Extract text from paragraphs
        paragraphs = body.find_all(['p', 'li', 'h1', 'h2', 'h3', 'h4', 'h5'])
        text_parts = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
        
        # Extract tables
        tables = body.find_all('table')
        for table in tables:
            table_text = self._extract_table_text(table)
            if table_text:
                text_parts.append(f"\n[TABLE]\n{table_text}\n[/TABLE]\n")
        
        # Clean and join
        full_text = "\n\n".join(text_parts)
        return self._clean_text(full_text)
    
    def _extract_table_text(self, table: BeautifulSoup) -> str:
        """Extract text from HTML table."""
        rows = []
        for tr in table.find_all('tr'):
            cells = [td.get_text(strip=True) for td in tr.find_all(['th', 'td'])]
            if any(cells):
                rows.append(" | ".join(cells))
        return "\n".join(rows)
    
    async def _extract_attachments(self, article: BeautifulSoup, base_url: str) -> str:
        """Extract text from all attachments (PDF, Excel, CSV)."""
        attachment_texts = []
        
        # 1. Extract from dedicated attachments section
        attachments_section = article.find('fieldset', class_='news-attachments')
        if attachments_section:
            links = attachments_section.find_all('a', href=True)
            for link in links:
                href = link['href']
                full_url = urljoin(base_url, href)
                text = await self._process_attachment_link(full_url, href)
                if text:
                    attachment_texts.append(text)
        
        # 2. Extract from inline links in article body with file-link attribute
        body = article.find('div', class_='field--name-body')
        if body:
            inline_links = body.find_all('a', {'file-link': True, 'href': True})
            for link in inline_links:
                href = link['href']
                full_url = urljoin(base_url, href)
                
                # Skip if already processed from attachments section
                if any(href in text for text in attachment_texts):
                    continue
                
                text = await self._process_attachment_link(full_url, href)
                if text:
                    attachment_texts.append(text)
        
        return "\n\n".join(attachment_texts)
    
    async def _process_attachment_link(self, full_url: str, href: str) -> str:
        """Process a single attachment link and extract text."""
        # Determine file type
        if href.lower().endswith('.pdf'):
            text = await self._extract_pdf_text(full_url)
        elif href.lower().endswith(('.xlsx', '.xls')):
            text = await self._extract_excel_text(full_url)
        elif href.lower().endswith('.csv'):
            text = await self._extract_csv_text(full_url)
        else:
            return ""
        
        if text:
            return f"[ATTACHMENT: {href}]\n{text}\n[/ATTACHMENT]"
        return ""
    
    async def _extract_pdf_text(self, url: str) -> str:
        """Extract text from PDF with OCR fallback."""
        # Check for duplicate
        pdf_hash = hashlib.md5(url.encode()).hexdigest()
        if pdf_hash in self.processed_pdfs:
            logger.debug(f"Skipping duplicate PDF: {url}")
            return ""
        
        self.processed_pdfs.add(pdf_hash)
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to download PDF: {url}")
                    return ""
                
                pdf_bytes = await response.read()
            
            # Try PyPDF2 first
            text = self._extract_pdf_with_pypdf2(pdf_bytes)
            
            # If text is too short, try OCR
            if len(text.strip()) < 100:
                logger.info(f"PDF has little extractable text, trying OCR: {url}")
                ocr_text = self._extract_pdf_with_ocr(pdf_bytes)
                if len(ocr_text) > len(text):
                    text = ocr_text
            
            return self._clean_text(text)
        
        except Exception as e:
            logger.error(f"Error extracting PDF {url}: {e}")
            return ""
    
    def _extract_pdf_with_pypdf2(self, pdf_bytes: bytes) -> str:
        """Extract text using PyPDF2."""
        try:
            pdf_file = io.BytesIO(pdf_bytes)
            reader = PyPDF2.PdfReader(pdf_file)
            
            text_parts = []
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            
            return "\n\n".join(text_parts)
        except Exception as e:
            logger.error(f"PyPDF2 extraction failed: {e}")
            return ""
    
    def _extract_pdf_with_ocr(self, pdf_bytes: bytes) -> str:
        """Extract text using OCR (pytesseract)."""
        try:
            images = convert_from_bytes(pdf_bytes)
            text_parts = []
            
            for i, image in enumerate(images):
                text = pytesseract.image_to_string(image)
                if text.strip():
                    text_parts.append(text)
            
            return "\n\n".join(text_parts)
        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")
            return ""
    
    async def _extract_excel_text(self, url: str) -> str:
        """Extract text from Excel file."""
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return ""
                
                excel_bytes = await response.read()
            
            # Read all sheets
            excel_file = io.BytesIO(excel_bytes)
            xls = pd.ExcelFile(excel_file)
            
            text_parts = []
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                
                # Convert to text
                sheet_text = f"[SHEET: {sheet_name}]\n"
                sheet_text += df.to_string(index=False, na_rep='')
                text_parts.append(sheet_text)
            
            return self._clean_text("\n\n".join(text_parts))
        
        except Exception as e:
            logger.error(f"Error extracting Excel {url}: {e}")
            return ""
    
    async def _extract_csv_text(self, url: str) -> str:
        """Extract text from CSV file."""
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return ""
                
                csv_text = await response.text()
            
            # Parse and reformat
            df = pd.read_csv(io.StringIO(csv_text))
            return self._clean_text(df.to_string(index=False, na_rep=''))
        
        except Exception as e:
            logger.error(f"Error extracting CSV {url}: {e}")
            return ""
    
    def _extract_related_links(self, article: BeautifulSoup) -> List[str]:
        """Extract relevant internal and external links from article body, excluding social media."""
        body = article.find('div', class_='field--name-body')
        if not body:
            return []
        
        links = body.find_all('a', href=True)
        related_links = []
        
        for link in links:
            href = link['href']
            full_url = urljoin(self.BASE_URL, href)
            
            # Parse domain
            parsed = urlparse(full_url)
            
            # Skip social media
            if any(social in parsed.netloc for social in self.SOCIAL_DOMAINS):
                continue
            
            # Skip base URL without path
            if full_url == self.BASE_URL or full_url == f"{self.BASE_URL}/":
                continue
            
            # Skip file attachments (they're in attachment_text)
            if href.lower().endswith(('.pdf', '.xlsx', '.xls', '.csv')):
                continue
            
            # Include both FDIC links and external regulatory/government links
            if full_url not in related_links:
                related_links.append(full_url)
        
        return related_links
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text for LLM consumption."""
        # Remove multiple whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        # Normalize line breaks
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        
        return text.strip()
    
    async def run(self):
        """Main execution method."""
        logger.info("Starting FDIC Press Release Scraper")
        
        # Load existing data
        existing_data = self.load_existing_data()
        
        # Get total pages
        total_pages = await self.get_total_pages()
        
        # Apply MAX_PAGES limit if set
        if self.MAX_PAGES is not None:
            total_pages = min(total_pages, self.MAX_PAGES)
            logger.info(f"MAX_PAGES limit applied. Scraping {total_pages} pages (out of available pages)")
        else:
            logger.info(f"Total pages to scrape: {total_pages}")
        
        # Scrape index pages
        all_basic_info = []
        for page_num in range(1, total_pages + 1):
            press_releases = await self.scrape_index_page(page_num)
            all_basic_info.extend(press_releases)
            await asyncio.sleep(self.rate_limit_delay)
        
        logger.info(f"Found {len(all_basic_info)} new press releases to scrape")
        
        # Scrape full articles
        new_articles = []
        for i, basic_info in enumerate(all_basic_info, 1):
            logger.info(f"Processing article {i}/{len(all_basic_info)}")
            article = await self.scrape_article(basic_info)
            if article:
                new_articles.append(article)
        
        # Combine with existing data
        all_data = existing_data + new_articles
        
        # Save
        self.save_data(all_data)
        
        logger.info(f"Scraping complete. Total articles: {len(all_data)}")
        logger.info(f"New articles added: {len(new_articles)}")


async def main():
    """Entry point for the scraper."""
    async with FDICScraper() as scraper:
        await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())