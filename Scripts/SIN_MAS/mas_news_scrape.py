"""
MAS News Scraper - Production Grade
====================================
Scrapes articles from the Monetary Authority of Singapore News section.
Designed for LLM consumption with full text extraction and deduplication.

Installation:
-------------
pip install playwright beautifulsoup4 requests pandas openpyxl pdfplumber PyPDF2
playwright install chromium

Usage:
------
python mas_news_scrape.py

Author: Production-Grade Scraper
Version: 2.0
Date: 2025-10-14
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
NEWS_URL = f"{BASE_URL}/news"
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "mas_news.json"

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

class MASScraper:
    """
    Production-grade MAS News scraper with:
    - Playwright for JavaScript-rendered pages
    - Anti-bot protection (stealth mode, random delays)
    - Deduplication (URL-based)
    - In-memory attachment processing (no file saving)
    - Incremental saves
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
        """Load existing scraped articles for deduplication"""
        if OUTPUT_FILE.exists():
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    self.existing_data = json.load(f)
                    # Build set of scraped URLs
                    self.scraped_urls = {
                        item.get('url') for item in self.existing_data 
                        if item.get('url')
                    }
                logger.info(f"✓ Loaded {len(self.existing_data)} existing articles")
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
            logger.info(f"✓ Saved {len(self.existing_data)} articles to {OUTPUT_FILE}")
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
    # LISTING PAGE EXTRACTION
    # ========================================================================
    
    def extract_article_links(self, html_content: str) -> List[Dict[str, str]]:
        """
        Extract article metadata from news listing page.
        Returns list of dicts with: url, title, type, published_date
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        articles = []
        
        # Find article cards: <article class="mas-search-card">
        article_cards = soup.find_all('article', class_='mas-search-card')
        logger.info(f"Found {len(article_cards)} article cards")
        
        for card in article_cards:
            try:
                # Extract URL from <a class="mas-link">
                link = card.find('a', class_='mas-link')
                if not link or not link.get('href'):
                    continue
                
                url = urljoin(BASE_URL, link['href'])
                
                # Skip if already scraped (deduplication)
                if url in self.scraped_urls:
                    logger.debug(f"⊗ Skip (already scraped): {url}")
                    continue
                
                # Extract title from <span class="mas-link__text">
                title_elem = link.find('span', class_='mas-link__text')
                title = title_elem.get_text(strip=True) if title_elem else ""
                
                # Extract type from <div class="mas-tag__text">
                tag = card.find('div', class_='mas-tag__text')
                article_type = tag.get_text(strip=True) if tag else ""
                
                # Extract published date from <div class="ts:xs">
                date_elem = card.find('div', class_='ts:xs')
                published_date = ""
                if date_elem:
                    published_date = date_elem.get_text(strip=True).replace('Published Date:', '').strip()
                
                articles.append({
                    'url': url,
                    'title': title,
                    'type': article_type,
                    'published_date': published_date
                })
                
            except Exception as e:
                logger.error(f"Error extracting article metadata: {e}")
                continue
        
        return articles
    
    def scrape_news_listing(self, page_num: int = 1) -> List[Dict]:
        """Scrape articles from a single news listing page"""
        url = NEWS_URL if page_num == 1 else f"{NEWS_URL}?page={page_num}"
        
        logger.info(f"📄 Scraping listing page {page_num}: {url}")
        
        # Wait for article cards to load
        html = self.fetch_page_with_playwright(
            url,
            wait_for_selectors=['.mas-search-card', '.mas-search-page__results-list']
        )
        
        if not html:
            logger.error(f"Failed to fetch page {page_num}")
            return []
        
        articles = self.extract_article_links(html)
        logger.info(f"✓ Found {len(articles)} new articles on page {page_num}")
        
        return articles
    
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
    # ARTICLE CONTENT EXTRACTION
    # ========================================================================
    
    def extract_article_content(self, url: str, html: str) -> Dict:
        """
        Extract complete article content from HTML.
        Implements ALL requirements from specification.
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'url': url,
            'scraped_date': datetime.now().isoformat(),
            'title': "",
            'published_date': "",
            'type': "",
            'sectors': [],
            'focus_areas': [],
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
            # 2. METADATA (Type, Published Date)
            # ============================================================
            ancillaries = soup.find('div', class_='mas-ancillaries')
            if ancillaries:
                # Type from <div class="mas-tag__text">
                tag = ancillaries.find('div', class_='mas-tag__text')
                if tag:
                    data['type'] = tag.get_text(strip=True)
                
                # Published date from <span>Published Date: ...</span>
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
                # 4. INTERNAL LINKS (from article content only)
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
            # 6. FOOTNOTES (always appear after "***")
            # ============================================================
            footnotes = []
            
            # Method 1: Look for rendered footnotes in the DOM
            # After Playwright renders, shadow DOM content becomes part of regular DOM
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
                # Find paragraphs with "***"
                for p in soup.find_all('p'):
                    if '***' in p.get_text():
                        # Look for footnotes after this element
                        next_elem = p.find_next_sibling()
                        while next_elem:
                            if next_elem.name == 'mas-footnote-group':
                                # Extract text from this group
                                text = next_elem.get_text(strip=True)
                                # Clean up [1], [2] prefixes
                                text = re.sub(r'^\[\d+\]\s*', '', text)
                                if text and len(text) > 10:
                                    footnotes.append(text)
                                break
                            next_elem = next_elem.find_next_sibling()
            
            if footnotes:
                data['footnotes'] = footnotes
                logger.info(f"✓ Extracted {len(footnotes)} footnotes")
            
            # ============================================================
            # 7. RESOURCES SECTION ATTACHMENTS
            # ============================================================
            for section in soup.find_all('div', class_='mas-section'):
                header = section.find('h2', class_='mas-section__title')
                if header and 'Resources' in header.get_text():
                    for link in section.find_all('a', class_='mas-link'):
                        href = link.get('href')
                        if href and any(ext in href.lower() for ext in ['.pdf', '.xlsx', '.xls', '.csv']):
                            full_url = urljoin(BASE_URL, href)
                            if not any(att['url'] == full_url for att in data['attachments']):
                                logger.info(f"📎 Resources attachment: {full_url}")
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
            
            # Check "Related News" section
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
            # 9. FOCUS AREAS / TAGS
            # ============================================================
            tags = []
            for link in soup.find_all('a', class_='mas-link'):
                href = link.get('href', '')
                if 'topics=' in href or 'focus_areas=' in href:
                    tag = link.get_text(strip=True)
                    if tag and tag not in tags:
                        tags.append(tag)
            
            if tags:
                data['focus_areas'] = tags
            
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
    
    def scrape_article(self, article_meta: Dict) -> Optional[Dict]:
        """Scrape full content from a single article page"""
        url = article_meta['url']
        logger.info(f"📰 Scraping: {url}")
        
        # Wait for article structure
        selectors = ['._mas-typeset', '.mas-layout__main--banner', '.mas-text-h1']
        html = self.fetch_page_with_playwright(url, wait_for_selectors=selectors)
        
        if not html:
            logger.error(f"Failed to fetch article: {url}")
            return None
        
        data = self.extract_article_content(url, html)
        
        # Merge metadata from listing if not found in article
        if not data['title']:
            data['title'] = article_meta.get('title', '')
        if not data['published_date']:
            data['published_date'] = article_meta.get('published_date', '')
        if not data['type']:
            data['type'] = article_meta.get('type', '')
        
        return data
    
    # ========================================================================
    # MAIN WORKFLOW
    # ========================================================================
    
    def run(self):
        """
        Main scraping workflow:
        1. Init browser
        2. Loop through pages (1 to MAX_PAGES)
        3. Extract article links from each page
        4. Scrape each article
        5. Save incrementally
        6. Close browser
        """
        logger.info("=" * 70)
        logger.info("🚀 MAS NEWS SCRAPER - STARTING")
        logger.info("=" * 70)
        logger.info(f"Max pages: {MAX_PAGES}")
        logger.info(f"Output: {OUTPUT_FILE}")
        logger.info(f"Existing articles: {len(self.existing_data)}")
        logger.info("=" * 70)
        
        try:
            self.init_browser()
            new_count = 0
            
            # Loop through pages
            for page_num in range(1, MAX_PAGES + 1):
                article_links = self.scrape_news_listing(page_num)
                
                if not article_links:
                    logger.warning(f"No new articles on page {page_num}, stopping")
                    break
                
                # Scrape each article
                for idx, meta in enumerate(article_links, 1):
                    try:
                        logger.info(f"[Page {page_num}, Article {idx}/{len(article_links)}]")
                        data = self.scrape_article(meta)
                        
                        if data:
                            self.existing_data.append(data)
                            self.scraped_urls.add(data['url'])
                            new_count += 1
                            
                            # Save every 5 articles
                            if new_count % 5 == 0:
                                self.save_data()
                                logger.info(f"💾 Checkpoint: {new_count} articles")
                        
                    except Exception as e:
                        logger.error(f"Error scraping {meta.get('url')}: {e}")
                        continue
            
            # Final save
            self.save_data()
            
            logger.info("=" * 70)
            logger.info("✅ SCRAPING COMPLETED")
            logger.info(f"New articles: {new_count}")
            logger.info(f"Total articles: {len(self.existing_data)}")
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
    scraper = MASScraper()
    scraper.run()


if __name__ == "__main__":
    main()