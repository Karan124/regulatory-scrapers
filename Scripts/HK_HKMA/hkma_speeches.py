#!/usr/bin/env python3
"""
HKMA Speeches Scraper
Production-grade scraper for Hong Kong Monetary Authority speeches
with anti-bot measures, incremental updates, and LLM-ready output.
Supports both English and Chinese content.
"""

import json
import logging
import hashlib
import time
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import fitz  # PyMuPDF
from pdfminer.high_level import extract_text as pdf_extract_text
import pytesseract
from PIL import Image
import io
import pandas as pd

# Configuration
CONFIG = {
    "base_url": "https://www.hkma.gov.hk",
    "speeches_url": "https://www.hkma.gov.hk/eng/news-and-media/speeches/",
    "start_date": "2025-09-01",  # Configurable: only scrape speeches on or after this date
    "output_file": "data/hkma_speeches.json",
    "delay_range": (2, 5),  # Random delay between requests (seconds)
    "max_retries": 3,
    "timeout": 30000,  # Playwright timeout in ms
    "incremental_save_interval": 10,  # Save every N speeches
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hkma_speeches_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]


class HKMASpeechesScraper:
    """Main scraper class for HKMA speeches."""
    
    def __init__(self):
        self.base_url = CONFIG["base_url"]
        self.speeches_url = CONFIG["speeches_url"]
        self.start_date = datetime.strptime(CONFIG["start_date"], "%Y-%m-%d")
        self.output_file = Path(CONFIG["output_file"])
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data and build indexes for deduplication
        self.existing_data = self._load_existing_data()
        self.existing_urls = {item["url"] for item in self.existing_data}
        self.existing_attachment_hashes = self._build_attachment_hash_set()
        
        self.session = self._create_session()
        
    def _load_existing_data(self) -> List[Dict]:
        """Load existing scraped data for deduplication."""
        if self.output_file.exists():
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded {len(data)} existing records from {self.output_file}")
                    return data
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from existing data: {e}")
                return []
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                return []
        else:
            logger.info("No existing data file found - starting fresh")
            return []
    
    def _build_attachment_hash_set(self) -> Set[str]:
        """Build a set of all existing attachment hashes for quick deduplication."""
        hashes = set()
        for speech in self.existing_data:
            for attachment in speech.get('attachments', []):
                if 'hash' in attachment:
                    hashes.add(attachment['hash'])
        logger.info(f"Built attachment hash index with {len(hashes)} entries")
        return hashes
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with browser-like headers."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,zh-HK;q=0.8,zh;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
        })
        return session
    
    def _warm_up_session(self):
        """Visit the main page to collect cookies and establish a session."""
        logger.info("Warming up session by visiting main page")
        try:
            self.session.headers['User-Agent'] = random.choice(USER_AGENTS)
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            logger.info("Session warmed up successfully")
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.warning(f"Session warmup failed (continuing anyway): {e}")
    
    def _random_delay(self):
        """Apply random delay to avoid detection."""
        delay = random.uniform(*CONFIG["delay_range"])
        logger.debug(f"Sleeping for {delay:.2f} seconds")
        time.sleep(delay)
    
    def _get_file_hash(self, content: bytes) -> str:
        """Generate SHA-256 hash for file deduplication."""
        return hashlib.sha256(content).hexdigest()
    
    def _detect_language(self, text: str) -> str:
        """Detect if text is primarily English or Chinese."""
        # Count Chinese characters (CJK Unified Ideographs)
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text[:1000]))
        # Count English letters
        english_chars = len(re.findall(r'[a-zA-Z]', text[:1000]))
        
        if chinese_chars > english_chars:
            return "Chinese"
        elif english_chars > 0:
            return "English"
        else:
            return "Unknown"
    
    def scrape_index_page(self) -> List[Dict]:
        """Scrape the speeches index page with dynamic loading."""
        logger.info("Starting to scrape speeches index page")
        speeches = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                java_script_enabled=True,
            )
            
            # Set additional headers
            context.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9,zh-HK;q=0.8,zh;q=0.7',
                'DNT': '1',
            })
            
            page = context.new_page()
            
            try:
                # Navigate to main page first to collect cookies
                logger.info("Visiting main page to establish session")
                page.goto(self.base_url, wait_until='domcontentloaded', timeout=CONFIG["timeout"])
                page.wait_for_timeout(2000)
                
                # Navigate to speeches page
                logger.info(f"Navigating to speeches page: {self.speeches_url}")
                page.goto(self.speeches_url, wait_until='domcontentloaded', timeout=CONFIG["timeout"])
                page.wait_for_timeout(3000)
                
                load_more_clicks = 0
                stop_loading = False
                
                # Load more speeches until we reach the start date
                while not stop_loading:
                    # Parse current page content
                    html = page.content()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Speeches use the same structure as press releases
                    speeches_div = soup.find('div', {'id': 'press-release-result'})
                    
                    if not speeches_div:
                        logger.warning("Speeches container not found")
                        break
                    
                    # Find all <ul> elements containing speeches
                    ul_elements = speeches_div.find_all('ul')
                    logger.info(f"Found {len(ul_elements)} speech entries on current page")
                    
                    for ul in ul_elements:
                        li_elements = ul.find_all('li')
                        
                        # Each speech has 2 <li> elements: date and link
                        if len(li_elements) >= 2:
                            date_text = li_elements[0].get_text(strip=True)
                            link_elem = li_elements[1].find('a')
                            
                            if link_elem:
                                # Parse date - HKMA format is "15 Oct 2025"
                                try:
                                    speech_date = datetime.strptime(date_text, "%d %b %Y")
                                except ValueError:
                                    logger.warning(f"Could not parse date: {date_text}")
                                    continue
                                
                                # Check if speech is before our start date
                                if speech_date < self.start_date:
                                    logger.info(f"Reached speeches before start date: {date_text} < {self.start_date.strftime('%Y-%m-%d')}")
                                    stop_loading = True
                                    break
                                
                                url = urljoin(self.base_url, link_elem['href'])
                                title = link_elem.get_text(strip=True)
                                
                                # Check if already scraped (deduplication)
                                if url in self.existing_urls:
                                    logger.debug(f"Skipping existing speech: {title}")
                                    continue
                                
                                # Add to list
                                speech_info = {
                                    'url': url,
                                    'title': title,
                                    'published_date': speech_date.strftime("%Y-%m-%d"),
                                }
                                
                                speeches.append(speech_info)
                                logger.info(f"Found new speech [{len(speeches)}]: {title} ({speech_date.strftime('%Y-%m-%d')})")
                    
                    if stop_loading:
                        logger.info("Stopping - reached date threshold")
                        break
                    
                    # Try to click "Load More" button
                    try:
                        load_more_btn = page.locator('#btn-press-release-more')
                        
                        if load_more_btn.is_visible(timeout=3000):
                            logger.info(f"Clicking 'Load More' button (click #{load_more_clicks + 1})")
                            load_more_btn.click()
                            load_more_clicks += 1
                            
                            # Wait for new content to load
                            page.wait_for_timeout(4000)
                            self._random_delay()
                        else:
                            logger.info("'Load More' button not visible - all speeches loaded")
                            break
                            
                    except PlaywrightTimeout:
                        logger.info("'Load More' button timeout - assuming all speeches loaded")
                        break
                    except Exception as e:
                        logger.warning(f"Error interacting with 'Load More' button: {e}")
                        break
                        
            except Exception as e:
                logger.error(f"Error scraping index page: {e}", exc_info=True)
            finally:
                browser.close()
        
        logger.info(f"Index page scraping complete: Found {len(speeches)} new speeches")
        return speeches
    
    def scrape_speech(self, speech_info: Dict) -> Optional[Dict]:
        """Scrape a single speech page with retry logic."""
        url = speech_info['url']
        logger.info(f"Scraping speech: {url}")
        
        for attempt in range(1, CONFIG["max_retries"] + 1):
            try:
                # Rotate user agent for each attempt
                self.session.headers['User-Agent'] = random.choice(USER_AGENTS)
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract content area
                content_area = soup.find('div', class_='template-content-area')
                if not content_area:
                    logger.warning(f"Content area not found for {url}")
                    return None
                
                # Extract all components
                body_text = self._extract_body_text(content_area)
                tables = self._extract_tables(content_area)
                internal_links = self._extract_internal_links(content_area)
                attachments = self._extract_attachments(content_area)
                
                # Detect language
                language = self._detect_language(body_text)
                
                # Infer theme/topic from title and content
                theme = self._infer_theme(speech_info['title'], body_text)
                
                # Extract speaker information if available
                speaker = self._extract_speaker(soup, speech_info['title'])
                
                speech_data = {
                    'url': url,
                    'title': speech_info['title'],
                    'published_date': speech_info['published_date'],
                    'scraped_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'language': language,
                    'speaker': speaker,
                    'theme': theme,
                    'body_text': body_text,
                    'tables': tables,
                    'internal_links': internal_links,
                    'attachments': attachments,
                }
                
                logger.info(f"Successfully scraped speech ({language}) with {len(tables)} tables and {len(attachments)} attachments")
                self._random_delay()
                return speech_data
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    logger.warning(f"403 Forbidden on attempt {attempt}/{CONFIG['max_retries']}")
                    if attempt < CONFIG["max_retries"]:
                        wait_time = random.uniform(5, 10) * attempt
                        logger.info(f"Waiting {wait_time:.1f} seconds before retry")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to scrape {url} after {CONFIG['max_retries']} attempts (403)")
                        return None
                else:
                    logger.error(f"HTTP error {e.response.status_code} scraping {url}: {e}")
                    return None
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt}/{CONFIG['max_retries']}")
                if attempt < CONFIG["max_retries"]:
                    time.sleep(random.uniform(3, 6))
                else:
                    logger.error(f"Failed to scrape {url} after {CONFIG['max_retries']} timeouts")
                    return None
                    
            except Exception as e:
                logger.error(f"Error scraping speech {url} (attempt {attempt}): {e}", exc_info=True)
                if attempt < CONFIG["max_retries"]:
                    time.sleep(random.uniform(2, 4))
                else:
                    return None
        
        return None
    
    def _extract_speaker(self, soup, title: str) -> str:
        """Extract speaker name from the page or title."""
        # Pattern 1: Look for speaker in <p> tag before template-content-area
        # Format: "Eddie Yue, Chief Executive, Hong Kong Monetary Authority"
        content_area = soup.find('div', class_='template-content-area')
        if content_area:
            # Find the previous <p> sibling
            prev_p = content_area.find_previous_sibling('p')
            if prev_p:
                speaker_text = prev_p.get_text(strip=True)
                # Check if it looks like speaker info (contains name and title)
                if any(term in speaker_text for term in ['Chief Executive', 'Deputy Chief Executive', 'Executive Director', 'Hong Kong Monetary Authority']):
                    return speaker_text
        
        # Pattern 2: "Speech by [Name]" or "Remarks by [Name]" in title
        match = re.search(r'(?:speech|remarks) by (.+?)(?:\s+at|\s+on|\s*$)', title, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Pattern 3: Look for author/speaker meta tags or specific elements
        speaker_elem = soup.find('div', class_='speaker')
        if speaker_elem:
            return speaker_elem.get_text(strip=True)
        
        # Pattern 4: Check for common HKMA officials in title
        hkma_officials = ['Chief Executive', 'Deputy Chief Executive', 'Executive Director']
        for official in hkma_officials:
            if official.lower() in title.lower():
                return official
        
        return "Unknown"
    
    def _extract_body_text(self, content_area) -> str:
        """Extract and clean body text from content area."""
        # Remove script and style elements
        for element in content_area(['script', 'style']):
            element.decompose()
        
        # Extract text from relevant elements
        paragraphs = []
        for elem in content_area.find_all(['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li']):
            text = elem.get_text(separator=' ', strip=True)
            if text and len(text) > 1:  # Skip empty or single-char strings
                paragraphs.append(text)
        
        # Join and clean
        body_text = '\n\n'.join(paragraphs)
        
        # Normalize whitespace
        body_text = re.sub(r' +', ' ', body_text)  # Multiple spaces to single
        body_text = re.sub(r'\n\n+', '\n\n', body_text)  # Multiple newlines to double
        body_text = re.sub(r'\t+', ' ', body_text)  # Tabs to spaces
        
        return body_text.strip()
    
    def _extract_tables(self, content_area) -> List[str]:
        """Extract tables as structured text."""
        tables = []
        
        for idx, table in enumerate(content_area.find_all('table')):
            try:
                # Use pandas to parse HTML table
                df = pd.read_html(str(table), header=0)[0]
                
                # Convert to clean string format
                table_text = df.to_string(index=False)
                tables.append(table_text)
                logger.debug(f"Extracted table {idx + 1} with shape {df.shape}")
                
            except Exception as e:
                logger.warning(f"Pandas failed on table {idx + 1}, using fallback: {e}")
                # Fallback: simple text extraction
                rows = []
                for tr in table.find_all('tr'):
                    cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
                    if cells:
                        rows.append(' | '.join(cells))
                
                if rows:
                    table_text = '\n'.join(rows)
                    tables.append(table_text)
        
        return tables
    
    def _extract_internal_links(self, content_area) -> List[Dict]:
        """Extract relevant internal links, excluding social media."""
        links = []
        excluded_domains = ['facebook.com', 'linkedin.com', 'twitter.com', 'x.com', 
                           'instagram.com', 'youtube.com', 'whatsapp.com']
        
        for a in content_area.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(self.base_url, href)
            parsed = urlparse(full_url)
            
            # Skip social media and external links
            if any(domain in parsed.netloc for domain in excluded_domains):
                continue
            
            # Only include HKMA internal links
            if 'hkma.gov.hk' in parsed.netloc or href.startswith('/'):
                link_text = a.get_text(strip=True)
                if link_text:  # Only add if there's actual text
                    links.append({
                        'text': link_text,
                        'url': full_url
                    })
        
        # Remove duplicates while preserving order
        seen = set()
        unique_links = []
        for link in links:
            if link['url'] not in seen:
                seen.add(link['url'])
                unique_links.append(link)
        
        return unique_links
    
    def _extract_attachments(self, content_area) -> List[Dict]:
        """Extract and process downloadable attachments."""
        attachments = []
        file_extensions = ['.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx']
        
        for a in content_area.find_all('a', href=True):
            href = a['href']
            
            # Check if it's a downloadable file
            if any(href.lower().endswith(ext) for ext in file_extensions):
                full_url = urljoin(self.base_url, href)
                
                try:
                    attachment_data = self._process_attachment(full_url)
                    if attachment_data:
                        attachments.append(attachment_data)
                except Exception as e:
                    logger.error(f"Error processing attachment {full_url}: {e}")
        
        return attachments
    
    def _process_attachment(self, url: str) -> Optional[Dict]:
        """Download and extract text from an attachment."""
        logger.info(f"Processing attachment: {url}")
        
        try:
            # Rotate user agent
            self.session.headers['User-Agent'] = random.choice(USER_AGENTS)
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            content = response.content
            file_hash = self._get_file_hash(content)
            
            # Check if already processed using hash set
            if file_hash in self.existing_attachment_hashes:
                logger.info(f"Attachment already processed (hash match): {url}")
                return None
            
            # Add to hash set immediately to prevent re-processing
            self.existing_attachment_hashes.add(file_hash)
            
            file_ext = Path(url).suffix.lower()
            extracted_text = ""
            
            # Extract based on file type
            if file_ext == '.pdf':
                extracted_text = self._extract_pdf_text(content)
            elif file_ext in ['.xlsx', '.xls']:
                extracted_text = self._extract_excel_text(content)
            elif file_ext == '.csv':
                extracted_text = self._extract_csv_text(content)
            elif file_ext in ['.doc', '.docx']:
                logger.warning(f"Word document extraction not implemented: {url}")
                extracted_text = "[Word document - extraction not implemented]"
            
            self._random_delay()
            
            return {
                'url': url,
                'hash': file_hash,
                'type': file_ext,
                'extracted_text': extracted_text.strip() if extracted_text else ""
            }
            
        except Exception as e:
            logger.error(f"Error processing attachment {url}: {e}")
            return None
    
    def _extract_pdf_text(self, content: bytes) -> str:
        """Extract text from PDF with OCR fallback for image-based PDFs."""
        text_parts = []
        
        try:
            # Try PyMuPDF first (faster and more reliable)
            pdf_document = fitz.open(stream=content, filetype="pdf")
            logger.debug(f"PDF has {pdf_document.page_count} pages")
            
            for page_num in range(pdf_document.page_count):
                page = pdf_document[page_num]
                text = page.get_text()
                
                # If no text found, try OCR (image-based PDF)
                if not text.strip():
                    logger.debug(f"Page {page_num + 1} has no text, attempting OCR")
                    try:
                        pix = page.get_pixmap(dpi=300)
                        img = Image.open(io.BytesIO(pix.tobytes()))
                        # Try both English and Chinese OCR
                        text = pytesseract.image_to_string(img, lang='eng+chi_tra+chi_sim')
                        logger.debug(f"OCR extracted {len(text)} characters from page {page_num + 1}")
                    except Exception as e:
                        logger.warning(f"OCR failed for page {page_num + 1}: {e}")
                
                if text.strip():
                    text_parts.append(text)
            
            pdf_document.close()
            
        except Exception as e:
            logger.warning(f"PyMuPDF failed, trying pdfminer: {e}")
            try:
                # Fallback to pdfminer.six
                text = pdf_extract_text(io.BytesIO(content))
                if text:
                    text_parts.append(text)
            except Exception as e2:
                logger.error(f"PDF extraction failed completely: {e2}")
        
        full_text = '\n\n'.join(text_parts)
        
        # Clean up common PDF artifacts
        full_text = re.sub(r'\s+', ' ', full_text)  # Normalize whitespace
        full_text = re.sub(r'(\w)-\s+(\w)', r'\1\2', full_text)  # Remove hyphenation
        
        return full_text.strip()
    
    def _extract_excel_text(self, content: bytes) -> str:
        """Extract text from Excel files."""
        try:
            excel_file = io.BytesIO(content)
            xls = pd.ExcelFile(excel_file)
            
            text_parts = []
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                sheet_text = f"=== Sheet: {sheet_name} ===\n{df.to_string(index=False)}"
                text_parts.append(sheet_text)
            
            return '\n\n'.join(text_parts)
            
        except Exception as e:
            logger.error(f"Error extracting Excel: {e}")
            return ""
    
    def _extract_csv_text(self, content: bytes) -> str:
        """Extract text from CSV files."""
        try:
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'gb2312', 'big5']:
                try:
                    csv_text = content.decode(encoding)
                    df = pd.read_csv(io.StringIO(csv_text))
                    return df.to_string(index=False)
                except UnicodeDecodeError:
                    continue
            
            logger.warning("Could not decode CSV with common encodings")
            return ""
            
        except Exception as e:
            logger.error(f"Error extracting CSV: {e}")
            return ""
    
    def _infer_theme(self, title: str, body: str) -> str:
        """Infer theme/topic from title and content using keyword matching."""
        text = (title + " " + body[:500]).lower()  # Use title + first 500 chars
        
        # Theme keywords (order matters - more specific first)
        themes = {
            'Banking Supervision': ['banking supervision', 'prudential', 'capital adequacy', 'liquidity', 'stress test'],
            'Monetary Policy': ['monetary policy', 'interest rate', 'base rate', 'money supply', 'inflation'],
            'Financial Stability': ['financial stability', 'systemic risk', 'macroprudential', 'crisis management'],
            'FinTech & Innovation': ['fintech', 'innovation', 'technology', 'digital', 'blockchain', 'cbdc', 'artificial intelligence', 'a.i.'],
            'Climate & Sustainable Finance': ['climate', 'sustainable', 'green finance', 'esg', 'environmental', 'carbon'],
            'International Finance': ['international', 'cross-border', 'global', 'cooperation', 'basel', 'fsb'],
            'Greater Bay Area': ['greater bay area', 'gba', 'guangdong', 'shenzhen', 'regional integration'],
            'Capital Markets': ['capital market', 'bond market', 'stock market', 'securities', 'ipo'],
            'Payment Systems': ['payment', 'fps', 'faster payment', 'rtgs', 'clearing', 'settlement'],
            'RMB Internationalisation': ['rmb', 'renminbi', 'yuan', 'internationalisation', 'offshore'],
            'Economic Outlook': ['economic outlook', 'growth', 'gdp', 'forecast', 'prospects'],
        }
        
        for theme, keywords in themes.items():
            if any(keyword in text for keyword in keywords):
                return theme
        
        return 'General'
    
    def _save_incremental(self, new_speeches: List[Dict]):
        """Save accumulated speeches incrementally (append to existing)."""
        try:
            # Combine existing + new speeches
            all_data = self.existing_data + new_speeches
            
            # Write to file
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            
            # Update existing_data reference to include newly saved speeches
            self.existing_data = all_data
            
            # Update URL set for deduplication
            for speech in new_speeches:
                self.existing_urls.add(speech['url'])
            
            logger.info(f"Incremental save: {len(new_speeches)} new speeches saved (total: {len(all_data)})")
            
        except Exception as e:
            logger.error(f"Error during incremental save: {e}", exc_info=True)
    
    def run(self):
        """Main execution method."""
        logger.info("=" * 80)
        logger.info("HKMA Speeches Scraper - Starting")
        logger.info(f"Start date filter: {CONFIG['start_date']}")
        logger.info(f"Existing records: {len(self.existing_data)}")
        logger.info("=" * 80)
        
        # Warm up the requests session
        self._warm_up_session()
        
        # Step 1: Scrape index page to get list of speeches
        speeches_to_scrape = self.scrape_index_page()
        
        if not speeches_to_scrape:
            logger.info("No new speeches to scrape - exiting")
            return
        
        logger.info(f"Will scrape {len(speeches_to_scrape)} new speeches")
        
        # Step 2: Scrape individual speeches
        successfully_scraped = []
        failed_urls = []
        
        for i, speech_info in enumerate(speeches_to_scrape, 1):
            logger.info(f"\n--- Processing speech {i}/{len(speeches_to_scrape)} ---")
            speech_data = self.scrape_speech(speech_info)
            
            if speech_data:
                successfully_scraped.append(speech_data)
                
                # Incremental save every N speeches
                if len(successfully_scraped) % CONFIG['incremental_save_interval'] == 0:
                    self._save_incremental(successfully_scraped)
                    successfully_scraped = []  # Clear after saving
            else:
                failed_urls.append(speech_info['url'])
                logger.warning(f"Failed to scrape: {speech_info['url']}")
        
        # Final save for any remaining speeches
        if successfully_scraped:
            self._save_incremental(successfully_scraped)
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("SCRAPING COMPLETED")
        logger.info(f"Total speeches in database: {len(self.existing_data)}")
        logger.info(f"Successfully scraped: {len(speeches_to_scrape) - len(failed_urls)}")
        logger.info(f"Failed: {len(failed_urls)}")
        if failed_urls:
            logger.info("Failed URLs:")
            for url in failed_urls:
                logger.info(f"  - {url}")
        logger.info("=" * 80)


def main():
    """Entry point for the scraper."""
    try:
        scraper = HKMASpeechesScraper()
        scraper.run()
    except KeyboardInterrupt:
        logger.info("\nScraper interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()