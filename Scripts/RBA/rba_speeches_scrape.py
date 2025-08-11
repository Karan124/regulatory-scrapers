#!/usr/bin/env python3
"""
RBA Speeches Scraper - Enhanced Version
Comprehensive scraper for Reserve Bank of Australia speeches with PDF extraction
"""

import os
import json
import time
import hashlib
import logging
import signal
import sys
import random
import re
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional, Iterator, Generator
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import PyPDF2
from fake_useragent import UserAgent

# Disable insecure request warnings from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------
# Configuration
# ----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
JSON_PATH = os.path.join(DATA_DIR, "rba_speeches.json")
CSV_PATH = os.path.join(DATA_DIR, "rba_speeches.csv")
LOG_PATH = os.path.join(DATA_DIR, 'rba_speeches_scraper.log')
BASE_URL = "https://www.rba.gov.au"

# Configuration based on run type
RUN_TYPE = os.environ.get('RUN_TYPE', 'daily')  # 'daily' or 'initial'
IS_DAILY_RUN = RUN_TYPE == 'daily'

if IS_DAILY_RUN:
    MAX_WORKERS = 2
    TARGET_YEARS = [datetime.now().year, datetime.now().year - 1]  # Current + last year
    ARTICLE_TIMEOUT = 15
    PAGE_LOAD_TIMEOUT = 20
    MIN_DELAY = 0.5
    RETRY_ATTEMPTS = 2
else:
    MAX_WORKERS = 3
    TARGET_YEARS = list(range(2020, datetime.now().year + 1))  # Full range for initial run
    ARTICLE_TIMEOUT = 25
    PAGE_LOAD_TIMEOUT = 30
    MIN_DELAY = 1.0
    RETRY_ATTEMPTS = 3

# Enhanced headers for better compatibility
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Statistics for monitoring
stats = {
    'years_processed': 0,
    'speeches_found': 0,
    'speeches_enriched': 0,
    'speeches_skipped': 0,
    'pdfs_extracted': 0,
    'errors': 0,
    'start_time': None,
    'run_type': RUN_TYPE
}

# Global flag for graceful shutdown
shutdown_flag = False
logger = None

# ----------------------------
# Setup Logging
# ----------------------------
def setup_logging():
    """Setup logging configuration"""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Configure logging with file overwrite for daily runs
    mode = 'w' if IS_DAILY_RUN else 'a'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] - %(message)s',
        handlers=[
            logging.FileHandler(LOG_PATH, mode=mode, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    
    logger = logging.getLogger(__name__)
    
    # Log run configuration
    logger.info("="*80)
    logger.info(f"RBA Speeches Scraper - {RUN_TYPE.upper()} RUN")
    logger.info(f"Target years: {TARGET_YEARS}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Output directory: {DATA_DIR}")
    logger.info("="*80)
    
    return logger

# ----------------------------
# Signal Handlers
# ----------------------------
def signal_handler(signum, frame):
    global shutdown_flag
    if logger:
        logger.info(f"Shutdown signal {signum} received. Scraper will stop after current tasks.")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ----------------------------
# Helper Functions
# ----------------------------
def is_orchestrator_environment():
    """Check if running in orchestrator environment"""
    orchestrator_indicators = [
        'ORCHESTRATOR_ENV', 'DOCKER_CONTAINER', 'CI', 'GITHUB_ACTIONS', 
        'JENKINS_URL', 'TASKSCHEDULER'
    ]
    
    for indicator in orchestrator_indicators:
        if os.environ.get(indicator):
            return True
    
    if not os.environ.get('DISPLAY') and os.name != 'nt':
        return True
        
    return False

@contextmanager
def setup_driver() -> Generator[WebDriver, None, None]:
    """Context manager for creating and safely closing a WebDriver instance"""
    chrome_options = Options()
    
    # Essential stability options for Linux
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Updated user agent to match current Chrome version
    chrome_options.add_argument(f'--user-agent={HEADERS["User-Agent"]}')
    
    # Stealth options
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Performance optimizations
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    
    # Additional stability options
    chrome_options.add_argument("--proxy-server='direct://'")
    chrome_options.add_argument("--proxy-bypass-list=*")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument('--disable-infobars')
    chrome_options.add_argument('--disable-background-networking')
    chrome_options.add_argument('--enable-features=NetworkService,NetworkServiceInProcess')
    chrome_options.add_argument('--disable-features=TranslateUI,BlinkGenPropertyTrees')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    if is_orchestrator_environment():
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-ipc-flooding-protection")

    driver = None
    try:
        # Try to find ChromeDriver
        possible_chromedriver_paths = [
            "/usr/bin/chromedriver",
            "/usr/local/bin/chromedriver",
            "/snap/bin/chromedriver"
        ]
        
        chromedriver_path = None
        for path in possible_chromedriver_paths:
            if os.path.exists(path):
                chromedriver_path = path
                if logger:
                    logger.info(f"Found ChromeDriver at: {path}")
                break
        
        # Initialize driver with simplified service configuration
        service_kwargs = {}
        if chromedriver_path:
            service_kwargs['executable_path'] = chromedriver_path
        
        service = Service(**service_kwargs)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set timeouts
        driver.implicitly_wait(10)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        
        # Remove automation indicators
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        if logger:
            logger.info("Chrome driver initialized successfully")
        
        yield driver
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to initialize Chrome driver: {e}")
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                if logger:
                    logger.warning(f"Error closing driver: {e}")

def setup_requests_session():
    """Setup requests session for PDF downloads"""
    session = requests.Session()
    ua = UserAgent()
    session.headers.update({
        'User-Agent': ua.random,
        'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
    })
    return session

def generate_hash_id(headline: str, published_date: str, speaker: str = "") -> str:
    """Generate unique hash ID using headline, published datetime, and speaker"""
    # Normalize the data for consistent hashing
    normalized_headline = re.sub(r'\s+', ' ', headline.strip().lower())
    normalized_date = re.sub(r'\s+', ' ', published_date.strip())
    normalized_speaker = re.sub(r'\s+', ' ', speaker.strip().lower())
    
    combined = f"{normalized_headline}|{normalized_date}|{normalized_speaker}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def load_existing_hash_ids() -> Set[str]:
    """Load existing hash IDs for deduplication"""
    if not os.path.exists(JSON_PATH): 
        return set()
    try:
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return {s.get("hash_id", "") for s in data if s.get("hash_id")}
    except (json.JSONDecodeError, FileNotFoundError):
        if logger:
            logger.warning("Could not read existing JSON file. Starting fresh.")
        return set()

def normalize_date(date_str):
    """Normalize date string for consistent comparison"""
    if not date_str:
        return ""
    
    # Try to parse and reformat date for consistency
    try:
        # Handle different date formats
        date_patterns = [
            r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
            r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY or DD/MM/YYYY
            r'(\d{1,2}\s+\w+\s+\d{4})',  # DD Month YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                return match.group(1)
        
        return date_str.strip()
    except:
        return date_str.strip()

def extract_pdf_text(pdf_url: str, session: requests.Session) -> str:
    """Extract clean text from PDF for LLM processing"""
    try:
        if logger:
            logger.info(f"Extracting PDF: {pdf_url}")
        
        response = session.get(pdf_url, timeout=30)
        response.raise_for_status()
        
        pdf_file = io.BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        text = ""
        for page_num, page in enumerate(pdf_reader.pages):
            try:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
            except Exception as e:
                if logger:
                    logger.warning(f"Error extracting page {page_num} from PDF {pdf_url}: {e}")
                continue
        
        # Clean text for LLM processing
        if text:
            # Remove excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            
            # Remove unwanted characters but preserve structure
            text = re.sub(r'[^\w\s\.,;:!?\-\(\)"\'\[\]\{\}%$@#&\+\=\/\\]', '', text)
            
            # Remove page numbers and common PDF artifacts
            text = re.sub(r'\b\d+\s*$', '', text, flags=re.MULTILINE)  # Page numbers at end of lines
            text = re.sub(r'^[\d\s]*\|[\d\s]*$', '', text, flags=re.MULTILINE)  # Page separators
            
            # Remove excessive line breaks
            text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)
            
            # Final cleanup
            text = text.strip()
            
            if logger:
                logger.info(f"Successfully extracted {len(text)} characters from PDF")
            stats['pdfs_extracted'] += 1
            return text
        else:
            if logger:
                logger.warning(f"No text content found in PDF: {pdf_url}")
            return ""
            
    except Exception as e:
        if logger:
            logger.error(f"Error extracting PDF {pdf_url}: {e}")
        stats['errors'] += 1
        return ""

def clean_web_content(content: str) -> str:
    """Clean web content for LLM processing"""
    if not content:
        return ""
    
    # Remove excessive whitespace
    content = re.sub(r'\s+', ' ', content)
    
    # Remove common navigation text and artifacts
    navigation_patterns = [
        r'Skip to main content',
        r'Skip to navigation',
        r'Back to top',
        r'Print this page',
        r'Share this page',
        r'Last updated:.*?\n',
        r'Published:.*?\n',
        r'Media contact:.*?\n'
    ]
    
    for pattern in navigation_patterns:
        content = re.sub(pattern, '', content, flags=re.IGNORECASE)
    
    # Clean up remaining artifacts
    content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    content = content.strip()
    
    return content

def safe_get_text(element, default="N/A"):
    """Safely extract text from element"""
    try:
        return element.get_text(strip=True) if element else default
    except Exception:
        return default

def safe_get_attribute(element, attr, default=None):
    """Safely extract attribute from element"""
    try:
        return element.get(attr) if element else default
    except Exception:
        return default

def wait_for_element(driver, selector, timeout=10, by=By.CSS_SELECTOR):
    """Wait for element with timeout"""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, selector))
        )
        return element
    except TimeoutException:
        if logger:
            logger.warning(f"Timeout waiting for element: {selector}")
        return None

def extract_full_speech_content(driver: WebDriver, url: str, session: requests.Session) -> Dict:
    """Extract complete speech content including PDFs"""
    try:
        if logger:
            logger.debug(f"Extracting content from: {url}")
        driver.get(url)
        time.sleep(MIN_DELAY)
        
        # Wait for content to load
        content_element = wait_for_element(driver, ".content-style", timeout=ARTICLE_TIMEOUT)
        if not content_element:
            if logger:
                logger.warning(f"Content not found for {url}")
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Extract basic metadata
        headline_elem = soup.select_one(".rss-speech-title")
        headline = safe_get_text(headline_elem)

        speaker_elem = soup.select_one(".rss-speech-speaker")
        speaker = safe_get_text(speaker_elem)
        
        speaker_position_elem = soup.select_one(".rss-speech-position")
        speaker_position = safe_get_text(speaker_position_elem)

        event_elem = soup.select_one(".rss-speech-occasion")
        event = safe_get_text(event_elem)
        
        venue_elem = soup.select_one(".rss-speech-venue")
        venue = safe_get_text(venue_elem)

        # Extract published date with enhanced parsing
        published_date = ""
        date_elem = soup.select_one(".rss-speech-date time")
        if date_elem:
            # Try datetime attribute first
            datetime_attr = safe_get_attribute(date_elem, "datetime")
            if datetime_attr:
                try:
                    # Parse ISO datetime and format consistently
                    dt = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                    published_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    published_date = safe_get_text(date_elem)
            else:
                published_date = safe_get_text(date_elem)

        # Extract main web content
        web_content = ""
        content_area = soup.select_one(".content-style")
        if content_area:
            # Remove navigation and non-content elements
            for unwanted in content_area.find_all(['nav', 'aside', 'footer', 'header']):
                unwanted.decompose()
            
            # Extract all paragraph text
            paragraphs = content_area.find_all(['p', 'h2', 'h3', 'h4', 'div'])
            text_parts = []
            for p in paragraphs:
                text = safe_get_text(p)
                if text and text != "N/A" and len(text) > 10:
                    # Skip common artifacts
                    if not any(skip in text.lower() for skip in ['print this page', 'share this', 'skip to']):
                        text_parts.append(text)
            web_content = "\n\n".join(text_parts)

        # Clean web content for LLM
        web_content = clean_web_content(web_content)

        # Extract media links and identify PDFs
        media_links = []
        pdf_links = []
        
        for link in soup.select(".links a"):
            href = safe_get_attribute(link, "href")
            if href:
                full_href = BASE_URL + href if href.startswith("/") else href
                link_text = safe_get_text(link)
                
                link_type = "other"
                if "pdf" in href.lower():
                    link_type = "pdf"
                    pdf_links.append(full_href)
                elif "audio" in href.lower() or "mp3" in href.lower():
                    link_type = "audio"
                
                media_links.append({
                    "text": link_text,
                    "url": full_href,
                    "type": link_type
                })

        # Extract content from all PDFs found
        pdf_content = ""
        if pdf_links:
            if logger:
                logger.info(f"Found {len(pdf_links)} PDF(s) for {url}")
            pdf_contents = []
            
            for pdf_url in pdf_links:
                pdf_text = extract_pdf_text(pdf_url, session)
                if pdf_text:
                    pdf_contents.append(f"--- PDF Content from {pdf_url} ---\n{pdf_text}")
            
            if pdf_contents:
                pdf_content = "\n\n".join(pdf_contents)

        # Combine web and PDF content for LLM analysis
        combined_content = ""
        if web_content:
            combined_content += f"--- Web Content ---\n{web_content}\n\n"
        if pdf_content:
            combined_content += f"--- PDF Content ---\n{pdf_content}\n\n"
        
        if not combined_content:
            combined_content = "Content extraction failed - no content found"

        return {
            "headline": headline,
            "speaker": speaker,
            "speaker_position": speaker_position,
            "event": event,
            "venue": venue,
            "published_date": published_date,
            "web_content": web_content,
            "pdf_content": pdf_content,
            "combined_content": combined_content.strip(),
            "media_links": media_links,
            "pdf_links": pdf_links,
            "content_length": len(web_content),
            "pdf_content_length": len(pdf_content),
            "total_content_length": len(combined_content)
        }

    except Exception as e:
        if logger:
            logger.error(f"Content extraction failed for {url}: {e}")
        stats['errors'] += 1
        return {
            "headline": "Extraction failed",
            "speaker": "N/A",
            "speaker_position": "N/A",
            "event": "N/A",
            "venue": "N/A",
            "published_date": "N/A",
            "web_content": "",
            "pdf_content": "",
            "combined_content": f"Content extraction failed: {type(e).__name__}",
            "media_links": [],
            "pdf_links": [],
            "content_length": 0,
            "pdf_content_length": 0,
            "total_content_length": 0
        }

def process_speech(speech_tag, year, existing_ids):
    """Process individual speech from listing page"""
    try:
        # Extract headline and URL
        headline_tag = speech_tag.select_one('.rss-speech-title a')
        if not headline_tag:
            return None

        headline = safe_get_text(headline_tag)
        url = safe_get_attribute(headline_tag, 'href')
        if not url:
            return None
            
        full_url = BASE_URL + url if url.startswith("/") else url

        # Extract speaker info
        speaker_tag = speech_tag.select_one('.rss-speech-speaker')
        speaker = safe_get_text(speaker_tag)
        
        speaker_position_tag = speech_tag.select_one('.rss-speech-position')
        speaker_position = safe_get_text(speaker_position_tag)

        # Extract published date
        date_tag = speech_tag.select_one('.rss-speech-date time')
        published_date = safe_get_text(date_tag, "Unknown")
        
        # Try to get datetime attribute for more precise date
        if date_tag and date_tag.get('datetime'):
            try:
                dt = datetime.fromisoformat(date_tag.get('datetime').replace('Z', '+00:00'))
                published_date = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass  # Keep the text version

        # Extract venue
        venue_elements = speech_tag.select('.rss-speech-venue')
        venue = safe_get_text(venue_elements[0]) if venue_elements else "N/A"

        # Generate hash using headline, published date, and speaker
        normalized_date = normalize_date(published_date)
        hash_id = generate_hash_id(headline, normalized_date, speaker)

        if hash_id in existing_ids:
            if logger:
                logger.debug(f"Skipping duplicate speech: {headline[:50]}...")
            stats['speeches_skipped'] += 1
            return None

        stats['speeches_found'] += 1
        return {
            "hash_id": hash_id,
            "headline": headline,
            "url": full_url,
            "year": year,
            "speaker": speaker,
            "speaker_position": speaker_position,
            "published_date": published_date,
            "venue": venue,
            "scraped_date": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        if logger:
            logger.error(f"Error processing speech element: {e}")
        stats['errors'] += 1
        return None

def scrape_year_page(driver: WebDriver, year: int, existing_ids: Set[str]) -> List[Dict]:
    """Scrape speeches from a specific year page"""
    url = f"{BASE_URL}/speeches/{year}/"
    if logger:
        logger.info(f"Scraping year {year} from {url}")
    
    try:
        driver.get(url)
        time.sleep(MIN_DELAY)

        # Wait for speeches to load
        speeches_container = wait_for_element(driver, ".list-speeches", timeout=15)
        if not speeches_container:
            if logger:
                logger.warning(f"No speeches container found for year {year}")
            return []
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        speech_tags = soup.select('.list-speeches .item')
        
        if not speech_tags:
            if logger:
                logger.warning(f"No speech items found for year {year}")
            return []
        
        speeches_data = []
        for speech_tag in speech_tags:
            if shutdown_flag:
                break
                
            speech_data = process_speech(speech_tag, year, existing_ids)
            if speech_data:
                speeches_data.append(speech_data)

        stats['years_processed'] += 1
        if logger:
            logger.info(f"Found {len(speeches_data)} new speeches for year {year}")
        return speeches_data

    except Exception as e:
        if logger:
            logger.error(f"Error scraping year {year}: {e}")
        stats['errors'] += 1
        return []

def fetch_and_enrich_speech(speech_metadata: Dict, session: requests.Session) -> Dict:
    """Fetch and enrich a single speech with full content and PDFs"""
    if shutdown_flag: 
        return {**speech_metadata, "combined_content": "Skipped due to shutdown."}
    
    # Delay between requests
    time.sleep(random.uniform(MIN_DELAY * 0.5, MIN_DELAY * 1.5))
    url = speech_metadata["url"]
    if logger:
        logger.debug(f"Enriching: {url}")
    
    try:
        with setup_driver() as driver:
            content_data = extract_full_speech_content(driver, url, session)
            speech_metadata.update(content_data)
            stats['speeches_enriched'] += 1
            
            if logger:
                logger.info(f"Enriched: {speech_metadata.get('headline', 'Unknown')} "
                           f"(Web: {content_data.get('content_length', 0)} chars, "
                           f"PDF: {content_data.get('pdf_content_length', 0)} chars)")
            
            return speech_metadata
    except Exception as e:
        if logger:
            logger.warning(f"Failed to enrich {url} (Reason: {type(e).__name__})")
        stats['errors'] += 1
        speech_metadata["combined_content"] = f"Content extraction failed: {type(e).__name__}"
        speech_metadata["web_content"] = ""
        speech_metadata["pdf_content"] = ""
    
    return speech_metadata

def save_speeches(new_speeches: List[Dict]) -> int:
    """Save speeches to JSON and CSV files"""
    if not new_speeches:
        if logger:
            logger.info("No new speeches to save.")
        return 0
        
    if logger:
        logger.info(f"Saving {len(new_speeches)} new speeches...")
    existing_speeches = []
    
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, 'r', encoding='utf-8') as f:
                existing_speeches = json.load(f)
        except json.JSONDecodeError:
            if logger:
                logger.warning("JSON file is corrupted and will be overwritten.")
    
    all_speeches = existing_speeches + new_speeches
    
    # Remove duplicates based on hash_id (final cleanup)
    unique_speeches = {}
    for speech in all_speeches:
        hash_id = speech.get('hash_id')
        if hash_id:
            unique_speeches[hash_id] = speech
    
    final_speeches = list(unique_speeches.values())
    
    # Sort by scraped_date (newest first)
    final_speeches.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
    
    try:
        with open(JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_speeches, f, indent=2, ensure_ascii=False)
        if logger:
            logger.info(f"Saved JSON: {len(final_speeches)} total speeches")
    except Exception as e:
        if logger:
            logger.error(f"Failed to save JSON: {e}")
        stats['errors'] += 1
    
    try:
        df = pd.DataFrame(final_speeches)
        
        # Define column order for better readability
        column_order = [
            "hash_id", "year", "headline", "speaker", "speaker_position", 
            "event", "venue", "published_date", "url", "scraped_date", 
            "combined_content", "web_content", "pdf_content", "media_links", 
            "pdf_links", "content_length", "pdf_content_length", "total_content_length"
        ]
        
        # Only include columns that exist
        df = df.reindex(columns=[c for c in column_order if c in df.columns])
        
        # Convert lists to strings for CSV
        list_columns = ['media_links', 'pdf_links']
        for col in list_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
        
        df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
        if logger:
            logger.info(f"Saved CSV: {len(final_speeches)} total speeches")
    except Exception as e:
        if logger:
            logger.error(f"Failed to save CSV: {e}")
        stats['errors'] += 1
    
    if logger:
        logger.info(f"Save complete. Total unique speeches: {len(final_speeches)}")
        logger.info(f"New speeches added: {len(new_speeches)}")
    return len(final_speeches)

def print_summary() -> int:
    """Print summary of scraping results and return exit code"""
    end_time = time.time()
    duration = end_time - stats['start_time']
    
    if logger:
        logger.info("\n" + "="*80)
        logger.info("SCRAPING SUMMARY")
        logger.info("="*80)
        logger.info(f"Run type: {stats['run_type'].upper()}")
        logger.info(f"Years processed: {stats['years_processed']}")
        logger.info(f"Speeches found: {stats['speeches_found']}")
        logger.info(f"Speeches enriched: {stats['speeches_enriched']}")
        logger.info(f"Speeches skipped: {stats['speeches_skipped']}")
        logger.info(f"PDFs extracted: {stats['pdfs_extracted']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("="*80)
    
    # Output for orchestrator monitoring
    print(f"RBA Speeches Scraper completed: {stats['speeches_enriched']} new speeches, {stats['pdfs_extracted']} PDFs")
    
    if stats['errors'] > 5:
        print("WARNINGS: Multiple errors occurred - check log file")
        return 1
    elif stats['speeches_enriched'] == 0 and stats['speeches_found'] > 0:
        print("No new speeches enriched (all were duplicates)")
        return 0
    elif stats['speeches_enriched'] == 0:
        print("No speeches found")
        return 0
    else:
        print("SUCCESS")
        return 0

def main():
    """Main execution function"""
    global logger, stats
    
    stats['start_time'] = time.time()
    
    # Setup logging and data directory
    logger = setup_logging()
    os.makedirs(DATA_DIR, exist_ok=True)
    
    logger.info(f"--- Starting RBA Speeches Scraper ({RUN_TYPE.upper()} run) ---")
    
    speeches_to_enrich = []
    session = setup_requests_session()
    
    try:
        with setup_driver() as driver:
            existing_ids = load_existing_hash_ids()
            logger.info(f"Found {len(existing_ids)} existing speeches to skip.")
            
            for year in TARGET_YEARS:
                if shutdown_flag: 
                    break
                
                year_speeches = scrape_year_page(driver, year, existing_ids)
                if year_speeches:
                    speeches_to_enrich.extend(year_speeches)
                    logger.info(f"Found {len(year_speeches)} new speeches for {year}.")
                else:
                    logger.info(f"No new speeches found for {year} (all duplicates).")
                
                time.sleep(MIN_DELAY)
                
    except Exception as e:
        logger.critical(f"Fatal error during metadata collection: {e}")
        stats['errors'] += 1
        print(f"FATAL ERROR: {e}")
        return 1
    
    if not speeches_to_enrich: 
        logger.info("No new speeches found to enrich. Run complete.")
        save_speeches([])
        return print_summary()
    
    logger.info(f"--- Starting enrichment for {len(speeches_to_enrich)} speeches using {MAX_WORKERS} workers ---")
    completed_speeches = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_speech = {
            executor.submit(fetch_and_enrich_speech, meta, session): meta 
            for meta in speeches_to_enrich
        }
        
        for future in as_completed(future_to_speech):
            if shutdown_flag: 
                for f in future_to_speech:
                    if not f.done():
                        f.cancel()
                break
                
            try:
                result = future.result()
                if result: 
                    completed_speeches.append(result)
            except Exception as e:
                logger.error(f"A task generated an unhandled exception: {e}")
                stats['errors'] += 1
    
    save_speeches(completed_speeches)
    exit_code = print_summary()
    
    return exit_code

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)