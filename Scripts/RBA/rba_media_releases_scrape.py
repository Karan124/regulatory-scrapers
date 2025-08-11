#!/usr/bin/env python3
"""
RBA Media Releases Scraper - Enhanced Version
Comprehensive scraper for Reserve Bank of Australia media releases with PDF extraction
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
from typing import List, Dict, Set, Iterator, Generator
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
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
JSON_PATH = os.path.join(DATA_DIR, "rba_media_releases.json")
CSV_PATH = os.path.join(DATA_DIR, "rba_media_releases.csv")
LOG_PATH = os.path.join(DATA_DIR, 'rba_media_scraper.log')
BASE_URL = "https://www.rba.gov.au"

# Configuration based on run type
RUN_TYPE = os.environ.get('RUN_TYPE', 'daily')  # 'daily' or 'initial'
IS_DAILY_RUN = RUN_TYPE == 'daily'

if IS_DAILY_RUN:
    MAX_WORKERS = 2
    TARGET_YEARS = [datetime.now().year, datetime.now().year - 1]  # Current + last year
    ARTICLE_TIMEOUT = 20
    PAGE_LOAD_TIMEOUT = 25
    MIN_DELAY = 0.5
else:
    MAX_WORKERS = 3
    TARGET_YEARS = list(range(2020, datetime.now().year + 1))  # Full range for initial run
    ARTICLE_TIMEOUT = 30
    PAGE_LOAD_TIMEOUT = 35
    MIN_DELAY = 1.0

# Enhanced headers for better compatibility
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Statistics for monitoring
stats = {
    'years_processed': 0,
    'articles_found': 0,
    'articles_enriched': 0,
    'articles_skipped': 0,
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
    logger.info(f"RBA Media Releases Scraper - {RUN_TYPE.upper()} RUN")
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
    """Simplified Chrome WebDriver setup - let system handle Chrome detection."""
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

def generate_hash_id(headline: str, published_date: str) -> str:
    """Generate unique hash ID using headline and published datetime"""
    # Normalize the data for consistent hashing
    normalized_headline = re.sub(r'\s+', ' ', headline.strip().lower())
    normalized_date = re.sub(r'\s+', ' ', published_date.strip())
    
    combined = f"{normalized_headline}|{normalized_date}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def load_existing_hash_ids() -> Set[str]:
    """Load existing hash IDs for deduplication"""
    if not os.path.exists(JSON_PATH): 
        return set()
    try:
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return {a.get("hash_id", "") for a in data if a.get("hash_id")}
    except (json.JSONDecodeError, FileNotFoundError):
        if logger:
            logger.warning("Could not read existing JSON file. Starting fresh.")
        return set()

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

def _get_element_text(soup: BeautifulSoup, selectors: List[str], default: str = "N/A") -> str:
    """Get text from first matching selector"""
    for selector in selectors:
        element = soup.select_one(selector)
        if element and element.get_text(strip=True): 
            return element.get_text(strip=True)
    return default

def extract_full_article_content(driver: WebDriver, url: str, session: requests.Session) -> Dict:
    """Extract complete article content including PDFs"""
    try:
        if logger:
            logger.debug(f"Extracting content from: {url}")
        driver.get(url)
        
        # Wait for content to load
        content_selectors = [
            '[itemprop="text"]', '.rss-mr-content', '#content', '.media-release-content',
            'article .content', '.main-content', '.page-content'
        ]
        
        try:
            WebDriverWait(driver, ARTICLE_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ", ".join(content_selectors)))
            )
        except TimeoutException:
            if logger:
                logger.warning(f"Timeout waiting for content to load: {url}")
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Extract basic metadata
        headline = _get_element_text(soup, ["h1", ".rss-mr-title", "h2.title", ".headline"])
        release_number = _get_element_text(soup, [".rss-mr-number", ".issue-name", ".release-number"])
        
        # Extract published date with multiple patterns
        published_date = ""
        date_selectors = [".rss-mr-date", "time[datetime]", ".published-date", ".date"]
        for selector in date_selectors:
            elem = soup.select_one(selector)
            if elem:
                # Try datetime attribute first
                datetime_attr = elem.get('datetime')
                if datetime_attr:
                    try:
                        # Parse ISO datetime and format consistently
                        dt = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                        published_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                        break
                    except:
                        pass
                
                # Fallback to text content
                date_text = elem.get_text(strip=True)
                if date_text and len(date_text) > 5:
                    published_date = date_text
                    break

        # Extract main web content
        web_content = ""
        for selector in content_selectors:
            body = soup.select_one(selector)
            if body and len(body.get_text(strip=True)) > 100:
                # Remove navigation and non-content elements
                for unwanted in body.find_all(['nav', 'aside', 'footer', 'header']):
                    unwanted.decompose()
                
                web_content = body.get_text(separator="\n", strip=True)
                break
        
        # Clean web content for LLM
        web_content = clean_web_content(web_content)
        
        # Extract related links
        related_links = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("/"): 
                href = BASE_URL + href
            if href.startswith("http") and href != url: 
                related_links.append(href)
        
        # Find and extract PDF content
        pdf_content = ""
        pdf_links = []
        
        # Look for PDF links in the page
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.lower().endswith('.pdf'):
                if href.startswith('/'):
                    href = BASE_URL + href
                if href.startswith('http'):
                    pdf_links.append(href)
        
        # Extract content from all PDFs found
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
            "release_number": release_number,
            "published_date": published_date,
            "web_content": web_content,
            "pdf_content": pdf_content,
            "combined_content": combined_content.strip(),
            "related_links": list(set(related_links[:50])),  # Limit to 50 links
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
            "release_number": "N/A",
            "published_date": "N/A",
            "web_content": "",
            "pdf_content": "",
            "combined_content": f"Content extraction failed: {type(e).__name__}",
            "related_links": [],
            "pdf_links": [],
            "content_length": 0,
            "pdf_content_length": 0,
            "total_content_length": 0
        }

def fetch_and_enrich_article(article_metadata: Dict, session: requests.Session) -> Dict:
    """Fetch and enrich a single article with full content and PDFs"""
    if shutdown_flag: 
        return {**article_metadata, "combined_content": "Skipped due to shutdown."}
    
    # Delay between requests
    time.sleep(random.uniform(MIN_DELAY * 0.5, MIN_DELAY * 1.5))
    url = article_metadata["url"]
    if logger:
        logger.debug(f"Enriching: {url}")
    
    try:
        with setup_driver() as driver:
            content_data = extract_full_article_content(driver, url, session)
            article_metadata.update(content_data)
            stats['articles_enriched'] += 1
            
            if logger:
                logger.info(f"Enriched: {article_metadata.get('headline', 'Unknown')} "
                           f"(Web: {content_data.get('content_length', 0)} chars, "
                           f"PDF: {content_data.get('pdf_content_length', 0)} chars)")
            
            return article_metadata
    except Exception as e:
        if logger:
            logger.warning(f"Failed to enrich {url} (Reason: {type(e).__name__})")
        stats['errors'] += 1
        article_metadata["combined_content"] = f"Content extraction failed: {type(e).__name__}"
        article_metadata["web_content"] = ""
        article_metadata["pdf_content"] = ""
    
    return article_metadata

def scrape_year_page(driver: WebDriver, year: int) -> List[Dict]:
    """Scrape articles from a specific year page"""
    url = f"{BASE_URL}/media-releases/{year}/"
    articles_metadata = []
    
    if logger:
        logger.info(f"Scraping list page for year {year}...")
    
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".list-media-releases"))
        )
    except TimeoutException:
        if logger:
            logger.warning(f"Could not find article list for year {year}. Skipping.")
        stats['errors'] += 1
        return []
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    for tag in soup.select('.list-media-releases .item'):
        htag = tag.select_one('.title a')
        if not htag:
            continue
        href = htag.get('href')
        if not href:
            continue
        
        headline = htag.text.strip()
        article_url = BASE_URL + href
        
        # Extract published date from the list page
        published_date = _get_element_text(tag, ['time[datetime]', '.date'], "Unknown")
        
        # Try to get datetime attribute for more precise date
        time_elem = tag.select_one('time[datetime]')
        if time_elem and time_elem.get('datetime'):
            try:
                dt = datetime.fromisoformat(time_elem.get('datetime').replace('Z', '+00:00'))
                published_date = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass  # Keep the text version
        
        # Generate hash using headline and published date
        hash_id = generate_hash_id(headline, published_date)
        
        articles_metadata.append({
            "hash_id": hash_id,
            "headline": headline,
            "url": article_url, 
            "year": year,
            "published_date": published_date,
            "scraped_date": datetime.now(timezone.utc).isoformat(),
        })
    
    stats['years_processed'] += 1
    stats['articles_found'] += len(articles_metadata)
    if logger:
        logger.info(f"Found {len(articles_metadata)} articles for year {year}")
    
    return articles_metadata

def save_articles(new_articles: List[Dict]) -> int:
    """Save articles to JSON and CSV files"""
    if not new_articles:
        if logger:
            logger.info("No new articles to save.")
        return 0
        
    if logger:
        logger.info(f"Saving {len(new_articles)} new articles...")
    existing_articles = []
    
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, 'r', encoding='utf-8') as f:
                existing_articles = json.load(f)
        except json.JSONDecodeError:
            if logger:
                logger.warning("JSON file is corrupted and will be overwritten.")
    
    all_articles = existing_articles + new_articles
    
    # Remove duplicates based on hash_id (final cleanup)
    unique_articles = {}
    for article in all_articles:
        hash_id = article.get('hash_id')
        if hash_id:
            unique_articles[hash_id] = article
    
    final_articles = list(unique_articles.values())
    
    # Sort by scraped_date (newest first)
    final_articles.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
    
    try:
        with open(JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_articles, f, indent=2, ensure_ascii=False)
        if logger:
            logger.info(f"Saved JSON: {len(final_articles)} total articles")
    except Exception as e:
        if logger:
            logger.error(f"Failed to save JSON: {e}")
        stats['errors'] += 1
    
    try:
        df = pd.DataFrame(final_articles)
        
        # Reorder columns for better readability
        column_order = [
            "hash_id", "year", "release_number", "headline", "url", "published_date", 
            "scraped_date", "combined_content", "web_content", "pdf_content", 
            "related_links", "pdf_links", "content_length", "pdf_content_length", 
            "total_content_length"
        ]
        
        # Only include columns that exist
        df = df.reindex(columns=[c for c in column_order if c in df.columns])
        
        # Convert lists to strings for CSV
        list_columns = ['related_links', 'pdf_links']
        for col in list_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: '|'.join(x) if isinstance(x, list) else x)
        
        df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
        if logger:
            logger.info(f"Saved CSV: {len(final_articles)} total articles")
    except Exception as e:
        if logger:
            logger.error(f"Failed to save CSV: {e}")
        stats['errors'] += 1
    
    if logger:
        logger.info(f"Save complete. Total unique articles: {len(final_articles)}")
        logger.info(f"New articles added: {len(new_articles)}")
    return len(final_articles)

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
        logger.info(f"Articles found: {stats['articles_found']}")
        logger.info(f"Articles enriched: {stats['articles_enriched']}")
        logger.info(f"Articles skipped: {stats['articles_skipped']}")
        logger.info(f"PDFs extracted: {stats['pdfs_extracted']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("="*80)
    
    # Output for orchestrator monitoring
    print(f"RBA Media Releases Scraper completed: {stats['articles_enriched']} new articles, {stats['pdfs_extracted']} PDFs")
    
    if stats['errors'] > 5:
        print("WARNINGS: Multiple errors occurred - check log file")
        return 1
    elif stats['articles_enriched'] == 0 and stats['articles_found'] > 0:
        print("No new articles enriched (all were duplicates)")
        return 0
    elif stats['articles_enriched'] == 0:
        print("No articles found")
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
    
    logger.info(f"--- Starting RBA Media Releases Scraper ({RUN_TYPE.upper()} run) ---")
    
    articles_to_enrich = []
    session = setup_requests_session()
    
    try:
        with setup_driver() as driver:
            existing_ids = load_existing_hash_ids()
            logger.info(f"Found {len(existing_ids)} existing articles to skip.")
            
            for year in TARGET_YEARS:
                if shutdown_flag: 
                    break
                
                year_articles = scrape_year_page(driver, year)
                new_articles = [
                    meta for meta in year_articles 
                    if meta["hash_id"] not in existing_ids
                ]
                
                if new_articles:
                    articles_to_enrich.extend(new_articles)
                    logger.info(f"Found {len(new_articles)} new articles for {year}.")
                else:
                    stats['articles_skipped'] += len(year_articles)
                    logger.info(f"No new articles found for {year} (all duplicates).")
                
                time.sleep(MIN_DELAY)
                
    except Exception as e:
        logger.critical(f"Fatal error during metadata collection: {e}")
        stats['errors'] += 1
        print(f"FATAL ERROR: {e}")
        return 1
    
    if not articles_to_enrich: 
        logger.info("No new articles found to enrich. Run complete.")
        save_articles([])
        return print_summary()
    
    logger.info(f"--- Starting enrichment for {len(articles_to_enrich)} articles using {MAX_WORKERS} workers ---")
    completed_articles = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_article = {
            executor.submit(fetch_and_enrich_article, meta, session): meta 
            for meta in articles_to_enrich
        }
        
        for future in as_completed(future_to_article):
            if shutdown_flag: 
                for f in future_to_article:
                    if not f.done():
                        f.cancel()
                break
                
            try:
                result = future.result()
                if result: 
                    completed_articles.append(result)
            except Exception as e:
                logger.error(f"A task generated an unhandled exception: {e}")
                stats['errors'] += 1
    
    save_articles(completed_articles)
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