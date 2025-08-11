import os
import json
import time
import hashlib
import logging
import requests
import urllib3
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
import re
import io

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import PyPDF2

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------
# Setup Logging
# ----------------------------
def setup_logging():
    """Setup comprehensive logging with both file and console output."""
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler("data/asic_media_releases_scraper.log"),
            logging.StreamHandler()
        ]
    )

# ----------------------------
# Configuration
# ----------------------------
DATA_DIR = "data"
JSON_PATH = os.path.join(DATA_DIR, "asic_media_releases.json")
CSV_PATH = os.path.join(DATA_DIR, "asic_media_releases.csv")
BASE_URL = "https://asic.gov.au"
MEDIA_RELEASES_URL = f"{BASE_URL}/newsroom/media-releases/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
}

# --- Enhanced Configuration ---
MAX_WORKERS = 4
ARTICLE_TIMEOUT = 20
SCROLL_PAUSE = 2
MAX_SCROLLS = 25

# NEW: MAX_PAGES property for pagination control
MAX_PAGES = 3  # Set to None for full scrape, or number for limited pages

# Daily mode settings - optimized for regular runs
DAILY_MODE = True  # Set to False for full historical scrape
DAILY_SCROLL_LIMIT = 10  # Reduced scrolling for daily runs
DAILY_ARTICLE_LIMIT = 50  # Maximum articles to process in daily mode
INITIAL_RUN_LIMIT = 400  # Higher limit for initial runs when database is empty

# LLM-ready validation settings
MIN_CONTENT_LENGTH = 100
MAX_RELATED_LINKS = 10
MIN_HEADLINE_LENGTH = 15
REQUEST_DELAY = 1  # Delay between PDF downloads

# ----------------------------
# Helper Functions
# ----------------------------

def ensure_data_directory():
    """Creates the data directory if it doesn't exist."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        logging.info(f"Data directory ensured: {DATA_DIR}")
    except Exception as e:
        logging.error(f"Failed to create data directory: {e}")
        raise

def setup_driver() -> WebDriver:
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
                logging.info(f"Found ChromeDriver at: {path}")
                break
        
        # Initialize driver with simplified service configuration
        service_kwargs = {}
        if chromedriver_path:
            service_kwargs['executable_path'] = chromedriver_path
        
        service = Service(**service_kwargs)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set timeouts
        driver.implicitly_wait(10)
        driver.set_page_load_timeout(30)
        
        # Remove automation indicators
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logging.info("Chrome driver initialized successfully")
        return driver
        
    except WebDriverException as e:
        logging.error(f"Failed to initialize WebDriver: {e}")
        logging.error("Please ensure chromedriver is installed and in your PATH.")
        raise

def create_session():
    """Create a requests session for downloading PDFs."""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    return session

def generate_hash_id(text: str) -> str:
    """Generates a SHA256 hash for a given string to create a unique ID."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def clean_text(text: str) -> str:
    """Clean text by removing extra whitespace and unwanted characters."""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)\[\]\"\'\/]', '', text)
    return text.strip()

def validate_content_for_llm(content: str) -> bool:
    """Validate if content is suitable for LLM processing."""
    if not content or len(content) < MIN_CONTENT_LENGTH:
        return False
    
    # Check if content is mostly meaningful (not just navigation/boilerplate)
    meaningful_words = len([word for word in content.split() if len(word) > 3])
    total_words = len(content.split())
    
    if total_words == 0:
        return False
        
    meaningful_ratio = meaningful_words / total_words
    return meaningful_ratio > 0.3  # At least 30% meaningful words

def extract_pdf_text(pdf_url: str, session: requests.Session) -> str:
    """Extract text from PDF file."""
    try:
        logging.info(f"Downloading PDF: {pdf_url}")
        
        # Handle relative URLs
        if not pdf_url.startswith('http'):
            pdf_url = urljoin(BASE_URL, pdf_url)
        
        response = session.get(pdf_url, timeout=30)
        response.raise_for_status()
        
        # Read PDF content
        pdf_file = io.BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        text_content = []
        for page_num, page in enumerate(pdf_reader.pages):
            try:
                page_text = page.extract_text()
                if page_text:
                    text_content.append(page_text)
                    logging.debug(f"Extracted text from PDF page {page_num + 1}")
            except Exception as e:
                logging.warning(f"Error extracting text from PDF page {page_num + 1}: {e}")
                continue
        
        full_text = "\n".join(text_content)
        cleaned_text = clean_text(full_text)
        
        logging.info(f"Successfully extracted {len(cleaned_text)} characters from PDF")
        return cleaned_text
        
    except Exception as e:
        logging.error(f"Error extracting PDF text from {pdf_url}: {e}")
        return ""

def extract_related_links(soup: BeautifulSoup, base_url: str, current_url: str) -> List[str]:
    """Extract related links from article content only (not navigation)."""
    links = []
    try:
        # Focus specifically on article content areas
        content_areas = soup.select('#nh-article-body, .nh-article-content, .article-content')
        
        if not content_areas:
            # Fallback to main content area but exclude navigation
            main_content = soup.select_one('main, .main-content, #main')
            if main_content:
                # Remove navigation elements
                nav_elements = main_content.select('nav, .nav, .navigation, .menu, .sidebar, .pagination, .nh-article-tags')
                for nav_elem in nav_elements:
                    nav_elem.decompose()
                content_areas = [main_content]
        
        for area in content_areas:
            for link in area.find_all('a', href=True):
                href = link.get('href')
                text = clean_text(link.get_text())
                
                if not href or not text or len(text) < 3:
                    continue
                
                # Skip navigation-type links
                skip_patterns = [
                    'javascript:', 'mailto:', 'tel:', '#',
                    '/newsroom/?', '/newsroom/page=', '/newsroom/media-releases/?',
                    'filter', 'sort', 'search'
                ]
                
                if any(pattern in href.lower() for pattern in skip_patterns):
                    continue
                
                # Skip navigation-type text
                skip_texts = [
                    'next', 'previous', 'page', 'more', 'read more',
                    'filter', 'search', 'home', 'back', 'media releases'
                ]
                
                if any(skip_text in text.lower() for skip_text in skip_texts):
                    continue
                
                # Convert relative URLs to absolute
                full_url = urljoin(base_url, href)
                
                # Only include meaningful links (ASIC and relevant external links)
                if (full_url != current_url and
                    not any(ext in href.lower() for ext in ['.xlsx', '.csv', '.mp3', '.mp4', '.wav']) and
                    len(text) > 3):
                    links.append(full_url)
    
    except Exception as e:
        logging.warning(f"Error extracting related links: {e}")
    
    # Remove duplicates and limit to reasonable number
    unique_links = list(set(links))
    limited_links = unique_links[:MAX_RELATED_LINKS]
    
    logging.debug(f"Extracted {len(limited_links)} related links from {len(unique_links)} unique links")
    return limited_links

def load_existing_hash_ids() -> Set[str]:
    """Loads hash IDs from the existing JSON file to prevent duplicate entries."""
    if not os.path.exists(JSON_PATH):
        return set()
    try:
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            articles = json.load(f)
            hash_ids = {article.get("hash_id") for article in articles if article.get("hash_id")}
            logging.info(f"Loaded {len(hash_ids)} existing hash IDs for deduplication")
            return hash_ids
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logging.warning(f"Could not read existing JSON file: {e}. Starting fresh.")
        return set()

def load_existing_articles() -> List[Dict]:
    """Loads existing articles from JSON file."""
    if not os.path.exists(JSON_PATH):
        return []
    try:
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logging.warning("Could not read or decode existing JSON file. Starting fresh.")
        return []

def validate_data_quality(articles: List[Dict]) -> Dict:
    """Validate data quality for LLM consumption."""
    stats = {
        'total_articles': len(articles),
        'articles_with_content': 0,
        'llm_ready_articles': 0,
        'articles_with_headlines': 0,
        'articles_with_dates': 0,
        'articles_with_types': 0,
        'articles_with_related_links': 0,
        'articles_with_pdf_content': 0,
        'average_content_length': 0,
        'quality_score': 0
    }
    
    if not articles:
        return stats
    
    total_content_length = 0
    
    for article in articles:
        if article.get('content') and len(article['content']) > MIN_CONTENT_LENGTH:
            stats['articles_with_content'] += 1
            total_content_length += len(article['content'])
            
        if article.get('llm_ready'):
            stats['llm_ready_articles'] += 1
            
        if (article.get('headline') and 
            article['headline'] not in ["Unknown", "N/A"] and 
            len(article['headline']) >= MIN_HEADLINE_LENGTH):
            stats['articles_with_headlines'] += 1
            
        if article.get('published_date') and article['published_date'] not in ["Unknown", "N/A"]:
            stats['articles_with_dates'] += 1
            
        if article.get('article_type') and article['article_type'] not in ["Unknown", "N/A"]:
            stats['articles_with_types'] += 1
            
        if article.get('related_links') and len(article['related_links']) > 0:
            stats['articles_with_related_links'] += 1
            
        if article.get('has_pdf_content'):
            stats['articles_with_pdf_content'] += 1
    
    if stats['articles_with_content'] > 0:
        stats['average_content_length'] = total_content_length // stats['articles_with_content']
    
    # Calculate quality score (0-100)
    quality_factors = [
        stats['articles_with_content'] / stats['total_articles'],
        stats['articles_with_headlines'] / stats['total_articles'],
        stats['articles_with_dates'] / stats['total_articles'],
        stats['llm_ready_articles'] / stats['total_articles']
    ]
    
    stats['quality_score'] = int(sum(quality_factors) / len(quality_factors) * 100)
    
    return stats

def save_articles(new_articles: List[Dict]):
    """Saves new articles to JSON and CSV files with enhanced validation."""
    if not new_articles:
        logging.info("No new articles to save.")
        return

    try:
        # Validate data quality
        quality_stats = validate_data_quality(new_articles)
        logging.info(f"Data Quality Report: {quality_stats}")
        
        existing_articles = load_existing_articles()
        
        # Create timestamped backup if file exists
        if os.path.exists(JSON_PATH) and existing_articles:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(DATA_DIR, f"asic_media_releases_backup_{timestamp}.json")
            
            # Ensure unique backup filename
            counter = 1
            while os.path.exists(backup_path):
                backup_path = os.path.join(DATA_DIR, f"asic_media_releases_backup_{timestamp}_{counter}.json")
                counter += 1
            
            try:
                import shutil
                shutil.copy2(JSON_PATH, backup_path)
                logging.info(f"Created backup: {backup_path}")
            except Exception as backup_error:
                logging.warning(f"Could not create backup: {backup_error}")

        # Combine and deduplicate
        all_articles = existing_articles + new_articles
        
        # Remove duplicates based on hash_id
        seen_ids = set()
        unique_articles = []
        for article in all_articles:
            if article["hash_id"] not in seen_ids:
                seen_ids.add(article["hash_id"])
                unique_articles.append(article)
        
        # Sort by scraped_date (newest first)
        unique_articles.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
        
        # Save JSON with proper formatting for LLM consumption
        with open(JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(unique_articles, f, indent=2, ensure_ascii=False, sort_keys=True)

        # Create DataFrame with proper column order for LLM processing
        df = pd.DataFrame(unique_articles)
        
        # Ensure required columns exist in correct order
        required_columns = [
            'hash_id', 'headline', 'url', 'published_date', 'scraped_date',
            'article_type', 'content', 'topics', 'related_links', 'summary',
            'media_release_number', 'has_pdf_content', 'content_length', 'llm_ready'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        
        # Handle list columns for CSV
        for col in ['topics', 'related_links']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x))
        
        # Reorder columns for optimal LLM consumption
        df = df[required_columns]
        df.to_csv(CSV_PATH, index=False, encoding='utf-8')
        
        logging.info(f"Successfully saved {len(new_articles)} new media releases. Total: {len(unique_articles)}")
        logging.info(f"LLM-ready articles: {quality_stats['llm_ready_articles']}/{len(new_articles)}")
        
        # Save quality report
        quality_report_path = os.path.join(DATA_DIR, "quality_report.json")
        with open(quality_report_path, 'w', encoding='utf-8') as f:
            json.dump(quality_stats, f, indent=2)
        
        print(f"SUCCESS: Saved {len(new_articles)} new media releases. Total in database: {len(unique_articles)}")

    except Exception as e:
        logging.error(f"Error saving articles: {e}")
        print(f"ERROR: Failed to save articles: {e}")

def fetch_media_release_details(article_summary: Dict, session: requests.Session) -> Optional[Dict]:
    """
    Enhanced worker function to fetch full content for a single media release including PDF content.
    """
    driver = setup_driver()
    url = article_summary["url"]
    logging.info(f"Fetching details for media release: {url}")
    
    try:
        driver.get(url)
        # Wait for the main content to load
        WebDriverWait(driver, ARTICLE_TIMEOUT).until(
            EC.presence_of_element_located((By.ID, "nh-article-body"))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Extract media release type
        article_type = soup.select_one("span.nh-mr-type")
        article_summary["article_type"] = article_type.get_text(strip=True) if article_type else "Media Release"

        # Extract main content
        article_body = soup.select_one("#nh-article-body")
        main_content = ""
        
        if article_body:
            # Remove script and style elements
            for script in article_body(["script", "style"]):
                script.decompose()
            main_content = article_body.get_text(separator="\n", strip=True)
        
        # Extract PDF links and content
        pdf_content = ""
        pdf_links = []
        has_pdf_content = False
        
        if article_body:
            # Find PDF links in the article content
            pdf_link_elements = article_body.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
            
            for pdf_link in pdf_link_elements:
                pdf_url = pdf_link.get('href')
                if pdf_url:
                    pdf_links.append(urljoin(BASE_URL, pdf_url))
            
            # Extract text from first PDF (as specified in requirements)
            if pdf_links:
                first_pdf_url = pdf_links[0]
                logging.info(f"Found {len(pdf_links)} PDF(s), extracting content from first: {first_pdf_url}")
                
                pdf_text = extract_pdf_text(first_pdf_url, session)
                if pdf_text and len(pdf_text.strip()) > 50:  # Only add substantial PDF content
                    pdf_content = pdf_text
                    has_pdf_content = True
                    main_content = f"{main_content}\n\n--- PDF Content ---\n{pdf_content}"
                
                # Add delay between PDF downloads
                time.sleep(REQUEST_DELAY)

        # Clean and validate main content
        main_content = clean_text(main_content)
        content_length = len(main_content)
        llm_ready = validate_content_for_llm(main_content)
        
        # Extract topics/tags
        tags_container = soup.select(".nh-article-tags a.nh-list-tag")
        topics = [clean_text(tag.get_text()) for tag in tags_container if tag.get_text().strip()]

        # Extract related links (content-only, not navigation)
        related_links = extract_related_links(soup, BASE_URL, url)

        # Extract additional metadata
        meta_date = soup.select_one('.nh-article-date')
        if meta_date:
            article_summary["published_date_detailed"] = clean_text(meta_date.get_text())

        # Extract media release number if present
        mr_number = soup.select_one('.nh-mr-number')
        if mr_number:
            article_summary["media_release_number"] = clean_text(mr_number.get_text())
        else:
            article_summary["media_release_number"] = "N/A"

        # Update article summary with all extracted data
        article_summary.update({
            "content": main_content,
            "topics": topics,
            "related_links": related_links,
            "has_pdf_content": has_pdf_content,
            "content_length": content_length,
            "llm_ready": llm_ready
        })

        logging.info(f"✓ Successfully processed: {article_summary['headline'][:50]}... (LLM-ready: {llm_ready})")
        return article_summary

    except TimeoutException:
        logging.warning(f"✗ Timeout while loading: {url}")
        return None
    except Exception as e:
        logging.error(f"✗ Error fetching {url}: {e}")
        return None
    finally:
        driver.quit()

def should_stop_early(article_summaries: List[Dict], existing_ids: Set[str]) -> bool:
    """
    Determine if we should stop early based on how many existing articles we've encountered.
    This is useful for daily runs to avoid processing old content.
    """
    if not DAILY_MODE:
        return False
    
    # Check the last 10 articles - if most are existing, we've probably hit old content
    recent_articles = article_summaries[-10:] if len(article_summaries) >= 10 else article_summaries
    existing_count = sum(1 for article in recent_articles if article["hash_id"] in existing_ids)
    
    # If 80% of recent articles are existing, stop
    if len(recent_articles) >= 5 and existing_count / len(recent_articles) >= 0.8:
        logging.info(f"Early stopping: {existing_count}/{len(recent_articles)} recent articles are existing")
        return True
    
    return False

def scrape_page_with_pagination(list_driver: WebDriver, page_num: int = 1) -> List[Dict]:
    """Scrape articles from a specific page with pagination support."""
    try:
        if page_num > 1:
            # Navigate to specific page if pagination exists
            page_url = f"{MEDIA_RELEASES_URL}?page={page_num}"
            logging.info(f"Navigating to page {page_num}: {page_url}")
            list_driver.get(page_url)
            
            # Wait for content to load
            WebDriverWait(list_driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#nr-list > li"))
            )
            time.sleep(2)  # Additional wait for dynamic content
        
        # Scroll to load all articles on current page
        scroll_limit = DAILY_SCROLL_LIMIT if DAILY_MODE else MAX_SCROLLS
        prev_height = -1
        consecutive_no_change = 0
        
        for i in range(scroll_limit):
            list_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE)
            new_height = list_driver.execute_script("return document.body.scrollHeight")
            
            if new_height == prev_height:
                consecutive_no_change += 1
                if consecutive_no_change >= 3:  # Stop if no change for 3 consecutive attempts
                    logging.info(f"Page {page_num}: Scrolling complete after {i + 1} scrolls.")
                    break
            else:
                consecutive_no_change = 0
            
            prev_height = new_height
        
        # Parse articles from current page
        soup = BeautifulSoup(list_driver.page_source, "html.parser")
        articles = soup.select("#nr-list > li")
        logging.info(f"Page {page_num}: Found {len(articles)} article summaries.")

        page_articles = []
        for article in articles:
            headline_tag = article.select_one('h3 > a')
            date_tag = article.select_one('.nr-date')
            
            if not (headline_tag and date_tag and headline_tag.get('href')):
                continue

            headline = clean_text(headline_tag.text)
            published_date = clean_text(date_tag.text)
            
            # Validate headline quality
            if len(headline) < MIN_HEADLINE_LENGTH:
                continue
            
            # Use headline + date for a more reliable unique identifier
            hash_id = generate_hash_id(headline + published_date)

            # Extract summary/excerpt if available
            summary_tag = article.select_one('.nr-summary, .summary, p')
            summary = clean_text(summary_tag.text) if summary_tag else ""

            page_articles.append({
                "hash_id": hash_id,
                "headline": headline,
                "url": BASE_URL + headline_tag['href'].strip(),
                "published_date": published_date,
                "summary": summary,
                "scraped_date": datetime.now(timezone.utc).isoformat()
            })

        return page_articles
        
    except Exception as e:
        logging.error(f"Error scraping page {page_num}: {e}")
        return []

def check_for_next_page(driver: WebDriver) -> bool:
    """Check if there's a next page available."""
    try:
        # Look for pagination indicators
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Check for "Load more" button or pagination
        load_more = soup.select_one('.load-more, .pagination .next, a[title*="next"]')
        if load_more:
            return True
            
        # Check if there are pagination links
        pagination_links = soup.select('.pagination a, .pager a')
        if pagination_links:
            return True
            
        return False
        
    except Exception as e:
        logging.warning(f"Error checking for next page: {e}")
        return False

# ----------------------------
# Main Scraping Logic
# ----------------------------
def main():
    """Enhanced main function with pagination support and PDF extraction."""
    # Setup logging
    setup_logging()
    
    mode_text = "DAILY" if DAILY_MODE else "FULL"
    logging.info("="*50)
    logging.info(f"Starting Enhanced ASIC Media Releases Scraper ({mode_text} MODE)")
    logging.info("="*50)
    
    ensure_data_directory()
    session = create_session()

    # Load existing articles to check for duplicates
    existing_ids = load_existing_hash_ids()
    logging.info(f"Found {len(existing_ids)} existing media releases in database")

    # --- Phase 1: Get all media release summaries with pagination ---
    list_driver = setup_driver()
    all_article_summaries = []
    
    try:
        logging.info(f"Starting article discovery from: {MEDIA_RELEASES_URL}")
        list_driver.get(MEDIA_RELEASES_URL)
        
        # Wait for initial content
        WebDriverWait(list_driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#nr-list > li"))
        )
        
        current_page = 1
        
        while True:
            logging.info(f"Processing page {current_page}")
            
            # Scrape current page
            page_articles = scrape_page_with_pagination(list_driver, current_page)
            
            if not page_articles:
                logging.info(f"No articles found on page {current_page}")
                break
            
            all_article_summaries.extend(page_articles)
            
            # Check early stopping for daily mode
            if DAILY_MODE and should_stop_early(all_article_summaries, existing_ids):
                logging.info("Early stopping triggered - mostly existing content found")
                break
            
            # Check if we should continue
            if MAX_PAGES and current_page >= MAX_PAGES:
                logging.info(f"Reached maximum pages limit: {MAX_PAGES}")
                break
            
            # Check for next page
            if not check_for_next_page(list_driver):
                logging.info("No more pages available")
                break
            
            current_page += 1
            time.sleep(1)  # Respectful delay between pages

        logging.info(f"Article discovery complete. Found {len(all_article_summaries)} total articles across {current_page} pages.")

    except Exception as e:
        logging.error(f"Fatal error during Phase 1 (article discovery): {e}")
        return
    finally:
        list_driver.quit()
        logging.info("Phase 1 driver closed.")

    if not all_article_summaries:
        logging.warning("No article summaries found. Check if the page structure has changed.")
        return

    # --- Phase 2: Filter for new articles and fetch details with PDF extraction ---
    new_articles_to_fetch = [
        article for article in all_article_summaries 
        if article["hash_id"] not in existing_ids
    ]

    if not new_articles_to_fetch:
        logging.info("No new media releases found. Scraper run complete.")
        print("INFO: No new media releases found - database is up to date")
        return

    # Apply article limits for daily mode
    is_initial_run = len(existing_ids) == 0
    if DAILY_MODE:
        article_limit = INITIAL_RUN_LIMIT if is_initial_run else DAILY_ARTICLE_LIMIT
        if len(new_articles_to_fetch) > article_limit:
            logging.info(f"Limiting to {article_limit} articles for {'initial' if is_initial_run else 'daily'} run")
            new_articles_to_fetch = new_articles_to_fetch[:article_limit]

    logging.info(f"Found {len(new_articles_to_fetch)} new media releases to scrape.")
    print(f"Processing {len(new_articles_to_fetch)} new media releases...")
    
    completed_articles = []

    # Use ThreadPoolExecutor for parallel processing with PDF extraction
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks with session for PDF downloading
        future_to_article = {
            executor.submit(fetch_media_release_details, summary, session): summary 
            for summary in new_articles_to_fetch
        }

        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_article), 1):
            try:
                result = future.result()
                if result:
                    completed_articles.append(result)
                    logging.info(f"Completed {i}/{len(new_articles_to_fetch)}: {result['headline'][:50]}...")
                    print(f"Progress: {i}/{len(new_articles_to_fetch)} media releases processed")
            except Exception as e:
                article_url = future_to_article[future]['url']
                logging.error(f"Task for URL {article_url} failed: {e}")

    # --- Phase 3: Save results with quality validation ---
    if completed_articles:
        save_articles(completed_articles)
        
        # Final quality report
        quality_stats = validate_data_quality(completed_articles)
        logging.info(f"Final Quality Score: {quality_stats['quality_score']}/100")
        logging.info(f"LLM-Ready Articles: {quality_stats['llm_ready_articles']}/{quality_stats['total_articles']}")
        logging.info(f"Articles with PDF Content: {quality_stats['articles_with_pdf_content']}")
    
    # Close session
    session.close()
    
    # Print summary
    print("="*60)
    print("ASIC MEDIA RELEASES SCRAPING SUMMARY")
    print("="*60)
    print(f"Mode: {mode_text}")
    if DAILY_MODE and len(existing_ids) == 0:
        print("Initial run: Higher article limit applied")
    print(f"Articles found on pages: {len(all_article_summaries)}")
    print(f"New articles to process: {len(new_articles_to_fetch)}")
    print(f"Successfully processed: {len(completed_articles)}")
    print(f"Existing articles in database: {len(existing_ids)}")
    total_articles = len(existing_ids) + len(completed_articles)
    print(f"Total articles now in database: {total_articles}")
    if completed_articles:
        quality_stats = validate_data_quality(completed_articles)
        print(f"Quality Score: {quality_stats['quality_score']}/100")
        print(f"LLM-Ready Articles: {quality_stats['llm_ready_articles']}")
        print(f"Articles with PDF Content: {quality_stats['articles_with_pdf_content']}")
    print("="*60)
    
    logging.info("="*50)
    logging.info(f"Enhanced ASIC Media Releases Scraper finished ({mode_text} MODE)")
    logging.info("="*50)


if __name__ == "__main__":
    # Check command line arguments for mode selection
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--full":
            DAILY_MODE = False
            print("Running in FULL mode (complete historical scrape)")
        elif sys.argv[1] == "--daily":
            DAILY_MODE = True
            print("Running in DAILY mode (recent content only)")
        else:
            print("Usage: python script.py [--daily|--full]")
            print("Default: --daily")
    
    main()