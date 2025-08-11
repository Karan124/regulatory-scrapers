import os
import json
import time
import hashlib
import logging
import requests
import urllib3
from datetime import datetime, timezone
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import PyPDF2
import io
import re
try:
    from selenium_stealth import stealth
except ImportError:
    stealth = None
    logging.warning("selenium-stealth not available, using basic anti-detection")

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------
# Setup Logging
# ----------------------------
def setup_logging():
    """Setup comprehensive logging with both file and console output"""
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    # File handler
    file_handler = logging.FileHandler('data/apra_scraper.log', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

# ----------------------------
# Configuration
# ----------------------------
DATA_DIR = "data"
JSON_PATH = os.path.join(DATA_DIR, "apra_news.json")
CSV_PATH = os.path.join(DATA_DIR, "apra_news.csv")
BASE_URL = "https://www.apra.gov.au"
START_URL = "https://www.apra.gov.au/news-and-publications"

# Browser headers to mimic real user
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0"
}

# Scraper settings
MAX_PAGES = 3  # Set to None for full scrape, 3 for daily runs
PAGE_LOAD_TIMEOUT = 30
ARTICLE_TIMEOUT = 20
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2
REQUEST_DELAY = 2  # Delay between requests to be respectful

# LLM-ready validation settings
MIN_CONTENT_LENGTH = 100  # Minimum content length for LLM processing
MAX_RELATED_LINKS = 5     # Maximum related links to keep output clean
MIN_HEADLINE_LENGTH = 15  # Minimum headline length for quality

# Standard APRA boilerplate text to remove
APRA_BOILERPLATE_PATTERNS = [
    r"The Australian Prudential Regulation Authority \(APRA\) is the prudential regulator of the financial services industry\. It oversees banks, mutuals, general insurance and reinsurance companies, life insurance, private health insurers, friendly societies, and most members of the superannuation industry\. APRA currently supervises institutions holding around \$9 trillion in assets for Australian depositors, policyholders and superannuation fund members\.?\s*",
    r"APRA acknowledges the Traditional Custodians of the lands and waters of Australia and pays respect to Aboriginal and Torres Strait Islander peoples past and present\. We would like to recognise our Aboriginal and Torres Strait Islander employees who are an integral part of our workforce\.?\s*",
    r"Media enquiries\s*Contact APRA Media Unit, on\s*\+61 2 9210 3636\s*All other enquiries\s*For more information contact APRA on\s*1300 558 849\.?\s*"
]

# ----------------------------
# Helper Functions
# ----------------------------
def ensure_data_directory():
    """Create data directory if it doesn't exist"""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        logging.info(f"Data directory ensured: {DATA_DIR}")
        return True
    except Exception as e:
        logging.error(f"Failed to create data directory: {e}")
        return False

def setup_driver():
    """Setup Chrome driver with webdriver-manager for automatic driver management"""
    chrome_options = Options()
    
    # Basic options for headless operation
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Anti-detection options
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--disable-default-apps")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--allow-running-insecure-content")
    
    # Additional stability options for servers
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-features=TranslateUI")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    
    # Set user agent
    chrome_options.add_argument(f'user-agent={HEADERS["User-Agent"]}')
    
    # Experimental options to avoid detection
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Try to find Chrome binary explicitly
    possible_chrome_paths = [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable", 
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
        "/opt/google/chrome/chrome"
    ]
    
    chrome_binary = None
    for path in possible_chrome_paths:
        if os.path.exists(path):
            chrome_binary = path
            logging.info(f"Found Chrome binary at: {path}")
            break
    
    if chrome_binary:
        chrome_options.binary_location = chrome_binary
        logging.info(f"Using Chrome binary: {chrome_binary}")
    else:
        logging.warning("Chrome binary not found in standard locations, relying on system PATH")
    
    try:
        # Use webdriver-manager to automatically download and manage ChromeDriver
        logging.info("Setting up ChromeDriver with webdriver-manager...")
        service = Service(ChromeDriverManager().install())
        
        # Create driver
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Apply stealth settings if available
        if stealth:
            stealth(driver,
                    languages=["en-US", "en"],
                    vendor="Google Inc.",
                    platform="Win32",
                    webgl_vendor="Intel Inc.",
                    renderer="Intel Iris OpenGL Engine",
                    fix_hairline=True,
            )
        
        # Remove automation indicators
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        
        logging.info("Chrome driver setup completed successfully with webdriver-manager")
        return driver
        
    except Exception as e:
        logging.error(f"Failed to setup Chrome driver with webdriver-manager: {e}")
        logging.info("Trying fallback method without webdriver-manager...")
        
        # Fallback: try without webdriver-manager
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            logging.info("Chrome driver setup completed with fallback method")
            return driver
        except Exception as e2:
            logging.error(f"Fallback method also failed: {e2}")
            raise Exception(f"Both webdriver-manager and fallback methods failed. Original error: {e}, Fallback error: {e2}")

def create_session():
    """Create a requests session with proper headers"""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False  # Disable SSL verification if needed
    return session

def generate_hash_id(text: str) -> str:
    """Generate unique hash ID for deduplication"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def clean_text(text: str) -> str:
    """Clean text by removing extra whitespace and unwanted characters"""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)\[\]\"\'\/]', '', text)
    return text.strip()

def remove_apra_boilerplate(content: str) -> str:
    """Remove standard APRA boilerplate text from content"""
    if not content:
        return ""
    
    cleaned_content = content
    
    # Remove boilerplate patterns
    for pattern in APRA_BOILERPLATE_PATTERNS:
        cleaned_content = re.sub(pattern, "", cleaned_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Clean up any resulting extra whitespace
    cleaned_content = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned_content)  # Remove triple+ newlines
    cleaned_content = re.sub(r'\s+', ' ', cleaned_content)  # Normalize spaces
    cleaned_content = cleaned_content.strip()
    
    return cleaned_content

def validate_content_for_llm(content: str) -> bool:
    """Validate if content is suitable for LLM processing"""
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
    """Extract text from PDF file"""
    try:
        logging.info(f"Downloading PDF: {pdf_url}")
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
    """Extract related links from page content (excluding navigation)"""
    links = []
    try:
        # Focus on article body content only, exclude navigation areas
        content_selectors = [
            'main .content',
            'article .content', 
            '.article-body', 
            '.news-content', 
            '.page-content',
            '.text-content',
            '[class*="body-content"]'
        ]
        
        content_areas = []
        for selector in content_selectors:
            areas = soup.select(selector)
            content_areas.extend(areas)
        
        # If no specific content areas found, try main/article but clean up
        if not content_areas:
            main_areas = soup.select('main, article')
            for main_area in main_areas:
                # Create a copy to avoid modifying original
                area_copy = BeautifulSoup(str(main_area), 'html.parser')
                
                # Remove navigation elements
                nav_selectors = [
                    'nav', '.pagination', '.filter', '.breadcrumb', 
                    '.menu', '.sidebar', '.header', '.footer',
                    '[class*="nav"]', '[class*="menu"]'
                ]
                
                for nav_selector in nav_selectors:
                    for nav_elem in area_copy.select(nav_selector):
                        nav_elem.decompose()
                
                content_areas.append(area_copy)
        
        # Extract links from content areas
        for area in content_areas:
            for link in area.find_all('a', href=True):
                href = link.get('href')
                text = clean_text(link.get_text())
                
                if not href or not text or len(text) < 4:
                    continue
                
                # Skip navigation-type links
                skip_patterns = [
                    '?page=', '?industry=', '?tags=', 'filter', 'sort=',
                    'sitemap', 'accessibility', 'privacy', 'copyright', 
                    'disclaimer', 'terms', 'contact'
                ]
                
                if any(pattern in href.lower() for pattern in skip_patterns):
                    continue
                
                # Skip navigation-type text
                skip_texts = [
                    'next', 'previous', 'page', 'filter', 'show all', 
                    'clear all', 'home', 'back', 'more'
                ]
                
                if any(skip_text in text.lower() for skip_text in skip_texts):
                    continue
                
                # Convert relative URLs to absolute
                full_url = urljoin(base_url, href)
                
                # Only include meaningful APRA links
                if ('apra.gov.au' in full_url and 
                    full_url != current_url and
                    not any(ext in href.lower() for ext in ['.xlsx', '.csv', '.mp3', '.mp4', '.wav'])):
                    links.append(full_url)
    
    except Exception as e:
        logging.warning(f"Error extracting related links: {e}")
    
    # Remove duplicates and limit to reasonable number
    unique_links = list(set(links))
    limited_links = unique_links[:MAX_RELATED_LINKS]
    
    logging.debug(f"Extracted {len(limited_links)} related links from {len(unique_links)} unique links")
    return limited_links

def simulate_human_browsing(driver):
    """Simulate human-like browsing behavior"""
    try:
        logging.info("Starting human-like browsing simulation")
        
        # Go to homepage first
        driver.get(BASE_URL)
        time.sleep(2)
        
        # Scroll a bit
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(1)
        
        # Navigate to news section
        driver.get(START_URL)
        time.sleep(2)
        
        # Random scroll
        driver.execute_script("window.scrollTo(0, 300);")
        time.sleep(1)
        
        logging.info("Human-like browsing simulation completed")
        return True
        
    except Exception as e:
        logging.warning(f"Error in browsing simulation: {e}")
        return False

def load_existing_hash_ids() -> set:
    """Load existing article hash IDs for deduplication"""
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, 'r', encoding='utf-8') as f:
                articles = json.load(f)
                hash_ids = {article.get("hash_id") for article in articles if article.get("hash_id")}
                logging.info(f"Loaded {len(hash_ids)} existing hash IDs for deduplication")
                return hash_ids
        except (json.JSONDecodeError, KeyError) as e:
            logging.warning(f"Error loading existing data, starting fresh: {e}")
            return set()
    return set()

def extract_article_from_listing(link_element, base_url: str) -> Optional[Dict]:
    """Extract article metadata from a listing page link element"""
    try:
        href = link_element.get('href')
        if not href:
            return None
            
        full_url = urljoin(base_url, href)
        
        # Skip filter/navigation URLs
        if any(param in href for param in ['?industry=', '?page=', '?tags=', 'filter']):
            return None
        
        # Find the container that holds this article's information
        containers_to_try = ['article', 'div', 'li', 'section', 'td']
        container = None
        
        for container_type in containers_to_try:
            container = link_element.find_parent(container_type)
            if container:
                break
        
        if not container:
            container = link_element.parent
            
        if not container:
            return None
        
        # Extract headline from link text first
        headline = clean_text(link_element.get_text())
        
        # If link text is not descriptive, look for headline in container
        if (not headline or 
            len(headline) < MIN_HEADLINE_LENGTH or 
            headline.lower() in ['read more', 'more', 'view', 'details'] or
            headline == "News and publications"):  # Skip generic titles
            
            headline_selectors = ['h1', 'h2', 'h3', 'h4', '.title', '[class*="title"]', '[class*="headline"]']
            for selector in headline_selectors:
                headline_elem = container.select_one(selector)
                if headline_elem:
                    potential_headline = clean_text(headline_elem.get_text())
                    if (potential_headline and 
                        len(potential_headline) >= MIN_HEADLINE_LENGTH and
                        potential_headline != "News and publications"):
                        headline = potential_headline
                        break
        
        # Validate headline quality
        if (not headline or 
            len(headline) < MIN_HEADLINE_LENGTH or
            headline.lower() in ['news and publications', 'unknown']):
            logging.debug(f"Skipping article with poor headline: '{headline}' from URL: {full_url}")
            return None
        
        # Extract published date
        published_date = "Unknown"
        datetime_str = None
        
        # Look for time element with datetime
        date_element = container.select_one('time[datetime]')
        if date_element:
            published_date = clean_text(date_element.get_text())
            datetime_str = date_element.get('datetime')
        else:
            # Try other date patterns and look for date-like text
            date_patterns = ['.date', '[class*="date"]', '.published', '[class*="published"]', '.time']
            for pattern in date_patterns:
                date_elem = container.select_one(pattern)
                if date_elem:
                    date_text = clean_text(date_elem.get_text())
                    # Validate if it looks like a date
                    date_keywords = ['january', 'february', 'march', 'april', 'may', 'june', 
                                   'july', 'august', 'september', 'october', 'november', 'december',
                                   '2024', '2025', 'jan', 'feb', 'mar', 'apr', 'jun',
                                   'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
                    if any(keyword in date_text.lower() for keyword in date_keywords):
                        published_date = date_text
                        break
            
            # If still no date, look for any text that might be a date in the container
            if published_date == "Unknown":
                container_text = container.get_text()
                date_pattern = r'\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}'
                date_match = re.search(date_pattern, container_text, re.IGNORECASE)
                if date_match:
                    published_date = date_match.group()
        
        # Extract article type/category
        article_type = "Unknown"
        category_selectors = ['.category', '[class*="category"]', '.type', '[class*="type"]', '.tag', '[class*="tag"]', '.label']
        for selector in category_selectors:
            type_elem = container.select_one(selector)
            if type_elem:
                type_text = clean_text(type_elem.get_text())
                if type_text and len(type_text) < 100:  # Reasonable category length
                    article_type = type_text
                    break
        
        # Create metadata object
        metadata = {
            'headline': headline,
            'url': full_url,
            'published_date': published_date,
            'datetime': datetime_str,
            'article_type': article_type,
            'scraped_date': datetime.now(timezone.utc).isoformat()
        }
        
        # Generate hash for deduplication based on URL instead of content
        # This is more reliable for avoiding duplicates
        url_for_hash = full_url.split('?')[0]  # Remove query parameters
        metadata['hash_id'] = generate_hash_id(url_for_hash)
        
        logging.debug(f"Extracted article: '{headline}' from {full_url}")
        return metadata
        
    except Exception as e:
        logging.warning(f"Error extracting article metadata: {e}")
        return None

def extract_full_article_content(driver, session: requests.Session, url: str) -> Dict:
    """Extract full article content including PDF content if needed"""
    try:
        logging.info(f"Extracting content from: {url}")
        
        # Load page with driver
        driver.get(url)
        WebDriverWait(driver, ARTICLE_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Small delay to ensure full page load
        time.sleep(2)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        content_data = {}
        
        # Extract headline (more accurate than listing page)
        headline_selectors = ['h1', 'main h1', 'article h1', '[class*="title"] h1']
        headline = "Unknown"
        for selector in headline_selectors:
            h1_elem = soup.select_one(selector)
            if h1_elem:
                headline = clean_text(h1_elem.get_text())
                if len(headline) >= MIN_HEADLINE_LENGTH:
                    break
        
        # IMPROVED: Extract main content with better selectors for APRA site
        main_content = ""
        
        # First try APRA-specific rich-text content area
        rich_text_area = soup.select_one('.rich-text')
        if rich_text_area:
            logging.debug("Found .rich-text area, extracting content...")
            
            # Extract all text elements and preserve structure
            content_parts = []
            for elem in rich_text_area.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'blockquote']):
                if elem.name == 'li':
                    # Handle list items
                    li_text = clean_text(elem.get_text())
                    if li_text and len(li_text) > 5:
                        content_parts.append(f"• {li_text}")
                elif elem.name in ['ul', 'ol']:
                    # Skip the list container itself, we handle li items
                    continue
                else:
                    # Handle paragraphs, headers, and other elements
                    elem_text = clean_text(elem.get_text())
                    if elem_text and len(elem_text) > 10:
                        content_parts.append(elem_text)
            
            main_content = '\n\n'.join(content_parts)
            logging.debug(f"Extracted {len(main_content)} characters from .rich-text area")
        
        # Fallback to original selectors if rich-text didn't work
        if not main_content or len(main_content) < MIN_CONTENT_LENGTH:
            logging.debug("Rich-text area insufficient, trying fallback selectors...")
            
            content_selectors = [
                '[class*="content"] p',     # Paragraphs in content areas
                '[class*="body"] p',        # Paragraphs in body areas
                '[class*="text"] p',        # Paragraphs in text areas
                'main p',                   # Paragraphs in main
                'article p',                # Paragraphs in article
                '[class*="content"]',       # Full content areas as fallback
                '[class*="body"]',          # Full body areas as fallback
                'main',                     # Main element as last resort
            ]
            
            for selector in content_selectors:
                content_elements = soup.select(selector)
                if content_elements:
                    # If we're selecting paragraphs, join them
                    if 'p' in selector:
                        content_parts = [clean_text(elem.get_text()) for elem in content_elements]
                        content_parts = [part for part in content_parts if len(part) > 20]  # Filter short paragraphs
                        main_content = "\n\n".join(content_parts)
                    else:
                        # For broader selectors, take the first substantial one
                        for elem in content_elements:
                            text = clean_text(elem.get_text())
                            if len(text) > MIN_CONTENT_LENGTH:
                                main_content = text
                                break
                    
                    # Check if we have substantial content
                    if validate_content_for_llm(main_content):
                        break
        
        # Remove APRA boilerplate content
        if main_content:
            original_length = len(main_content)
            main_content = remove_apra_boilerplate(main_content)
            logging.debug(f"Content after boilerplate removal: {len(main_content)} chars (removed {original_length - len(main_content)} chars)")
        
        # Extract article type/category from full page
        category_selectors = ['.category', '[class*="category"]', '.type', '[class*="type"]', '.breadcrumb a:last-child']
        article_type = "Unknown"
        for selector in category_selectors:
            type_elem = soup.select_one(selector)
            if type_elem:
                type_text = clean_text(type_elem.get_text())
                if type_text and len(type_text) < 100:
                    article_type = type_text
                    break
        
        # Extract published date from full page
        date_element = soup.select_one('time[datetime]')
        if date_element:
            published_date = clean_text(date_element.get_text())
            datetime_str = date_element.get('datetime')
        else:
            published_date = "Unknown"
            datetime_str = None
        
        # Extract related links (only from content)
        related_links = extract_related_links(soup, BASE_URL, url)
        
        # Look for PDF links and extract content
        pdf_content = ""
        pdf_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
        
        if pdf_links:
            # Use first PDF link as specified
            first_pdf = pdf_links[0]
            pdf_url = urljoin(BASE_URL, first_pdf.get('href'))
            logging.info(f"Found PDF link: {pdf_url}")
            
            pdf_text = extract_pdf_text(pdf_url, session)
            if pdf_text and len(pdf_text) > 50:  # Only add substantial PDF content
                pdf_content = pdf_text
                main_content = f"{main_content}\n\n--- PDF Content ---\n{pdf_content}"
        
        # Extract image if available
        image_url = None
        img_selectors = ['main img[src]', 'article img[src]', '[class*="content"] img[src]']
        for selector in img_selectors:
            img_elem = soup.select_one(selector)
            if img_elem and img_elem.get('src'):
                src = img_elem.get('src')
                # Skip small icons and logos
                if not any(skip in src.lower() for skip in ['logo', 'icon', 'favicon']):
                    image_url = urljoin(BASE_URL, src)
                    break
        
        # Validate content quality
        if not validate_content_for_llm(main_content):
            logging.warning(f"Content validation failed for {url} - content may be too short or low quality")
        
        content_data = {
            'headline': headline,
            'content': main_content,
            'published_date': published_date,
            'datetime': datetime_str,
            'article_type': article_type,
            'related_links': related_links,
            'image_url': image_url,
            'has_pdf_content': bool(pdf_content),
            'content_length': len(main_content),
            'llm_ready': validate_content_for_llm(main_content)
        }
        
        logging.info(f"Successfully extracted content: {len(main_content)} characters, LLM-ready: {content_data['llm_ready']}")
        return content_data
        
    except TimeoutException:
        logging.warning(f"Timeout loading article: {url}")
        return {'error': 'Timeout'}
    except Exception as e:
        logging.error(f"Error extracting article content from {url}: {e}")
        return {'error': str(e)}

def scrape_articles_from_page(driver, existing_ids: set, page_num: int = 1) -> List[Dict]:
    """Scrape articles from a single page with improved article detection"""
    try:
        if page_num > 1:
            url = f"{START_URL}?page={page_num-1}"
        else:
            url = START_URL
            
        logging.info(f"Scraping page {page_num}: {url}")
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        time.sleep(3)  # Additional wait for dynamic content
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find article links with multiple strategies
        article_links = []
        
        # Strategy 1: Look for links that clearly go to news articles
        news_links = soup.select('a[href*="news-and-publications"]')
        for link in news_links:
            href = link.get('href')
            if (href and 
                not any(param in href for param in ['?page=', '?industry=', '?tags=', '#']) and
                href.count('/') >= 2 and 
                not href.endswith('news-and-publications')):
                article_links.append(link)
        
        # Strategy 2: Look in common article containers
        container_selectors = ['article', '.news-item', '.publication-item', '[class*="item"]', 'li']
        for selector in container_selectors:
            containers = soup.select(selector)
            for container in containers:
                link = container.select_one('a[href*="news-and-publications"]')
                if link and link not in article_links:
                    href = link.get('href')
                    if (href and 
                        not any(param in href for param in ['?page=', '?industry=', '?tags=', '#']) and
                        href.count('/') >= 2):
                        article_links.append(link)
        
        logging.info(f"Found {len(article_links)} potential article links on page {page_num}")
        
        # Debug: Log some sample URLs to understand what we're getting
        sample_urls = []
        for i, link in enumerate(article_links[:5]):
            href = link.get('href')
            if href:
                sample_urls.append(urljoin(BASE_URL, href))
        
        logging.debug(f"Sample URLs from page {page_num}: {sample_urls}")
        
        # Process each article link
        valid_articles = []
        processed_urls = set()  # Avoid duplicates
        skipped_count = 0
        
        for link in article_links:
            try:
                metadata = extract_article_from_listing(link, BASE_URL)
                if metadata:
                    # Check if we've already processed this URL
                    if metadata['url'] in processed_urls:
                        logging.debug(f"Skipping duplicate URL: {metadata['url']}")
                        continue
                    
                    # Check against existing hash IDs
                    if metadata['hash_id'] in existing_ids:
                        logging.debug(f"Skipping existing article: {metadata['headline'][:30]}...")
                        skipped_count += 1
                        continue
                    
                    valid_articles.append(metadata)
                    processed_urls.add(metadata['url'])
                    logging.debug(f"Added article: {metadata['headline'][:50]}...")
                else:
                    logging.debug(f"Failed to extract metadata from link")
                    
            except Exception as e:
                logging.warning(f"Error processing article link: {e}")
                continue
        
        logging.info(f"Found {len(valid_articles)} valid new articles on page {page_num} (skipped {skipped_count} existing)")
        
        # Log sample headlines for verification
        if valid_articles:
            sample_headlines = [article['headline'][:50] + "..." for article in valid_articles[:3]]
            logging.info(f"Sample headlines: {sample_headlines}")
        elif article_links:
            logging.warning(f"No valid articles extracted despite finding {len(article_links)} links. All may be duplicates or have poor headlines.")
        
        return valid_articles
        
    except Exception as e:
        logging.error(f"Error scraping page {page_num}: {e}")
        return []

def check_pagination(driver) -> bool:
    """Check if there are more pages to scrape"""
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Look for next page link
        next_selectors = [
            '.pagination__next a',
            'a[title*="Go to next page"]',
            'a[rel="next"]',
            '.next a',
            'a:contains("Next")'
        ]
        
        for selector in next_selectors:
            next_link = soup.select_one(selector)
            if next_link and next_link.get('href'):
                logging.debug(f"Found next page link: {next_link.get('href')}")
                return True
        
        logging.debug("No next page link found")
        return False
        
    except Exception as e:
        logging.warning(f"Error checking pagination: {e}")
        return False

def enrich_articles_with_content(driver, session: requests.Session, articles: List[Dict]) -> List[Dict]:
    """Enrich articles with full content"""
    enriched_articles = []
    failed_count = 0
    
    logging.info(f"Starting content enrichment for {len(articles)} articles")
    
    for i, article in enumerate(articles):
        try:
            logging.info(f"Processing article {i+1}/{len(articles)}: {article['headline'][:50]}...")
            
            content_data = extract_full_article_content(driver, session, article['url'])
            
            if content_data and 'error' not in content_data:
                # Merge metadata with content data
                enriched_article = {
                    **article,  # Original metadata
                    'content': content_data.get('content', ''),
                    'related_links': content_data.get('related_links', []),
                    'image_url': content_data.get('image_url'),
                    'has_pdf_content': content_data.get('has_pdf_content', False),
                    'content_length': content_data.get('content_length', 0),
                    'llm_ready': content_data.get('llm_ready', False),
                    # Override with more accurate data from full page if available
                    'headline': content_data.get('headline', article['headline']),
                    'article_type': content_data.get('article_type', article['article_type']),
                    'published_date': content_data.get('published_date', article['published_date']),
                }
                enriched_articles.append(enriched_article)
                
                if enriched_article['llm_ready']:
                    logging.info(f"✓ Successfully processed and validated for LLM: {article['headline'][:50]}...")
                else:
                    logging.warning(f"⚠ Processed but content may need review: {article['headline'][:50]}...")
            else:
                logging.warning(f"✗ Failed to extract content for {article['url']}: {content_data.get('error', 'Unknown error')}")
                # Add article with minimal content for completeness
                article['content'] = ""
                article['related_links'] = []
                article['image_url'] = None
                article['has_pdf_content'] = False
                article['content_length'] = 0
                article['llm_ready'] = False
                enriched_articles.append(article)
                failed_count += 1
            
            # Respectful delay between requests
            if i < len(articles) - 1:
                time.sleep(REQUEST_DELAY)
                
        except Exception as e:
            logging.error(f"Error processing article {article['url']}: {e}")
            # Add article with error information
            article['content'] = f"Error extracting content: {str(e)}"
            article['related_links'] = []
            article['image_url'] = None
            article['has_pdf_content'] = False
            article['content_length'] = 0
            article['llm_ready'] = False
            enriched_articles.append(article)
            failed_count += 1
    
    success_count = len(enriched_articles) - failed_count
    llm_ready_count = sum(1 for article in enriched_articles if article.get('llm_ready', False))
    
    logging.info(f"Content enrichment completed: {success_count}/{len(articles)} successful, {llm_ready_count} LLM-ready")
    
    return enriched_articles

def validate_data_quality(articles: List[Dict]) -> Dict:
    """Validate data quality for LLM consumption"""
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
            
        if article.get('headline') and article['headline'] != "Unknown" and len(article['headline']) >= MIN_HEADLINE_LENGTH:
            stats['articles_with_headlines'] += 1
            
        if article.get('published_date') and article['published_date'] != "Unknown":
            stats['articles_with_dates'] += 1
            
        if article.get('article_type') and article['article_type'] != "Unknown":
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
    """Save articles to JSON and CSV files with validation"""
    try:
        # Validate data quality before saving
        quality_stats = validate_data_quality(new_articles)
        logging.info(f"Data Quality Report: {quality_stats}")
        
        # Load existing articles
        all_articles = []
        if os.path.exists(JSON_PATH):
            try:
                with open(JSON_PATH, 'r', encoding='utf-8') as f:
                    all_articles = json.load(f)
                logging.info(f"Loaded {len(all_articles)} existing articles")
            except json.JSONDecodeError:
                logging.warning("JSON file corrupted. Starting fresh.")
                all_articles = []
        
        # Add new articles
        all_articles.extend(new_articles)
        
        # Save JSON with proper formatting for LLM consumption
        with open(JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(all_articles, f, indent=2, ensure_ascii=False, sort_keys=True)
        
        # Create DataFrame and save CSV
        df = pd.DataFrame(all_articles)
        
        # Ensure required columns exist in correct order for LLM processing
        required_columns = [
            'hash_id', 'headline', 'url', 'published_date', 'scraped_date',
            'article_type', 'content', 'related_links', 'image_url', 
            'has_pdf_content', 'content_length', 'llm_ready'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        
        # Reorder columns for optimal LLM consumption
        df = df[required_columns]
        df.to_csv(CSV_PATH, index=False, encoding='utf-8')
        
        logging.info(f"Successfully saved {len(new_articles)} new articles")
        logging.info(f"Total articles in database: {len(all_articles)}")
        logging.info(f"LLM-ready articles: {quality_stats['llm_ready_articles']}/{len(new_articles)}")
        
        # Save quality report
        quality_report_path = os.path.join(DATA_DIR, "quality_report.json")
        with open(quality_report_path, 'w', encoding='utf-8') as f:
            json.dump(quality_stats, f, indent=2)
        
        return True
        
    except Exception as e:
        logging.error(f"Error saving articles: {e}")
        return False

def run_basic_test(driver, session):
    """Run a basic test to ensure the scraper works"""
    try:
        logging.info("Running basic functionality test...")
        
        # Test page loading
        driver.get(START_URL)
        time.sleep(3)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Test article link detection
        article_links = soup.select('a[href*="news-and-publications"]')
        filtered_links = [link for link in article_links if 
                         link.get('href') and 
                         not any(param in link.get('href') for param in ['?page=', '?industry=', '?tags=']) and
                         link.get('href').count('/') >= 2 and
                         not link.get('href').endswith('news-and-publications')]
        
        logging.info(f"Test: Found {len(filtered_links)} potential article links")
        
        # Log some sample URLs for debugging
        sample_urls = [urljoin(BASE_URL, link.get('href')) for link in filtered_links[:5]]
        logging.info(f"Test: Sample URLs: {sample_urls}")
        
        if filtered_links:
            # Test metadata extraction on first few links until we find a good one
            test_metadata = None
            for i, link in enumerate(filtered_links[:10]):  # Try up to 10 links
                test_metadata = extract_article_from_listing(link, BASE_URL)
                if test_metadata and test_metadata['headline'] not in ['Unknown', 'News and publications']:
                    logging.info(f"Test: Successfully extracted metadata (attempt {i+1}) - Headline: {test_metadata['headline'][:50]}...")
                    break
            
            if test_metadata:
                # Test content extraction
                test_content = extract_full_article_content(driver, session, test_metadata['url'])
                if test_content and 'error' not in test_content:
                    logging.info(f"Test: Successfully extracted content - Length: {len(test_content.get('content', ''))} chars")
                    logging.info(f"Test: LLM-ready: {test_content.get('llm_ready', False)}")
                    
                    # Test pagination detection
                    has_pagination = check_pagination(driver)
                    logging.info(f"Test: Pagination detected: {has_pagination}")
                    
                    return True
                else:
                    logging.error(f"Test: Failed to extract content - {test_content.get('error', 'Unknown error')}")
                    return False
            else:
                logging.error("Test: Failed to extract metadata from any link")
                return False
        else:
            logging.error("Test: No article links found")
            return False
            
    except Exception as e:
        logging.error(f"Test failed: {e}")
        return False

def main():
    """Main scraping function with comprehensive testing and validation"""
    # Setup logging
    logger = setup_logging()
    logger.info("="*50)
    logger.info("Starting APRA News Scraper - LLM-Ready Version with webdriver-manager")
    logger.info("="*50)
    
    try:
        # Ensure data directory
        if not ensure_data_directory():
            raise Exception("Failed to create data directory")
        
        # Setup driver and session
        driver = setup_driver()
        session = create_session()
        
        try:
            # Run basic test first
            if not run_basic_test(driver, session):
                logging.error("Basic test failed. Check website structure or selectors.")
                return False
            
            logging.info("✓ Basic test passed. Proceeding with full scrape...")
            
            # Simulate human browsing to collect cookies
            if not simulate_human_browsing(driver):
                logging.warning("Human browsing simulation failed, continuing anyway...")
            
            # Load existing IDs for deduplication
            existing_ids = load_existing_hash_ids()
            
            # Start scraping
            all_new_articles = []
            current_page = 1
            
            while True:
                logging.info(f"Processing page {current_page}")
                
                # Scrape articles from current page
                page_articles = scrape_articles_from_page(driver, existing_ids, current_page)
                
                if not page_articles:
                    logging.info(f"No new articles found on page {current_page}")
                    
                    # Check if this is because all articles are duplicates vs no articles exist
                    # If we're on page 1 and found 0 articles, there might be an issue
                    if current_page == 1:
                        logging.warning("No articles found on page 1. This might indicate a problem with article detection.")
                        # Continue to check pagination anyway
                    
                    # Check for next page before stopping
                    has_next_page = check_pagination(driver)
                    logging.info(f"Has next page: {has_next_page}")
                    
                    if not has_next_page:
                        logging.info("No more pages to scrape")
                        break
                    elif current_page == 1:
                        # If page 1 has no new articles but there are more pages, continue
                        logging.info("Page 1 had no new articles, but more pages exist. Continuing...")
                        current_page += 1
                        time.sleep(REQUEST_DELAY)
                        continue
                    else:
                        # For subsequent pages, if no new articles found, stop
                        logging.info("No new articles on this page and we're past page 1, stopping.")
                        break
                else:
                    all_new_articles.extend(page_articles)
                
                # Check if we should continue
                if MAX_PAGES and current_page >= MAX_PAGES:
                    logging.info(f"Reached maximum pages limit: {MAX_PAGES}")
                    break
                
                # Check for next page
                if not check_pagination(driver):
                    logging.info("No more pages to scrape")
                    break
                
                current_page += 1
                time.sleep(REQUEST_DELAY)  # Respectful delay between pages
            
            if all_new_articles:
                logging.info(f"Found {len(all_new_articles)} new articles. Extracting full content...")
                
                # Enrich with full content
                enriched_articles = enrich_articles_with_content(driver, session, all_new_articles)
                
                # Save results with validation
                if save_articles(enriched_articles):
                    logging.info("✓ Scraping completed successfully!")
                    
                    # Final quality report
                    quality_stats = validate_data_quality(enriched_articles)
                    logging.info(f"Final Quality Score: {quality_stats['quality_score']}/100")
                    logging.info(f"LLM-Ready Articles: {quality_stats['llm_ready_articles']}/{quality_stats['total_articles']}")
                    
                    return True
                else:
                    logging.error("Failed to save articles")
                    return False
                
            else:
                logging.info("No new articles found.")
                return True
        
        finally:
            driver.quit()
            session.close()
            
    except Exception as e:
        logging.error(f"Critical error in main scraping process: {e}")
        return False
    
    finally:
        logging.info("="*50)
        logging.info("APRA News Scraper finished")
        logging.info("="*50)

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)