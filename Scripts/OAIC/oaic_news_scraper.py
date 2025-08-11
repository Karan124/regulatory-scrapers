#!/usr/bin/env python3
"""
Enhanced OAIC Selenium Scraper - Simplified and Fixed
Extracts both article metadata and full article content from all pages
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
import subprocess
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin, urlparse
import urllib.parse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from fake_useragent import UserAgent

# ----------------------------
# Configuration
# ----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
JSON_PATH = os.path.join(DATA_DIR, "oaic_media_releases.json")
CSV_PATH = os.path.join(DATA_DIR, "oaic_media_releases.csv")
LOG_PATH = os.path.join(DATA_DIR, 'oaic_selenium_scraper.log')
BASE_URL = "https://www.oaic.gov.au"
MEDIA_CENTRE_URL = "https://www.oaic.gov.au/news/media-centre"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
}


# Configuration based on run type
RUN_TYPE = os.environ.get('RUN_TYPE', 'daily')  # 'daily' or 'initial'
IS_DAILY_RUN = RUN_TYPE == 'daily'

if IS_DAILY_RUN:
    MAX_PAGES = 3  # Recent pages only for daily
    HUMAN_DELAY_MIN = 2
    HUMAN_DELAY_MAX = 5
else:
    MAX_PAGES = 50  # More comprehensive for initial run
    HUMAN_DELAY_MIN = 3
    HUMAN_DELAY_MAX = 8

# Use non-headless mode for human-like behavior
USE_HEADLESS = os.environ.get('HEADLESS', 'false').lower() == 'true'

# Statistics for monitoring
stats = {
    'pages_processed': 0,
    'articles_found': 0,
    'articles_enriched': 0,
    'articles_skipped': 0,
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
    logger.info(f"Enhanced OAIC Selenium Scraper - {RUN_TYPE.upper()} RUN")
    logger.info(f"Max pages: {MAX_PAGES}")
    logger.info(f"Headless mode: {USE_HEADLESS}")
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
def setup_chrome_driver() -> WebDriver:
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

def human_like_delay(min_seconds=HUMAN_DELAY_MIN, max_seconds=HUMAN_DELAY_MAX):
    """Add human-like delay"""
    delay = random.uniform(min_seconds, max_seconds)
    logger.debug(f"Human delay: {delay:.2f} seconds")
    time.sleep(delay)

def human_scroll(driver):
    """Perform human-like scrolling"""
    try:
        # Scroll down slowly
        for i in range(3):
            driver.execute_script(f"window.scrollBy(0, {random.randint(200, 400)});")
            time.sleep(random.uniform(0.5, 1.5))
        
        # Scroll back to top
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(random.uniform(1, 2))
    except Exception as e:
        logger.debug(f"Scroll error: {e}")

def generate_hash_id(headline: str, published_date: str) -> str:
    """Generate unique hash ID using headline and published datetime"""
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
            return {article.get("hash_id", "") for article in data if article.get("hash_id")}
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning("Could not read existing JSON file. Starting fresh.")
        return set()

def normalize_date(date_str):
    """Normalize date string for consistent comparison"""
    if not date_str:
        return ""
    
    try:
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

def visit_homepage_human_like(driver):
    """Visit homepage in a human-like manner"""
    logger.info("Visiting homepage like a human...")
    try:
        driver.get(BASE_URL)
        human_like_delay(3, 6)
        
        # Perform human-like actions
        human_scroll(driver)
        
        # Check if page loaded properly
        page_title = driver.title
        logger.info(f"Homepage title: {page_title}")
        
        if "OAIC" not in page_title:
            logger.warning("Homepage might not have loaded correctly")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error visiting homepage: {e}")
        return False

def extract_full_article_content(driver, article_url: str) -> Dict:
    """Extract full content from an individual article page"""
    content_data = {
        'full_content': '',
        'content_html': '',
        'headings': [],
        'links': [],
        'reading_time': '',
        'content_sections': []
    }
    
    try:
        logger.info(f"Extracting full content from: {article_url}")
        driver.get(article_url)
        human_like_delay(2, 4)
        
        # Wait for content to load
        wait = WebDriverWait(driver, 20)
        try:
            wait.until(EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article")),
                EC.presence_of_element_located((By.CSS_SELECTOR, ".page-content")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "main")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "#main-content-area")),
                EC.presence_of_element_located((By.CSS_SELECTOR, ".container-max-width"))
            ))
        except TimeoutException:
            logger.warning(f"Timeout waiting for article content: {article_url}")
        
        # Additional wait for dynamic content
        time.sleep(2)
        
        # Get page source and parse
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Extract reading time if available
        reading_time_selectors = [
            'span#reading-time-text',
            '.reading-time',
            '[id*="reading-time"]'
        ]
        
        for selector in reading_time_selectors:
            reading_time_elem = soup.select_one(selector)
            if reading_time_elem:
                content_data['reading_time'] = reading_time_elem.get_text(strip=True)
                break
        
        # Content extraction with prioritized selectors
        content_selectors = [
            'article.container-max-width',
            'div.container-max-width article',
            '#main-content-area article',
            '.page-content article',
            'article',
            '#main-content-area',
            '.page-content',
            'main',
            'body'
        ]
        
        article_content = None
        selected_selector = None
        
        for selector in content_selectors:
            article_content = soup.select_one(selector)
            if article_content:
                content_text = article_content.get_text(strip=True)
                if content_text and len(content_text) > 100:
                    selected_selector = selector
                    logger.info(f"Found content using selector: {selector} ({len(content_text)} chars)")
                    break
        
        if not article_content or len(article_content.get_text(strip=True)) < 100:
            logger.warning(f"Could not find substantial article content for: {article_url}")
            # Try alternative approach: look for paragraphs
            all_paragraphs = soup.find_all('p')
            substantial_paragraphs = [p for p in all_paragraphs if len(p.get_text(strip=True)) > 50]
            
            if substantial_paragraphs:
                article_content = soup.new_tag('div')
                for p in substantial_paragraphs:
                    article_content.append(p)
                logger.info(f"Alternative extraction found {len(substantial_paragraphs)} paragraphs")
            else:
                logger.error(f"No substantial content found for: {article_url}")
                return content_data
        
        # Extract content
        full_text = article_content.get_text(strip=True)
        content_data['full_content'] = full_text
        content_data['content_html'] = str(article_content)
        
        logger.info(f"Extracted {len(full_text)} characters of content")
        
        # Extract headings
        headings = article_content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        content_data['headings'] = []
        
        for heading in headings:
            heading_text = heading.get_text(strip=True)
            if heading_text and len(heading_text) > 2:
                content_data['headings'].append({
                    'level': heading.name,
                    'text': heading_text
                })
        
        # Extract links
        links = article_content.find_all('a', href=True)
        content_data['links'] = []
        
        for link in links:
            link_text = link.get_text(strip=True)
            href = link.get('href', '')
            
            if link_text and href:
                full_url = urljoin(BASE_URL, href)
                is_external = not href.startswith('/') and BASE_URL not in href
                
                content_data['links'].append({
                    'text': link_text,
                    'url': full_url,
                    'is_external': is_external
                })
        
        # Extract content sections
        sections = []
        content_elements = article_content.find_all(['p', 'ul', 'ol', 'blockquote', 'div'])
        
        for element in content_elements:
            element_text = element.get_text(strip=True)
            if element_text and len(element_text) > 20:
                sections.append({
                    'type': element.name,
                    'content': element_text,
                    'html': str(element)
                })
        
        content_data['content_sections'] = sections
        
        logger.info(f"Successfully extracted: {len(content_data['headings'])} headings, {len(content_data['links'])} links, {len(sections)} sections")
        
    except Exception as e:
        logger.error(f"Error extracting article content from {article_url}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return content_data

def extract_articles_metadata_from_page(driver) -> List[Dict]:
    """Extract article metadata from current page"""
    articles = []
    
    try:
        # Wait for content to load
        wait = WebDriverWait(driver, 15)
        
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.card")))
        except TimeoutException:
            logger.warning("Timeout waiting for cards to load")
        
        # Get page source and parse
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Find cards
        card_selectors = [
            'div.card',
            '.search-results-cards div.card',
            '.cards-container div.card',
            '.content-cards div.card'
        ]
        
        cards = []
        for selector in card_selectors:
            cards = soup.select(selector)
            if cards:
                logger.info(f"Found {len(cards)} cards using selector: {selector}")
                break
        
        if not cards:
            logger.warning("No cards found with any selector")
            return articles
        
        # Parse each card
        for i, card in enumerate(cards):
            if shutdown_flag:
                break
                
            try:
                # Extract title and URL
                title_selectors = [
                    'a.card-title',
                    '.card-title a',
                    '.title a',
                    'h3 a',
                    'h2 a',
                    'a[href*="/news/"]'
                ]
                
                title_link = None
                for selector in title_selectors:
                    title_link = card.select_one(selector)
                    if title_link:
                        break
                
                if not title_link:
                    title_link = card.find('a', href=True)
                
                if not title_link:
                    logger.debug(f"No title link found in card {i}")
                    continue
                
                title = title_link.get_text(strip=True)
                href = title_link.get('href', '')
                
                if not title or not href:
                    logger.debug(f"Missing title or href in card {i}")
                    continue
                
                # Handle redirect URLs
                if href.startswith('/s/redirect'):
                    if 'url=' in href:
                        url_part = href.split('url=')[1].split('&')[0]
                        article_url = urllib.parse.unquote(url_part)
                    else:
                        article_url = urljoin(BASE_URL, href)
                else:
                    article_url = urljoin(BASE_URL, href)
                
                # Extract description
                description = ""
                desc_selectors = [
                    'p.card-text',
                    '.card-text',
                    '.description',
                    '.excerpt',
                    'p'
                ]
                
                for selector in desc_selectors:
                    desc_elem = card.select_one(selector)
                    if desc_elem:
                        description = desc_elem.get_text(strip=True)
                        if description and len(description) > 20:
                            break
                
                # Extract tags
                tags = []
                article_type = ''
                category = ''
                
                tag_selectors = [
                    'div.tags div.tag',
                    '.tags .tag',
                    '.tag',
                    '.category'
                ]
                
                for selector in tag_selectors:
                    tag_elements = card.select(selector)
                    if tag_elements:
                        tag_texts = [tag.get_text(strip=True) for tag in tag_elements]
                        tags = [t for t in tag_texts if t]
                        if tags:
                            article_type = tags[0]
                            if len(tags) > 1:
                                category = ', '.join(tags[1:])
                        break
                
                # Extract date
                published_date = ""
                date_selectors = [
                    'div.info p.date',
                    'p.date',
                    '.date',
                    '.published-date',
                    'time',
                    '.meta-date'
                ]
                
                for selector in date_selectors:
                    date_element = card.select_one(selector)
                    if date_element:
                        published_date = date_element.get_text(strip=True)
                        if published_date:
                            break
                
                # Generate hash
                normalized_date = normalize_date(published_date)
                hash_id = generate_hash_id(title, normalized_date)
                
                article_data = {
                    'hash_id': hash_id,
                    'title': title,
                    'url': article_url,
                    'description': description,
                    'tags': tags,
                    'type': article_type,
                    'category': category,
                    'published_date': published_date,
                    'scraped_date': datetime.now(timezone.utc).isoformat(),
                }
                
                articles.append(article_data)
                logger.debug(f"Extracted metadata for: {title[:50]}...")
                
            except Exception as e:
                logger.error(f"Error parsing card {i}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Error extracting articles metadata: {e}")
    
    return articles

def find_next_page_link(driver) -> Optional[str]:
    """Find the URL for the next page"""
    try:
        # OAIC-specific pagination selectors
        next_selectors = [
            "a img.chevron-forward",
            ".search-results__pagination-navlinks img.chevron-forward",
            ".search-results__pagination a img.chevron-forward",
            "a[aria-label*='next']",
            "a[aria-label*='Next']",
            ".pagination a[rel='next']",
            ".pagination .next"
        ]
        
        for selector in next_selectors:
            try:
                if "chevron-forward" in selector:
                    # Find chevron image and get parent link
                    chevron_imgs = driver.find_elements(By.CSS_SELECTOR, selector)
                    for img in chevron_imgs:
                        parent_link = img.find_element(By.XPATH, "..")
                        next_url = parent_link.get_attribute('href')
                        if next_url and next_url != driver.current_url:
                            logger.info(f"Found next page using chevron: {next_url}")
                            return parent_link
                else:
                    # Direct link approach
                    next_links = driver.find_elements(By.CSS_SELECTOR, selector)
                    for link in next_links:
                        href = link.get_attribute('href')
                        if href and href != driver.current_url:
                            logger.info(f"Found next page using {selector}: {href}")
                            return link
                            
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        # Try URL construction as fallback
        try:
            current_url = driver.current_url
            if "start_rank=" in current_url:
                import re
                match = re.search(r'start_rank=(\d+)', current_url)
                if match:
                    current_start = int(match.group(1))
                    next_start = current_start + 10
                    next_url = re.sub(r'start_rank=\d+', f'start_rank={next_start}', current_url)
                    
                    potential_links = driver.find_elements(By.CSS_SELECTOR, f"a[href*='start_rank={next_start}']")
                    if potential_links:
                        logger.info(f"Found next page using URL construction: {next_url}")
                        return potential_links[0]
            else:
                separator = "&" if "?" in current_url else "?"
                next_url = f"{current_url}{separator}start_rank=11"
                potential_links = driver.find_elements(By.CSS_SELECTOR, f"a[href*='start_rank=11']")
                if potential_links:
                    logger.info(f"Found next page using URL construction: {next_url}")
                    return potential_links[0]
                        
        except Exception as e:
            logger.debug(f"URL construction approach failed: {e}")
        
        logger.info("No next page link found")
        return None
        
    except Exception as e:
        logger.error(f"Error finding next page: {e}")
        return None

def scrape_with_selenium(existing_ids: Set[str]) -> List[Dict]:
    """Main scraping function using Selenium"""
    all_articles = []
    
    driver = setup_chrome_driver()
    
    try:
        # Visit homepage first
        if not visit_homepage_human_like(driver):
            logger.error("Failed to establish session with homepage")
            return []
        
        # Navigate to media centre
        logger.info("Navigating to media centre...")
        driver.get(MEDIA_CENTRE_URL)
        human_like_delay(4, 8)
        
        # Perform human-like scrolling
        human_scroll(driver)
        
        page_count = 0
        current_url = driver.current_url
        
        while page_count < MAX_PAGES:
            if shutdown_flag:
                break
            
            page_count += 1
            logger.info(f"Processing page {page_count} - URL: {current_url}")
            
            # Extract article metadata from current page
            page_articles = extract_articles_metadata_from_page(driver)
            
            if not page_articles:
                logger.warning(f"No articles found on page {page_count}")
            else:
                logger.info(f"Found {len(page_articles)} articles on page {page_count}")
                
                # Filter out existing articles and extract full content
                new_articles = []
                for article in page_articles:
                    if article['hash_id'] not in existing_ids:
                        # Extract full content for new articles
                        logger.info(f"Extracting full content for: {article['title'][:50]}...")
                        content_data = extract_full_article_content(driver, article['url'])
                        article.update(content_data)
                        
                        # Add success flag
                        article['content_extracted'] = bool(content_data['full_content'])
                        
                        if article['content_extracted']:
                            stats['articles_enriched'] += 1
                        
                        new_articles.append(article)
                        stats['articles_found'] += 1
                        
                        # Navigate back to listing page
                        driver.get(current_url)
                        human_like_delay(2, 4)
                        
                    else:
                        stats['articles_skipped'] += 1
                
                logger.info(f"Page {page_count}: Found {len(new_articles)} new articles")
                all_articles.extend(new_articles)
            
            stats['pages_processed'] += 1
            
            # Try to find and navigate to next page
            if page_count < MAX_PAGES:
                next_link = find_next_page_link(driver)
                
                if next_link:
                    try:
                        next_url = next_link.get_attribute('href')
                        logger.info(f"Navigating to next page: {next_url}")
                        
                        # Direct navigation
                        if next_url:
                            driver.get(next_url)
                        else:
                            # Fallback to clicking
                            actions = ActionChains(driver)
                            actions.move_to_element(next_link).pause(random.uniform(0.5, 1.5)).click().perform()
                        
                        # Wait for navigation
                        human_like_delay(5, 10)
                        
                        # Verify we moved to a new page
                        new_url = driver.current_url
                        if new_url == current_url:
                            logger.warning("URL didn't change after navigation attempt")
                            break
                        else:
                            logger.info(f"Successfully navigated to page {page_count + 1}")
                            current_url = new_url
                            human_scroll(driver)
                        
                    except Exception as e:
                        logger.error(f"Error navigating to next page: {e}")
                        break
                else:
                    logger.info("No more pages found")
                    break
    
    finally:
        driver.quit()
    
    return all_articles

def save_articles(new_articles: List[Dict]) -> int:
    """Save articles to JSON and CSV files"""
    if not new_articles:
        logger.info("No new articles to save.")
        return 0
        
    logger.info(f"Saving {len(new_articles)} new articles...")
    existing_articles = []
    
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, 'r', encoding='utf-8') as f:
                existing_articles = json.load(f)
        except json.JSONDecodeError:
            logger.warning("JSON file is corrupted and will be overwritten.")
    
    all_articles = existing_articles + new_articles
    
    # Remove duplicates based on hash_id
    unique_articles = {}
    for article in all_articles:
        hash_id = article.get('hash_id')
        if hash_id:
            unique_articles[hash_id] = article
    
    final_articles = list(unique_articles.values())
    final_articles.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
    
    # Save JSON
    try:
        with open(JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_articles, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON: {len(final_articles)} total articles")
    except Exception as e:
        logger.error(f"Failed to save JSON: {e}")
        stats['errors'] += 1
    
    # Save CSV
    try:
        csv_articles = []
        for article in final_articles:
            csv_article = article.copy()
            
            # Convert complex fields to strings
            csv_article['tags'] = ', '.join(article.get('tags', []))
            csv_article['headings'] = '; '.join([h.get('text', '') for h in article.get('headings', [])])
            csv_article['links_count'] = len(article.get('links', []))
            csv_article['content_length'] = len(article.get('full_content', ''))
            
            # Remove complex nested data from CSV
            for key in ['links', 'content_sections', 'content_html']:
                csv_article.pop(key, None)
            
            csv_articles.append(csv_article)
        
        df = pd.DataFrame(csv_articles)
        df.to_csv(CSV_PATH, index=False, encoding='utf-8-sig')
        logger.info(f"Saved CSV: {len(csv_articles)} total articles")
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")
        stats['errors'] += 1
    
    logger.info(f"Save complete. Total unique articles: {len(final_articles)}")
    logger.info(f"New articles added: {len(new_articles)}")
    return len(final_articles)

def print_summary() -> int:
    """Print summary of scraping results and return exit code"""
    end_time = time.time()
    duration = end_time - stats['start_time']
    
    logger.info("\n" + "="*80)
    logger.info("SCRAPING SUMMARY")
    logger.info("="*80)
    logger.info(f"Run type: {stats['run_type'].upper()}")
    logger.info(f"Pages processed: {stats['pages_processed']}")
    logger.info(f"Articles found: {stats['articles_found']}")
    logger.info(f"Articles with full content: {stats['articles_enriched']}")
    logger.info(f"Articles skipped: {stats['articles_skipped']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info("="*80)
    
    print(f"Enhanced OAIC Selenium Scraper completed: {stats['articles_found']} new articles")
    print(f"Full content extracted for: {stats['articles_enriched']} articles")
    print(f"Pages processed: {stats['pages_processed']}")
    
    if stats['errors'] > 5:
        print("WARNINGS: Multiple errors occurred - check log file")
        return 1
    elif stats['articles_found'] == 0:
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
    
    logger.info(f"--- Starting Enhanced OAIC Selenium Scraper ({RUN_TYPE.upper()} run) ---")
    
    try:
        existing_ids = load_existing_hash_ids()
        logger.info(f"Found {len(existing_ids)} existing articles to skip.")
        
        # Scrape using Selenium
        new_articles = scrape_with_selenium(existing_ids)
        
        # Save results
        save_articles(new_articles)
        exit_code = print_summary()
        
        return exit_code
        
    except Exception as e:
        logger.critical(f"Fatal error in main process: {e}")
        stats['errors'] += 1
        print(f"FATAL ERROR: {e}")
        return 1

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