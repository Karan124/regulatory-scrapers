#!/usr/bin/env python3
"""
APRA Letters, Notes, and Advice Scraper - SELENIUM VERSION
Uses a real browser to appear completely human
"""

import os
import json
import csv
import time
import random
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
import hashlib

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.apra.gov.au"
LETTERS_URL = f"{BASE_URL}/letters-notes-advice"
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "apra_all_letters.json")
CSV_FILE = os.path.join(DATA_DIR, "apra_all_letters.csv")
LOG_FILE = os.path.join(DATA_DIR, "apra_scraper_selenium.log")

# MAX_PAGES: None for first run (all pages), 3 for daily runs
MAX_PAGES = 3  # Set to 3 for daily runs

# Delay range between actions (in seconds)
MIN_DELAY = 3
MAX_DELAY = 7

# Setup logging
os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class APRAScraperSelenium:
    def __init__(self):
        self.driver = None
        self.existing_articles = self.load_existing_articles()
        self.new_articles = []
        
    def load_existing_articles(self):
        """Load existing articles from JSON file for deduplication"""
        if os.path.exists(JSON_FILE):
            try:
                with open(JSON_FILE, 'r', encoding='utf-8') as f:
                    articles = json.load(f)
                    # Create a set of unique identifiers (URL hashes)
                    return {self.get_article_hash(article['url']) for article in articles}
            except Exception as e:
                logger.error(f"Error loading existing articles: {e}")
                return set()
        return set()
    
    def get_article_hash(self, url):
        """Generate a hash for article URL for deduplication"""
        return hashlib.md5(url.encode()).hexdigest()
    
    def setup_driver(self):
        """Setup Chrome driver with human-like settings"""
        logger.info("Setting up Chrome browser...")
        
        chrome_options = Options()
        
        # Make it look human - DON'T run headless
        # chrome_options.add_argument("--headless")  # Commented out to show browser
        
        # Human-like browser settings
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Set a realistic user agent
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Window size
        chrome_options.add_argument("--window-size=1920,1080")
        
        try:
            # Use webdriver-manager to automatically download/setup chromedriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Chrome browser started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Chrome driver: {e}")
            logger.info("Please ensure Chrome and Chromedriver are installed")
            logger.info("You can install chromedriver via: pip install chromedriver-autoinstaller")
            return False
    
    def human_delay(self, min_seconds=None, max_seconds=None):
        """Add human-like delays"""
        if min_seconds is None:
            min_seconds = MIN_DELAY
        if max_seconds is None:
            max_seconds = MAX_DELAY
            
        delay = random.uniform(min_seconds, max_seconds)
        logger.debug(f"Waiting {delay:.1f} seconds...")
        time.sleep(delay)
    
    def extract_articles_from_page(self, page_source):
        """Extract all articles from the current page HTML"""
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Find article containers
        article_containers = soup.select('div.grid--4-col.views-row')
        
        logger.info(f"Found {len(article_containers)} article containers on page")
        
        articles = []
        
        for container in article_containers:
            # Find the tile within this container
            tile = container.select_one('article .tile--teaser')
            if not tile:
                tile = container.select_one('.tile--teaser')
            
            if not tile:
                logger.debug(f"No tile found in container")
                continue
            
            # Extract URL
            article_url = None
            link_elem = tile.select_one('a.tile__link-cover')
            if link_elem:
                href = link_elem.get('href')
                if href:
                    article_url = urljoin(BASE_URL, href)
            
            if not article_url:
                logger.debug(f"No URL found for tile")
                continue
            
            # Check if article already exists
            article_hash = self.get_article_hash(article_url)
            if article_hash in self.existing_articles:
                logger.info(f"Article already exists, skipping: {article_url}")
                continue
            
            # Extract title
            title = None
            title_elem = tile.find('h4')
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Extract type
            article_type = None
            type_elem = tile.select_one('.tile__subject .field-field-letter-type')
            if type_elem:
                article_type = type_elem.get_text(strip=True)
            
            # Extract date
            pub_date = None
            time_elem = tile.select_one('.tile__date time')
            if time_elem:
                pub_date = time_elem.get('datetime') or time_elem.get_text(strip=True)
            
            article = {
                'url': article_url,
                'title': title,
                'type': article_type,
                'published_date': pub_date,
                'scraped_date': datetime.now().isoformat()
            }
            
            logger.info(f"Found new article: {title}")
            articles.append(article)
        
        return articles
    
    def scrape_article_content(self, article_url):
        """Scrape individual article content using the browser"""
        logger.info(f"Scraping article content: {article_url}")
        
        try:
            # Navigate to article page
            self.driver.get(article_url)
            
            # Add human-like delay
            self.human_delay(2, 4)
            
            # Wait for content to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
            
            # Get page source and parse
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract content
            content_text = ""
            content_elem = soup.select_one('.rich-text .block-field-blocknodeletterbody')
            
            if not content_elem:
                content_elem = soup.select_one('.rich-text')
                
            if not content_elem:
                content_elem = soup.select_one('.field-field-body')
            
            if content_elem:
                # Remove scripts and styles
                for script in content_elem(["script", "style"]):
                    script.decompose()
                content_text = content_elem.get_text(separator='\n', strip=True)
            
            # Extract category from breadcrumb
            category = None
            breadcrumb = soup.select_one('nav.nav--breadcrumb')
            if breadcrumb:
                links = breadcrumb.find_all('a')
                if len(links) >= 3:
                    category = links[2].get_text(strip=True)
            
            # Extract tags
            tags = []
            tags_elem = soup.select_one('.field-field-letter-tags')
            if tags_elem:
                tag_notifications = tags_elem.select('.notification')
                for notification in tag_notifications:
                    tag_text = notification.get_text(strip=True)
                    if tag_text:
                        tags.append(tag_text)
            
            # Extract related links from content
            related_links = []
            if content_elem:
                for link in content_elem.find_all('a', href=True):
                    href = link.get('href')
                    if href and not href.startswith('#'):
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in related_links and full_url != article_url:
                            related_links.append(full_url)
            
            # Extract images
            images = []
            if content_elem:
                for img in content_elem.find_all('img'):
                    img_src = img.get('src')
                    if img_src:
                        images.append(urljoin(BASE_URL, img_src))
            
            return {
                'content_text': content_text,
                'category': category,
                'tags': tags,
                'related_links': related_links,
                'images': images
            }
            
        except Exception as e:
            logger.error(f"Error scraping article content {article_url}: {e}")
            return {
                'content_text': '',
                'category': None,
                'tags': [],
                'related_links': [],
                'images': []
            }
    
    def scrape_page(self, page_url):
        """Scrape a single page of articles"""
        logger.info(f"Navigating to: {page_url}")
        
        try:
            # Navigate to the page
            self.driver.get(page_url)
            
            # Add human-like delay
            self.human_delay()
            
            # Wait for the articles to load
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".tiles"))
                )
                logger.info("Page loaded successfully")
            except TimeoutException:
                logger.warning("Timeout waiting for articles to load")
            
            # Additional wait to ensure dynamic content is loaded
            self.human_delay(2, 3)
            
            # Get page source and extract articles
            page_source = self.driver.page_source
            
            # Save page source for debugging
            with open(f'selenium_page_source.html', 'w', encoding='utf-8') as f:
                f.write(page_source)
            logger.info("Saved page source to selenium_page_source.html")
            
            articles = self.extract_articles_from_page(page_source)
            
            # Scrape content for each new article
            for article in articles:
                content_details = self.scrape_article_content(article['url'])
                article.update(content_details)
                self.new_articles.append(article)
                
                # Human-like delay between articles
                self.human_delay(1, 2)
            
            # Find next page URL
            next_page = None
            try:
                next_button = self.driver.find_element(By.CSS_SELECTOR, "li.pagination__next a")
                next_page = next_button.get_attribute('href')
                logger.info(f"Found next page: {next_page}")
            except NoSuchElementException:
                logger.info("No next page found")
            
            return articles, next_page
            
        except Exception as e:
            logger.error(f"Error scraping page {page_url}: {e}")
            return [], None
    
    def scrape_all_pages(self):
        """Scrape all pages with pagination"""
        if not self.setup_driver():
            logger.error("Failed to setup browser driver")
            return []
        
        try:
            all_articles = []
            current_url = LETTERS_URL
            page_count = 0
            consecutive_empty_pages = 0
            
            while current_url:
                page_count += 1
                logger.info(f"Scraping page {page_count}")
                
                # Check if we've reached MAX_PAGES limit
                if MAX_PAGES and page_count > MAX_PAGES:
                    logger.info(f"Reached MAX_PAGES limit ({MAX_PAGES})")
                    break
                
                articles, next_url = self.scrape_page(current_url)
                all_articles.extend(articles)
                
                # Track consecutive pages with no new articles
                if not articles:
                    consecutive_empty_pages += 1
                    logger.info(f"No new articles found on this page (consecutive empty: {consecutive_empty_pages})")
                    if consecutive_empty_pages >= 3:
                        logger.info("3 consecutive pages with no new articles, stopping")
                        break
                else:
                    consecutive_empty_pages = 0
                
                # Ensure we don't get stuck in a loop
                if next_url == current_url:
                    logger.warning("Next URL is same as current URL, stopping to prevent infinite loop")
                    break
                
                current_url = next_url
                
                # Human-like delay between pages
                if current_url:
                    self.human_delay()
            
            logger.info(f"Finished scraping. Total pages processed: {page_count}")
            return all_articles
            
        finally:
            # Always close the browser
            if self.driver:
                logger.info("Closing browser...")
                self.driver.quit()
    
    def save_results(self):
        """Save results to JSON and CSV files"""
        # Load existing articles if any
        existing_data = []
        if os.path.exists(JSON_FILE):
            try:
                with open(JSON_FILE, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except:
                existing_data = []
        
        # Combine with new articles
        all_data = existing_data + self.new_articles
        
        # Save to JSON
        with open(JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(all_data)} total articles to {JSON_FILE}")
        
        # Save to CSV
        if all_data:
            keys = ['url', 'title', 'type', 'category', 'published_date', 'scraped_date', 
                    'tags', 'related_links', 'images', 'content_text']
            
            with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                
                for article in all_data:
                    # Convert lists to strings for CSV
                    row = article.copy()
                    row['tags'] = '|'.join(article.get('tags', []))
                    row['related_links'] = '|'.join(article.get('related_links', []))
                    row['images'] = '|'.join(article.get('images', []))
                    writer.writerow(row)
            
            logger.info(f"Saved {len(all_data)} total articles to {CSV_FILE}")
    
    def run(self):
        """Main execution method"""
        logger.info("Starting APRA scraper (SELENIUM VERSION)...")
        logger.info(f"MAX_PAGES setting: {MAX_PAGES}")
        logger.info(f"Existing articles: {len(self.existing_articles)}")
        
        try:
            self.scrape_all_pages()
            self.save_results()
            logger.info(f"Scraping completed successfully! New articles found: {len(self.new_articles)}")
        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}")
            raise


if __name__ == "__main__":
    scraper = APRAScraperSelenium()
    scraper.run()