#!/usr/bin/env python3
"""
MBIE News Scraper - Final Working Version
Scrapes news from MBIE website and saves to JSON dataset
"""
import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime
import os
import sys
import random
import re
from urllib.parse import urljoin

# Configuration
BASE_URL = "https://www.mbie.govt.nz"
NEWS_URL = "https://www.mbie.govt.nz/about/news"
MAX_PAGES = 1
DELAY_BETWEEN_REQUESTS = 2

# Setup paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
JSON_FILE_PATH = os.path.join(DATA_DIR, 'mbie_news.json')

# Create data directory
os.makedirs(DATA_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, 'mbie_scraper.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Try to import optional dependencies
SELENIUM_AVAILABLE = False
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    logger.warning("Selenium not available - will use requests only")

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

class MBIENewsScraper:
    def __init__(self):
        self.session = requests.Session()
        self.driver = None
        self.use_selenium = False
        self.existing_articles = {}
        self.scraped_articles = []
        
        # Setup session headers
        self.session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        logger.info(f"Data file: {JSON_FILE_PATH}")
        logger.info(f"Selenium available: {SELENIUM_AVAILABLE}")
    
    def setup_selenium(self):
        """Setup headless Chrome as fallback option."""
        if not SELENIUM_AVAILABLE:
            return False
        
        try:
            logger.info("Setting up headless Chrome...")
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument(f'--user-agent={random.choice(USER_AGENTS)}')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            self.use_selenium = True
            logger.info("✅ Headless Chrome ready")
            return True
        except Exception as e:
            logger.warning(f"Selenium setup failed: {e}")
            return False
    
    def get_page_with_requests(self, url):
        """Get page content using requests."""
        try:
            time.sleep(random.uniform(1, 3))
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.error(f"Requests failed for {url}: {e}")
            return None
    
    def get_page_with_selenium(self, url):
        """Get page content using Selenium."""
        if not self.driver:
            return None
        try:
            self.driver.get(url)
            
            # Wait for the page to load properly
            if '/news' in url:
                # Wait specifically for news listing items to appear
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.listing-item"))
                )
                logger.info("News listing items loaded successfully")
            else:
                # General page load wait
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            
            # Give a bit more time for dynamic content
            time.sleep(random.uniform(2, 4))
            
            return self.driver.page_source.encode('utf-8')
        except TimeoutException:
            logger.warning(f"Timeout waiting for page to load: {url}")
            # Still return the page source even if specific elements didn't load
            return self.driver.page_source.encode('utf-8')
        except Exception as e:
            logger.error(f"Selenium failed for {url}: {e}")
            return None
    
    def get_page_content(self, url):
        """Get page content using best available method."""
        # For news pages, prefer Selenium since they might load dynamically
        if '/news' in url and self.use_selenium:
            logger.info(f"Using Selenium for news page: {url}")
            content = self.get_page_with_selenium(url)
            if content:
                return content
        
        # Try requests first for other pages or as fallback
        content = self.get_page_with_requests(url)
        
        # Fallback to selenium if available and requests failed
        if not content and self.use_selenium:
            logger.info(f"Requests failed, trying Selenium for {url}")
            content = self.get_page_with_selenium(url)
        
        return content
    
    def clean_content_text(self, text):
        """Clean up extracted content text."""
        if not text:
            return ""
        
        # Remove unwanted patterns
        unwanted_patterns = [
            r'BreadcrumbsHome›About›News.*?(?=Tags|$)',
            r'Tags.*?Back to News',
            r'MBIE media contact.*?Email:media@mbie\.govt\.nz',
            r'Share:https://www\.mbie\.govt\.nz[^\s]*',
            r'Please note: This content will change over time.*?$',
            r'Skip to main content',
            r'Skip to page navigation'
        ]
        
        cleaned_text = text
        for pattern in unwanted_patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE | re.DOTALL)
        
        # Clean up whitespace
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
    def get_article_links(self, page_num=1):
        """Get article links from news listing page."""
        url = f"{NEWS_URL}?start={(page_num - 1) * 10}" if page_num > 1 else NEWS_URL
        
        logger.info(f"Fetching page {page_num}: {url}")
        
        content = self.get_page_content(url)
        if not content:
            logger.error(f"Could not fetch page {page_num}")
            return []
        
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for the listing items with the exact structure you showed
            cards = soup.select("div.listing-item")
            
            if not cards:
                # DEBUG: Save HTML and try alternative approaches
                debug_path = os.path.join(DATA_DIR, f'debug_page_{page_num}.html')
                with open(debug_path, 'w', encoding='utf-8') as f:
                    f.write(soup.prettify())
                logger.warning(f"No div.listing-item found on page {page_num}. Saved HTML to {debug_path}")
                
                # Check if page loaded properly
                if "listing-item" in str(soup):
                    logger.info("Found 'listing-item' in raw HTML - selector might be wrong")
                else:
                    logger.warning("No 'listing-item' found in HTML at all - page may not have loaded")
                
                return []
            
            links = []
            for i, card in enumerate(cards):
                try:
                    # Extract link using the exact structure you provided
                    link_tag = card.select_one("h3 a.listing-link")
                    if not link_tag or 'href' not in link_tag.attrs:
                        logger.debug(f"Card {i+1}: No valid link found")
                        continue
                    
                    # Extract date
                    date_tag = card.select_one("span.listing-date")
                    
                    # Build the article data
                    article_url = urljoin(BASE_URL, link_tag['href'])
                    title = link_tag.get_text(strip=True)
                    date = date_tag.get_text(strip=True) if date_tag else ""
                    
                    # Validate it's actually a news article
                    if '/news/' not in article_url:
                        logger.debug(f"Skipping non-news link: {article_url}")
                        continue
                    
                    links.append({
                        'url': article_url,
                        'title': title,
                        'date': date
                    })
                    
                    logger.debug(f"Found: {title} ({date})")
                    
                except Exception as e:
                    logger.warning(f"Error processing card {i+1}: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(links)} article links from page {page_num}")
            
            # If we got no links but found cards, that's suspicious
            if len(cards) > 0 and len(links) == 0:
                logger.warning(f"Found {len(cards)} cards but extracted 0 links - check selectors")
                # Show structure of first card for debugging
                if cards:
                    logger.info(f"First card structure: {cards[0].prettify()[:500]}...")
            
            return links
            
        except Exception as e:
            logger.error(f"Error parsing page {page_num}: {e}")
            return []
    
    def extract_article_content(self, article_info):
        """Extract content from individual article."""
        article_url = article_info['url']
        
        if article_url in self.existing_articles:
            logger.info(f"Skipping existing article: {article_info['title']}")
            return None
        
        logger.info(f"Scraping article: {article_info['title']}")
        
        content = self.get_page_content(article_url)
        if not content:
            logger.error(f"Could not fetch article: {article_url}")
            return None
        
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract title
            title_elem = soup.select_one("h1.content-page-heading")
            title = title_elem.get_text(strip=True) if title_elem else article_info.get('title', '')
            
            # Extract content
            content_elem = soup.select_one("div.content-area")
            content_text = ""
            
            if content_elem:
                content_text = content_elem.get_text(strip=True, separator=' ')
                content_text = self.clean_content_text(content_text)
            
            # Extract theme
            tag_elems = soup.select("div.category-sidenav a.tag")
            theme = ", ".join([tag.get_text(strip=True) for tag in tag_elems])
            
            # Extract image
            img_elem = soup.select_one("div.content-area img")
            image_url = ""
            if img_elem and img_elem.get('src'):
                image_url = urljoin(BASE_URL, img_elem['src'])
            
            # Extract links
            related_links = []
            pdf_links = []
            
            if content_elem:
                for a in content_elem.select('a[href]'):
                    href = a.get('href')
                    if href:
                        full_url = urljoin(BASE_URL, href)
                        if href.endswith('.pdf'):
                            pdf_links.append(full_url)
                        else:
                            related_links.append(full_url)
            
            # Create article data
            article_data = {
                "url": article_url,
                "title": title,
                "published_date": article_info.get('date', ''),
                "scraped_date": datetime.now().isoformat(),
                "content": content_text,
                "pdf_content": "",
                "theme": theme,
                "image_url": image_url,
                "related_links": list(set(related_links)),
                "pdf_links": list(set(pdf_links)),
                "content_length": len(content_text),
                "pdf_content_length": 0
            }
            
            logger.info(f"Successfully scraped: {title} ({len(content_text)} chars)")
            return article_data
            
        except Exception as e:
            logger.error(f"Error extracting article {article_url}: {e}")
            return None
    
    def load_existing_articles(self):
        """Load existing articles from JSON file."""
        if os.path.exists(JSON_FILE_PATH):
            try:
                with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
                    articles_list = json.load(f)
                return {article['url']: article for article in articles_list}
            except Exception as e:
                logger.warning(f"Could not load existing articles: {e}")
        return {}
    
    def save_data(self):
        """Save articles to JSON file."""
        if not self.scraped_articles:
            logger.info("No new articles to save.")
            return
        
        # Combine existing and new articles
        all_articles_map = self.existing_articles.copy()
        for article in self.scraped_articles:
            all_articles_map[article['url']] = article
        
        # Convert to sorted list
        all_articles = sorted(
            list(all_articles_map.values()),
            key=lambda x: x.get('scraped_date', ''),
            reverse=True
        )
        
        # Save to file
        try:
            with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(all_articles)} total articles ({len(self.scraped_articles)} new)")
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    def scrape_all_articles(self):
        """Main scraping loop."""
        for page_num in range(1, MAX_PAGES + 1):
            article_links = self.get_article_links(page_num)
            
            if not article_links:
                logger.warning(f"No articles found on page {page_num}, stopping")
                break
            
            for article_info in article_links:
                article_data = self.extract_article_content(article_info)
                if article_data:
                    self.scraped_articles.append(article_data)
                
                # Be respectful to the server
                time.sleep(DELAY_BETWEEN_REQUESTS)
    
    def cleanup(self):
        """Clean up resources."""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Chrome driver closed")
            except Exception:
                pass
    
    def run(self):
        """Main execution method."""
        try:
            logger.info("🚀 Starting MBIE News Scraper")
            
            # Setup selenium if available (optional)
            if SELENIUM_AVAILABLE:
                self.setup_selenium()
            
            # Load existing articles
            self.existing_articles = self.load_existing_articles()
            logger.info(f"Found {len(self.existing_articles)} existing articles")
            
            # Scrape articles
            self.scrape_all_articles()
            
            # Save data
            self.save_data()
            
            logger.info("✅ Scraping completed successfully!")
            
        except Exception as e:
            logger.error(f"Critical error: {e}", exc_info=True)
        finally:
            self.cleanup()

def clean_data_file():
    """Clean up data file by removing articles with insufficient content."""
    logger.info("Starting data cleaning process")
    
    if not os.path.exists(JSON_FILE_PATH):
        logger.warning("Data file not found.")
        return
    
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Could not read JSON file: {e}")
        return
    
    total = len(data)
    # Keep articles with substantial content
    good = [article for article in data 
            if article.get("content") and len(article.get("content", "")) > 200]
    bad = total - len(good)
    
    logger.info(f"Found {total} records ({len(good)} good, {bad} bad)")
    
    if bad > 0:
        # Create backup
        backup_path = JSON_FILE_PATH + f".backup.{int(datetime.now().timestamp())}"
        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Created backup: {backup_path}")
        
        # Save cleaned data
        with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(good, f, indent=2, ensure_ascii=False)
        logger.info(f"Cleaned and saved {len(good)} good articles")
    else:
        logger.info("All articles are already clean")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--clean':
        clean_data_file()
    else:
        scraper = MBIENewsScraper()
        scraper.run()