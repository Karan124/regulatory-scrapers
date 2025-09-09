#!/usr/bin/env python3
"""
Commerce Commission NZ News Scraper
Scrapes news and events from https://comcom.govt.nz/news-and-media/news-and-events
with pagination support, anti-bot measures, and deduplication.
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import time
import random
from datetime import datetime
import logging
from urllib.parse import urljoin, urlparse
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import hashlib

# Configuration
BASE_URL = "https://comcom.govt.nz"
SEARCH_URL = "https://comcom.govt.nz/news-and-media/news-and-events"
DATA_FOLDER = "data"
JSON_FILE = "comcom_nz_all_news.json"
CSV_FILE = "comcom_nz_all_news.csv"
LOG_FILE = "comcom_scraper.log"

# Set to 3 for daily runs, or higher for initial full scrape
MAX_PAGES = 1  # Change this manually for full scrape vs daily runs

# Create data folder if it doesn't exist
os.makedirs(DATA_FOLDER, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_FOLDER, LOG_FILE)),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ComComScraper:
    def __init__(self):
        self.session = requests.Session()
        self.setup_session()
        self.scraped_articles = set()
        self.existing_data = []
        self.load_existing_data()
        
    def setup_session(self):
        """Setup session with realistic headers and retry strategy"""
        # Realistic browser headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        self.session.headers.update(headers)
        
        # Setup retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
    def create_session_warmup(self):
        """Warm up session by visiting main pages to collect cookies"""
        logger.info("Warming up session...")
        try:
            # Visit main page first
            response = self.session.get(BASE_URL, timeout=10)
            response.raise_for_status()
            time.sleep(random.uniform(1, 3))
            
            # Visit news section
            response = self.session.get(f"{BASE_URL}/news-and-media", timeout=10)
            response.raise_for_status()
            time.sleep(random.uniform(1, 3))
            
            logger.info("Session warmed up successfully")
            return True
        except Exception as e:
            logger.error(f"Session warmup failed: {str(e)}")
            return False
    
    def load_existing_data(self):
        """Load existing data to check for duplicates"""
        json_path = os.path.join(DATA_FOLDER, JSON_FILE)
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    self.existing_data = json.load(f)
                
                # Create set of existing article identifiers for deduplication
                for article in self.existing_data:
                    article_id = self.create_article_id(article.get('url', ''), article.get('headline', ''))
                    self.scraped_articles.add(article_id)
                
                logger.info(f"Loaded {len(self.existing_data)} existing articles")
            except Exception as e:
                logger.error(f"Error loading existing data: {str(e)}")
                self.existing_data = []
    
    def create_article_id(self, url, headline):
        """Create unique identifier for article"""
        identifier = f"{url}_{headline}".encode('utf-8')
        return hashlib.md5(identifier).hexdigest()
    
    def get_page_content(self, url, retries=3):
        """Get page content with error handling and retries"""
        for attempt in range(retries):
            try:
                logger.info(f"Fetching: {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 403:
                    logger.warning(f"403 Forbidden - attempt {attempt + 1}")
                    if attempt < retries - 1:
                        time.sleep(random.uniform(5, 10))
                        continue
                    else:
                        raise Exception("Access forbidden after retries")
                
                response.raise_for_status()
                time.sleep(random.uniform(2, 5))  # Random delay
                return response.text
                
            except Exception as e:
                logger.error(f"Error fetching {url}: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(3, 7))
                else:
                    raise
    
    def extract_links_from_content(self, soup):
        """Extract all links from article content"""
        links = []
        content_div = soup.find('div', class_='main-content')
        if content_div:
            for link in content_div.find_all('a', href=True):
                href = link.get('href')
                if href:
                    full_url = urljoin(BASE_URL, href)
                    link_text = link.get_text(strip=True)
                    if link_text and full_url not in [BASE_URL, BASE_URL + "/"]:
                        links.append({
                            'url': full_url,
                            'text': link_text
                        })
        return links
    
    def extract_images(self, soup):
        """Extract images from article content"""
        images = []
        content_div = soup.find('div', class_='main-content')
        if content_div:
            for img in content_div.find_all('img'):
                src = img.get('src')
                if src:
                    full_url = urljoin(BASE_URL, src)
                    alt_text = img.get('alt', '').strip()
                    images.append({
                        'url': full_url,
                        'alt': alt_text
                    })
        return images
    
    def parse_article_content(self, url):
        """Parse individual article content"""
        try:
            html_content = self.get_page_content(url)
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract main content
            content_div = soup.find('div', class_='main-content')
            content_text = ""
            if content_div:
                # Remove script and style elements
                for script in content_div(["script", "style"]):
                    script.decompose()
                content_text = content_div.get_text(separator='\n', strip=True)
            
            # Extract category from breadcrumb or navigation
            category = ""
            breadcrumb = soup.find('ol', class_='breadcrumb')
            if breadcrumb:
                links = breadcrumb.find_all('a')
                if len(links) > 1:
                    category = links[-2].get_text(strip=True)
            
            # Extract links and images
            related_links = self.extract_links_from_content(soup)
            images = self.extract_images(soup)
            
            return {
                'content': content_text,
                'category': category,
                'related_links': related_links,
                'images': images
            }
            
        except Exception as e:
            logger.error(f"Error parsing article {url}: {str(e)}")
            return {
                'content': "",
                'category': "",
                'related_links': [],
                'images': []
            }
    
    def parse_news_listing_page(self, page_num=1):
        """Parse news listing page"""
        # Construct URL for specific page
        if page_num == 1:
            url = SEARCH_URL
        else:
            start_rank = (page_num - 1) * 10 + 1
            url = f"{SEARCH_URL}?collection=comcom-www-meta&form=mediareleases&scope=/media-releases/,-collection=,-11132&fmo=yes&&sort=date&query=&&meta_d3=01Jan1901&meta_d4=&profile=noise&start_rank={start_rank}"
        
        try:
            html_content = self.get_page_content(url)
            soup = BeautifulSoup(html_content, 'html.parser')
            
            articles = []
            
            # Find all media release items
            media_items = soup.find_all('div', class_='media-release-item')
            
            for item in media_items:
                try:
                    # Extract date
                    date_span = item.find('span')
                    published_date = ""
                    if date_span:
                        published_date = date_span.get_text(strip=True)
                    
                    # Extract headline and URL
                    headline_link = item.find('h4').find('a') if item.find('h4') else None
                    if not headline_link:
                        continue
                    
                    headline = headline_link.get_text(strip=True)
                    article_url = headline_link.get('href')
                    
                    # Clean up URL (remove redirect wrapper)
                    if 'redirect?' in article_url:
                        # Extract actual URL from redirect
                        import urllib.parse
                        parsed = urllib.parse.parse_qs(urllib.parse.urlparse(article_url).query)
                        if 'url' in parsed:
                            article_url = urllib.parse.unquote(parsed['url'][0])
                    
                    article_url = urljoin(BASE_URL, article_url)
                    
                    # Extract summary
                    summary_p = item.find('p')
                    summary = ""
                    if summary_p:
                        summary = summary_p.get_text(strip=True)
                    
                    # Check for duplicates
                    article_id = self.create_article_id(article_url, headline)
                    if article_id in self.scraped_articles:
                        logger.info(f"Skipping duplicate article: {headline}")
                        continue
                    
                    # Parse full article content
                    logger.info(f"Parsing article: {headline}")
                    article_details = self.parse_article_content(article_url)
                    
                    article_data = {
                        'headline': headline,
                        'url': article_url,
                        'published_date': published_date,
                        'scraped_date': datetime.now().isoformat(),
                        'summary': summary,
                        'category': article_details.get('category', ''),
                        'content': article_details.get('content', ''),
                        'related_links': article_details.get('related_links', []),
                        'images': article_details.get('images', []),
                        'page_number': page_num
                    }
                    
                    articles.append(article_data)
                    self.scraped_articles.add(article_id)
                    
                except Exception as e:
                    logger.error(f"Error parsing article item: {str(e)}")
                    continue
            
            logger.info(f"Found {len(articles)} new articles on page {page_num}")
            return articles
            
        except Exception as e:
            logger.error(f"Error parsing page {page_num}: {str(e)}")
            return []
    
    def get_total_pages(self):
        """Get total number of pages from pagination"""
        try:
            html_content = self.get_page_content(SEARCH_URL)
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find pagination
            pagination = soup.find('nav', class_='pagination')
            if pagination:
                # Find the last page number
                page_links = pagination.find_all('a')
                for link in reversed(page_links):
                    span = link.find('span')
                    if span and span.get_text(strip=True).isdigit():
                        return int(span.get_text(strip=True))
            
            return 1
            
        except Exception as e:
            logger.error(f"Error getting total pages: {str(e)}")
            return 1
    
    def save_data(self, all_articles):
        """Save data to JSON and CSV files"""
        # Combine with existing data
        combined_data = self.existing_data + all_articles
        
        # Save JSON
        json_path = os.path.join(DATA_FOLDER, JSON_FILE)
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(combined_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(combined_data)} articles to {json_path}")
        except Exception as e:
            logger.error(f"Error saving JSON: {str(e)}")
        
        # Save CSV
        csv_path = os.path.join(DATA_FOLDER, CSV_FILE)
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                if combined_data:
                    # Flatten the data for CSV
                    flattened_data = []
                    for article in combined_data:
                        flat_article = article.copy()
                        # Convert lists to strings for CSV
                        flat_article['related_links'] = json.dumps(article.get('related_links', []))
                        flat_article['images'] = json.dumps(article.get('images', []))
                        flattened_data.append(flat_article)
                    
                    fieldnames = flattened_data[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(flattened_data)
            
            logger.info(f"Saved {len(combined_data)} articles to {csv_path}")
        except Exception as e:
            logger.error(f"Error saving CSV: {str(e)}")
    
    def scrape_all_news(self):
        """Main scraping function"""
        logger.info("Starting Commerce Commission NZ news scraping...")
        
        # Warm up session
        if not self.create_session_warmup():
            logger.error("Failed to warm up session, continuing anyway...")
        
        all_new_articles = []
        
        try:
            # Determine how many pages to scrape
            if MAX_PAGES == -1:  # Full scrape
                total_pages = self.get_total_pages()
                pages_to_scrape = total_pages
                logger.info(f"Full scrape: will scrape {total_pages} pages")
            else:
                pages_to_scrape = MAX_PAGES
                logger.info(f"Limited scrape: will scrape {pages_to_scrape} pages")
            
            # Scrape pages
            for page_num in range(1, pages_to_scrape + 1):
                logger.info(f"Scraping page {page_num} of {pages_to_scrape}")
                articles = self.parse_news_listing_page(page_num)
                all_new_articles.extend(articles)
                
                # Random delay between pages
                if page_num < pages_to_scrape:
                    delay = random.uniform(3, 8)
                    logger.info(f"Waiting {delay:.1f} seconds before next page...")
                    time.sleep(delay)
            
            logger.info(f"Scraping completed. Found {len(all_new_articles)} new articles")
            
            # Save data
            if all_new_articles:
                self.save_data(all_new_articles)
            else:
                logger.info("No new articles found")
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            # Still try to save any articles we managed to scrape
            if all_new_articles:
                self.save_data(all_new_articles)

def main():
    """Main function"""
    logger.info("="*50)
    logger.info("Commerce Commission NZ News Scraper Started")
    logger.info(f"MAX_PAGES setting: {MAX_PAGES}")
    logger.info("="*50)
    
    scraper = ComComScraper()
    scraper.scrape_all_news()
    
    logger.info("="*50)
    logger.info("Scraping completed!")
    logger.info("="*50)

if __name__ == "__main__":
    main()