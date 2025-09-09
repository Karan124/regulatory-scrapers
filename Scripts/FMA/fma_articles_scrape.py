#!/usr/bin/env python3
"""
FMA NZ Articles Scraper
Scrapes articles from https://www.fma.govt.nz/library/articles/
with support for pagination, PDF extraction, and deduplication.
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import re
import time
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path
import hashlib
import PyPDF2
import io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from fake_useragent import UserAgent

# Configuration
BASE_URL = "https://www.fma.govt.nz"
ARTICLES_URL = f"{BASE_URL}/library/articles/"
DATA_DIR = "data"
MAX_PAGES = 1  # Set to 3 for daily runs, 10+ for initial full scrape
DELAY_RANGE = (2, 5)  # Random delay between requests (seconds)

# File paths
ARTICLES_JSON = os.path.join(DATA_DIR, "fma_articles.json")
ARTICLES_CSV = os.path.join(DATA_DIR, "fma_articles.csv")
LOG_FILE = os.path.join(DATA_DIR, "scraper.log")

# Create data directory
os.makedirs(DATA_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FMAArticleScraper:
    def __init__(self):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.setup_session()
        self.existing_articles = self.load_existing_articles()
        
    def setup_session(self):
        """Configure session with retry strategy and realistic headers"""
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set realistic headers
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def random_delay(self):
        """Random delay to avoid being detected as bot"""
        delay = random.uniform(*DELAY_RANGE)
        time.sleep(delay)
    
    def establish_session(self):
        """Visit main page to establish session and collect cookies"""
        try:
            logger.info("Establishing session with FMA website...")
            response = self.session.get(BASE_URL)
            response.raise_for_status()
            
            # Visit articles page to collect more cookies
            self.random_delay()
            response = self.session.get(ARTICLES_URL)
            response.raise_for_status()
            
            logger.info("Session established successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to establish session: {str(e)}")
            return False
    
    def load_existing_articles(self):
        """Load existing articles for deduplication"""
        existing = {}
        if os.path.exists(ARTICLES_JSON):
            try:
                with open(ARTICLES_JSON, 'r', encoding='utf-8') as f:
                    articles = json.load(f)
                    for article in articles:
                        if 'url' in article:
                            existing[article['url']] = article
                logger.info(f"Loaded {len(existing)} existing articles")
            except Exception as e:
                logger.error(f"Error loading existing articles: {str(e)}")
        return existing
    
    # --- CHANGE 1: MODIFIED get_article_links FUNCTION ---
    # This function now finds the date on the listing page and returns it along with the URL.
    def get_article_links(self, page_url):
        """Extract article links and their dates from a listing page"""
        try:
            self.random_delay()
            response = self.session.get(page_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            article_links = []
            # Find the container for each search result
            results = soup.find_all('section', class_='results-list__result-body')
            
            for result in results:
                link_tag = result.find('h3', class_='results-list__result-title').find('a')
                date_tag = result.find('p', class_='results-list__result-date')
                
                if link_tag and link_tag.get('href'):
                    full_url = urljoin(BASE_URL, link_tag.get('href'))
                    title = link_tag.get_text(strip=True) or 'No title'
                    # Get the date from the same container
                    date = date_tag.get_text(strip=True) if date_tag else ""
                    
                    if full_url not in [al['url'] for al in article_links]:
                        article_links.append({
                            'url': full_url,
                            'title': title,
                            'date': date  # Store the date
                        })
            
            logger.info(f"Found {len(article_links)} article links on page: {page_url}")
            return article_links
            
        except Exception as e:
            logger.error(f"Error getting article links from {page_url}: {str(e)}")
            return []

    def get_pagination_urls(self, soup):
        """Extract pagination URLs"""
        pagination_urls = []
        
        # Look for pagination container
        pagination = soup.find('div', class_='pagination-container')
        if pagination:
            links = pagination.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href and 'start=' in href:
                    full_url = urljoin(BASE_URL, href)
                    pagination_urls.append(full_url)
        
        return pagination_urls
    
    def extract_pdf_text(self, pdf_url):
        """Extract text from PDF"""
        try:
            self.random_delay()
            response = self.session.get(pdf_url)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text()
            
            # Clean up text
            text = re.sub(r'\s+', ' ', text)  # Replace multiple whitespaces
            text = re.sub(r'[^\w\s\.\,\;\:\!\?\-\(\)]', '', text)  # Remove unwanted chars
            text = text.strip()
            
            logger.info(f"Extracted {len(text)} characters from PDF: {pdf_url}")
            return text
            
        except Exception as e:
            logger.error(f"Error extracting PDF text from {pdf_url}: {str(e)}")
            return ""
    
    def extract_links_from_content(self, soup):
        """Extract all links from article content"""
        links = []
        main_content = soup.find('main') or soup
        
        for link in main_content.find_all('a', href=True):
            href = link.get('href')
            if href:
                full_url = urljoin(BASE_URL, href)
                link_text = link.get_text(strip=True)
                if link_text:
                    links.append({
                        'url': full_url,
                        'text': link_text
                    })
        
        return links
    
    # --- CHANGE 2: MODIFIED scrape_article FUNCTION SIGNATURE ---
    # This function now accepts the date as a parameter.
    def scrape_article(self, article_url, published_date):
        """Scrape individual article"""
        try:
            # Check if already scraped
            if article_url in self.existing_articles:
                logger.info(f"Article already exists: {article_url}")
                return self.existing_articles[article_url]
            
            logger.info(f"Scraping article: {article_url}")
            self.random_delay()
            
            response = self.session.get(article_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract basic information
            title = ""
            if soup.find('h1'):
                title = soup.find('h1').get_text(strip=True)
            
            # Extract meta description
            description = ""
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                description = meta_desc.get('content', '')
            
            # Extract main content
            content = ""
            main_content = soup.find('main')
            if main_content:
                # Remove script and style elements
                for element in main_content(['script', 'style', 'nav', 'header', 'footer']):
                    element.decompose()
                content = main_content.get_text(separator=' ', strip=True)
            
            # Extract theme/category
            theme = ""
            breadcrumbs = soup.find('nav', class_='breadcrumbs') or soup.find('ol', class_='breadcrumb')
            if breadcrumbs:
                theme = breadcrumbs.get_text(separator=' > ', strip=True)
            
            # --- The date is now passed in, so we remove the search logic here ---
            
            # Extract image
            image_url = ""
            img = soup.find('img')
            if img and img.get('src'):
                image_url = urljoin(BASE_URL, img.get('src'))
            
            # Extract related links
            related_links = self.extract_links_from_content(soup)
            
            # Check for PDF content
            pdf_content = ""
            pdf_links = []
            
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href and href.lower().endswith('.pdf'):
                    pdf_url = urljoin(BASE_URL, href)
                    pdf_links.append(pdf_url)
            
            # Extract content from first PDF if available
            if pdf_links:
                pdf_content = self.extract_pdf_text(pdf_links[0])
            
            # Combine content
            full_content = content
            if pdf_content:
                full_content += "\n\nPDF Content:\n" + pdf_content
            
            # Generate unique ID
            article_id = hashlib.md5(article_url.encode()).hexdigest()
            
            article_data = {
                'id': article_id,
                'url': article_url,
                'title': title,
                'description': description,
                'theme': theme,
                'published_date': published_date, # Use the passed-in date
                'scraped_date': datetime.now().isoformat(),
                'content': full_content,
                'image_url': image_url,
                'related_links': related_links,
                'pdf_links': pdf_links,
                'content_length': len(full_content)
            }
            
            logger.info(f"Successfully scraped article: {title}")
            return article_data
            
        except Exception as e:
            logger.error(f"Error scraping article {article_url}: {str(e)}")
            return None
    
    def save_data(self, articles):
        """Save articles to JSON and CSV files"""
        try:
            # Save JSON
            with open(ARTICLES_JSON, 'w', encoding='utf-8') as f:
                json.dump(articles, f, indent=2, ensure_ascii=False)
            
            # Save CSV
            if articles:
                fieldnames = [
                    'id', 'url', 'title', 'description', 'theme', 
                    'published_date', 'scraped_date', 'content', 
                    'image_url', 'related_links', 'pdf_links', 'content_length'
                ]
                
                with open(ARTICLES_CSV, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for article in articles:
                        # Convert lists to strings for CSV
                        csv_article = article.copy()
                        csv_article['related_links'] = json.dumps(article.get('related_links', []))
                        csv_article['pdf_links'] = json.dumps(article.get('pdf_links', []))
                        writer.writerow(csv_article)
            
            logger.info(f"Saved {len(articles)} articles to {ARTICLES_JSON} and {ARTICLES_CSV}")
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
    
    def run(self):
        """Main scraping function"""
        logger.info("Starting FMA article scraper...")
        
        if not self.establish_session():
            logger.error("Failed to establish session. Exiting.")
            return
        
        all_articles = list(self.existing_articles.values())
        new_articles_count = 0
        
        try:
            # Start with first page
            current_page = 1
            page_url = ARTICLES_URL
            
            while current_page <= MAX_PAGES:
                logger.info(f"Scraping page {current_page}: {page_url}")
                
                # Get article links from current page
                article_links = self.get_article_links(page_url)
                
                if not article_links:
                    logger.info("No more articles found.")
                    break
                
                # Scrape each article
                for link_info in article_links:
                    article_url = link_info['url']
                    
                    # Skip if already exists
                    if article_url in self.existing_articles:
                        continue
                    
                    # --- CHANGE 3: PASS THE DATE TO THE scrape_article FUNCTION ---
                    article_data = self.scrape_article(article_url, link_info['date'])
                    if article_data:
                        all_articles.append(article_data)
                        new_articles_count += 1
                        
                        # Save periodically
                        if new_articles_count % 5 == 0:
                            self.save_data(all_articles)
                
                # Get next page URL
                if current_page < MAX_PAGES:
                    self.random_delay()
                    response = self.session.get(page_url)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for next page
                    next_link = soup.find('a', class_='next')
                    if next_link and next_link.get('href'):
                        page_url = urljoin(BASE_URL, next_link.get('href'))
                        current_page += 1
                    else:
                        logger.info("No next page found.")
                        break
                else:
                    break
            
            # Final save
            self.save_data(all_articles)
            
            logger.info(f"Scraping completed. Total articles: {len(all_articles)}, New articles: {new_articles_count}")
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            # Save whatever we have
            self.save_data(all_articles)

def main():
    """Main function"""
    scraper = FMAArticleScraper()
    scraper.run()

if __name__ == "__main__":
    main()