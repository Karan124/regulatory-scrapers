#!/usr/bin/env python3
"""
MBIE News Scraper
Scrapes news articles from New Zealand's Ministry of Business, Innovation & Employment
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import logging
from datetime import datetime
import os
import re
from urllib.parse import urljoin, urlparse
import hashlib
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import PyPDF2
import io
import requests_cache
from fake_useragent import UserAgent

# Configuration
BASE_URL = "https://www.mbie.govt.nz"
NEWS_URL = "https://www.mbie.govt.nz/about/news"
DATA_DIR = "data"
MAX_PAGES = 3  # Set to 3 for daily runs, 50 for initial full scrape
DELAY_BETWEEN_REQUESTS = 2  # seconds
MAX_RETRIES = 3

# Ensure data directory exists
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

# Setup requests cache
requests_cache.install_cache('mbie_cache', expire_after=3600)

class MBIENewsScraper:
    def __init__(self):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.setup_session()
        self.existing_articles = self.load_existing_articles()
        self.scraped_articles = []
        
    def setup_session(self):
        """Setup session with proper headers and cookies"""
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        # Visit main page to collect cookies
        try:
            logger.info("Visiting main page to collect cookies...")
            response = self.session.get(BASE_URL, timeout=30)
            response.raise_for_status()
            time.sleep(2)
            
            # Visit news page
            response = self.session.get(NEWS_URL, timeout=30)
            response.raise_for_status()
            logger.info("Successfully established session")
        except Exception as e:
            logger.error(f"Failed to setup session: {e}")
            raise
    
    def load_existing_articles(self):
        """Load existing articles to avoid duplicates"""
        json_file = os.path.join(DATA_DIR, 'mbie_news.json')
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {article['url']: article for article in data}
            except Exception as e:
                logger.warning(f"Could not load existing articles: {e}")
        return {}
    
    def get_article_links(self, page_num=1):
        """Get article links from a specific page"""
        if page_num == 1:
            url = NEWS_URL
        else:
            start = (page_num - 1) * 10
            url = f"{NEWS_URL}?start={start}"
        
        try:
            logger.info(f"Fetching page {page_num}: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find article links using the recommended selector
            article_links = []
            
            # Look for news article links
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href and '/about/news/' in href and href not in ['/about/news/', '/about/news']:
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in [article['url'] for article in article_links]:
                        article_links.append({
                            'url': full_url,
                            'title': link.get_text(strip=True) or 'No title'
                        })
            
            logger.info(f"Found {len(article_links)} article links on page {page_num}")
            return article_links
            
        except Exception as e:
            logger.error(f"Error fetching page {page_num}: {e}")
            return []
    
    def extract_pdf_text(self, pdf_url):
        """Extract text from PDF"""
        try:
            logger.info(f"Extracting PDF: {pdf_url}")
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text()
            
            # Clean the text
            text = re.sub(r'\s+', ' ', text)  # Replace multiple whitespaces
            text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)\[\]\{\}]', '', text)  # Remove unwanted chars
            text = text.strip()
            
            logger.info(f"Extracted {len(text)} characters from PDF")
            return text
            
        except Exception as e:
            logger.error(f"Error extracting PDF {pdf_url}: {e}")
            return ""
    
    def extract_article_content(self, article_url):
        """Extract content from a single article"""
        try:
            logger.info(f"Scraping article: {article_url}")
            
            # Check if already scraped
            if article_url in self.existing_articles:
                logger.info(f"Article already exists: {article_url}")
                return None
            
            response = self.session.get(article_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract title
            title = ""
            title_elem = soup.find('h1')
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Extract published date
            published_date = ""
            date_patterns = [
                r'Published:\s*(\d{1,2}\s+\w+\s+\d{4})',
                r'(\d{1,2}\s+\w+\s+\d{4})'
            ]
            
            page_text = soup.get_text()
            for pattern in date_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    published_date = match.group(1)
                    break
            
            # Extract main content
            content = ""
            
            # Try different content selectors
            content_selectors = [
                'main',
                '[class*="content"]',
                '[class*="body"]',
                '.article-content',
                '.news-content'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Remove navigation and other non-content elements
                    for elem in content_elem.find_all(['nav', 'header', 'footer', 'aside', '.breadcrumb']):
                        elem.decompose()
                    
                    content = content_elem.get_text(strip=True)
                    break
            
            # Extract related links
            related_links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href and (href.startswith('http') or href.startswith('/')):
                    full_url = urljoin(BASE_URL, href)
                    if full_url != article_url and full_url not in related_links:
                        related_links.append(full_url)
            
            # Look for PDF links and extract content
            pdf_content = ""
            pdf_links = []
            
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href and href.lower().endswith('.pdf'):
                    pdf_url = urljoin(BASE_URL, href)
                    pdf_links.append(pdf_url)
            
            # Extract from first PDF if available
            if pdf_links:
                pdf_content = self.extract_pdf_text(pdf_links[0])
            
            # Extract theme/tags
            theme = ""
            theme_elem = soup.find('div', class_='tags') or soup.find('div', class_='categories')
            if theme_elem:
                theme = theme_elem.get_text(strip=True)
            
            # Extract image URL
            image_url = ""
            img_elem = soup.find('img', src=True)
            if img_elem:
                image_url = urljoin(BASE_URL, img_elem['src'])
            
            # Create article data
            article_data = {
                'url': article_url,
                'title': title,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'content': content,
                'pdf_content': pdf_content,
                'theme': theme,
                'image_url': image_url,
                'related_links': related_links[:20],  # Limit to first 20 links
                'pdf_links': pdf_links,
                'content_length': len(content),
                'pdf_content_length': len(pdf_content)
            }
            
            logger.info(f"Successfully scraped: {title}")
            return article_data
            
        except Exception as e:
            logger.error(f"Error scraping article {article_url}: {e}")
            return None
    
    def scrape_all_articles(self):
        """Scrape all articles from all pages"""
        logger.info(f"Starting scrape of up to {MAX_PAGES} pages")
        
        for page_num in range(1, MAX_PAGES + 1):
            try:
                # Get article links for this page
                article_links = self.get_article_links(page_num)
                
                if not article_links:
                    logger.info(f"No articles found on page {page_num}, stopping")
                    break
                
                # Scrape each article
                for article_link in article_links:
                    article_data = self.extract_article_content(article_link['url'])
                    if article_data:
                        self.scraped_articles.append(article_data)
                    
                    # Delay between requests
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                
                logger.info(f"Completed page {page_num}")
                
                # Delay between pages
                time.sleep(DELAY_BETWEEN_REQUESTS * 2)
                
            except Exception as e:
                logger.error(f"Error on page {page_num}: {e}")
                continue
        
        logger.info(f"Scraping completed. Found {len(self.scraped_articles)} new articles")
    
    def save_data(self):
        """Save scraped data to JSON and CSV files"""
        if not self.scraped_articles:
            logger.info("No new articles to save")
            return
        
        # Combine existing and new articles
        all_articles = list(self.existing_articles.values()) + self.scraped_articles
        
        # Sort by scraped date (newest first)
        all_articles.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
        
        # Save to JSON
        json_file = os.path.join(DATA_DIR, 'mbie_news.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(all_articles, f, indent=2, ensure_ascii=False)
        
        # Save to CSV
        csv_file = os.path.join(DATA_DIR, 'mbie_news.csv')
        df = pd.DataFrame(all_articles)
        
        # Convert lists to strings for CSV
        list_columns = ['related_links', 'pdf_links']
        for col in list_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: '|'.join(x) if isinstance(x, list) else x)
        
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        logger.info(f"Saved {len(all_articles)} articles to {json_file} and {csv_file}")
        logger.info(f"New articles added: {len(self.scraped_articles)}")
    
    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting MBIE News Scraper")
            logger.info(f"Max pages to scrape: {MAX_PAGES}")
            logger.info(f"Existing articles: {len(self.existing_articles)}")
            
            self.scrape_all_articles()
            self.save_data()
            
            logger.info("Scraping completed successfully")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise

def main():
    """Main entry point"""
    scraper = MBIENewsScraper()
    scraper.run()

if __name__ == "__main__":
    main()