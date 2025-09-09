#!/usr/bin/env python3
"""
TGA Alerts Scraper
Scrapes alerts from TGA's website https://www.tga.gov.au/resources/alerts
with bot detection avoidance, PDF extraction, and deduplication logic.
"""

import requests
import json
import csv
import os
import time
import logging
import hashlib
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import pandas as pd
import PyPDF2
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from fake_useragent import UserAgent

# Configuration
BASE_URL = "https://www.tga.gov.au"
ALERTS_URL = f"{BASE_URL}/resources/alerts"
DATA_DIR = "data"
MAX_PAGES = 1 # Set to higher number for first run, 3 for daily runs
DELAY_RANGE = (2, 5)  # Random delay between requests
TIMEOUT = 30
FORCE_RESCRAPE = False  # Set to True to ignore existing data and rescrape everything
DEBUG_MODE = True  # Set to True for detailed debugging

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, 'scraper.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TGAAlertsScraper:
    def __init__(self):
        self.session = self._create_session()
        self.scraped_articles = self._load_existing_data() if not FORCE_RESCRAPE else []
        self.scraped_urls = set(article.get('url', '') for article in self.scraped_articles)
        self.new_articles = []
        
        if FORCE_RESCRAPE:
            logger.info("FORCE_RESCRAPE enabled - ignoring existing data")
        else:
            logger.info(f"Loaded {len(self.scraped_articles)} existing articles")
        
    def _create_session(self):
        """Create a requests session with retry strategy and stealth headers"""
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Stealth headers
        ua = UserAgent()
        session.headers.update({
            'User-Agent': ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        return session
    
    def _load_existing_data(self):
        """Load existing scraped articles from JSON file"""
        json_file = os.path.join(DATA_DIR, 'tga_alerts.json')
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                return []
        return []
    
    def _random_delay(self):
        """Random delay between requests to avoid detection"""
        delay = random.uniform(*DELAY_RANGE)
        time.sleep(delay)
    
    def _get_page(self, url, params=None):
        """Get page content with error handling"""
        try:
            self._random_delay()
            response = self.session.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def _initialize_session(self):
        """Initialize session by visiting main page first"""
        logger.info("Initializing session...")
        
        # Visit main TGA page first
        main_page = self._get_page(BASE_URL)
        if not main_page:
            logger.error("Failed to initialize session")
            return False
            
        # Visit alerts page
        alerts_page = self._get_page(ALERTS_URL)
        if not alerts_page:
            logger.error("Failed to access alerts page")
            return False
            
        logger.info("Session initialized successfully")
        return True
    
    def _set_sort_to_latest(self):
        """Set the sort order to Latest (published_date_sort)"""
        params = {
            'keywords': '',
            'sort_by': 'published_date_sort',
            'sort_field': 'published_date_sort'
        }
        return params
    
    def _extract_article_links(self, soup):
        """Extract article links from the alerts listing page"""
        articles = []
        
        # Find all article elements - try multiple selectors
        article_elements = soup.find_all('article', class_='node--alert')
        
        if not article_elements:
            # Try alternative selector
            article_elements = soup.find_all('article', class_=lambda x: x and 'node--alert' in x)
        
        if not article_elements:
            # Try even broader selector
            article_elements = soup.find_all('li')
            # Filter for those containing articles
            article_elements = [li.find('article') for li in article_elements if li.find('article')]
            article_elements = [art for art in article_elements if art]
        
        logger.info(f"Found {len(article_elements)} article elements on page")
        
        for i, article in enumerate(article_elements):
            try:
                # Get article link - try multiple approaches
                link_element = None
                
                # Try method 1: h3 with summary__title class
                h3_element = article.find('h3', class_='summary__title')
                if h3_element:
                    link_element = h3_element.find('a')
                
                # Try method 2: any h3 with a link
                if not link_element:
                    h3_elements = article.find_all('h3')
                    for h3 in h3_elements:
                        link_candidate = h3.find('a')
                        if link_candidate:
                            link_element = link_candidate
                            break
                
                # Try method 3: any link in the article
                if not link_element:
                    link_element = article.find('a', href=True)
                
                if not link_element:
                    logger.warning(f"No link found in article element {i}")
                    continue
                    
                relative_url = link_element.get('href')
                if not relative_url:
                    continue
                    
                full_url = urljoin(BASE_URL, relative_url)
                
                # Skip if already scraped (unless FORCE_RESCRAPE is True)
                if full_url in self.scraped_urls and not FORCE_RESCRAPE:
                    logger.debug(f"Skipping already scraped URL: {full_url}")
                    continue
                
                # Get title
                title = link_element.get_text(strip=True)
                
                # Get published date - try multiple approaches
                date_element = article.find('time')
                published_date = None
                if date_element:
                    published_date = date_element.get('datetime') or date_element.get_text(strip=True)
                
                # Get summary - try multiple class names
                summary = ""
                summary_selectors = [
                    'div.field--name-field-summary',
                    'div[class*="field-summary"]',
                    'div[class*="summary"]'
                ]
                
                for selector in summary_selectors:
                    summary_element = article.select_one(selector)
                    if summary_element:
                        summary = summary_element.get_text(strip=True)
                        break
                
                # Get alert type - try multiple approaches
                alert_type = ""
                alert_type_selectors = [
                    'div.field--name-field-alert-type',
                    'div[class*="alert-type"]',
                    'div[class*="field-alert"]'
                ]
                
                for selector in alert_type_selectors:
                    alert_type_element = article.select_one(selector)
                    if alert_type_element:
                        alert_type = alert_type_element.get_text(strip=True)
                        break
                
                article_info = {
                    'url': full_url,
                    'title': title,
                    'published_date': published_date,
                    'summary': summary,
                    'alert_type': alert_type
                }
                
                articles.append(article_info)
                logger.debug(f"Extracted article: {title}")
                
            except Exception as e:
                logger.error(f"Error extracting article info from element {i}: {e}")
                continue
        
        logger.info(f"Successfully extracted {len(articles)} articles (excluding already scraped)")
        return articles
    
    def _extract_pdf_text(self, pdf_url):
        """Extract text from PDF file"""
        try:
            logger.info(f"Extracting PDF: {pdf_url}")
            response = self._get_page(pdf_url)
            if not response:
                return ""
            
            # Save PDF temporarily
            temp_pdf = os.path.join(DATA_DIR, 'temp.pdf')
            with open(temp_pdf, 'wb') as f:
                f.write(response.content)
            
            # Extract text
            text = ""
            with open(temp_pdf, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
            
            # Clean up
            os.remove(temp_pdf)
            
            # Clean text
            text = re.sub(r'\s+', ' ', text)  # Replace multiple whitespace with single space
            text = re.sub(r'[^\w\s\-.,;:!?()"]', '', text)  # Remove unwanted characters
            text = text.strip()
            
            return text
            
        except Exception as e:
            logger.error(f"Error extracting PDF {pdf_url}: {e}")
            return ""
    
    def _extract_article_content(self, article_info):
        """Extract full content from individual article page"""
        try:
            logger.info(f"Extracting content from: {article_info['url']}")
            
            response = self._get_page(article_info['url'])
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract main content
            content_div = soup.find('div', class_='field--name-field-body')
            content_text = ""
            if content_div:
                # Remove script and style elements
                for script in content_div(["script", "style"]):
                    script.decompose()
                content_text = content_div.get_text(separator='\n', strip=True)
            
            # Extract theme/topics
            topics_div = soup.find('div', class_='field--name-field-topics')
            topics = []
            if topics_div:
                topic_links = topics_div.find_all('a')
                topics = [link.get_text(strip=True) for link in topic_links]
            
            # Extract image if available
            image_url = ""
            img_element = soup.find('img', alt=True)
            if img_element and img_element.get('src'):
                image_url = urljoin(BASE_URL, img_element.get('src'))
            
            # Extract related links
            related_links = []
            content_links = soup.find_all('a', href=True)
            for link in content_links:
                href = link.get('href')
                if href and (href.startswith('http') or href.startswith('/')):
                    full_link = urljoin(BASE_URL, href)
                    link_text = link.get_text(strip=True)
                    if link_text and full_link not in [article_info['url']]:
                        related_links.append({
                            'url': full_link,
                            'text': link_text
                        })
            
            # Look for PDF links and extract content
            pdf_content = ""
            pdf_links = [link for link in related_links if link['url'].lower().endswith('.pdf')]
            
            if pdf_links:
                # Use first PDF as specified
                first_pdf = pdf_links[0]['url']
                pdf_content = self._extract_pdf_text(first_pdf)
                if pdf_content:
                    content_text = f"{content_text}\n\nPDF Content:\n{pdf_content}"
            
            # If main content is minimal and there's PDF content, prioritize PDF
            if len(content_text.strip()) < 200 and pdf_content:
                content_text = pdf_content
            
            # Create article data
            article_data = {
                'url': article_info['url'],
                'title': article_info['title'],
                'published_date': article_info['published_date'],
                'scraped_date': datetime.now().isoformat(),
                'alert_type': article_info['alert_type'],
                'summary': article_info['summary'],
                'content': content_text,
                'topics': topics,
                'image_url': image_url,
                'related_links': related_links,
                'pdf_links': [link['url'] for link in pdf_links],
                'content_hash': hashlib.md5(content_text.encode()).hexdigest()
            }
            
            return article_data
            
        except Exception as e:
            logger.error(f"Error extracting content from {article_info['url']}: {e}")
            return None
    
    def _save_data(self):
        """Save scraped data to JSON and CSV files"""
        try:
            # Combine existing and new articles
            all_articles = self.scraped_articles + self.new_articles
            
            # Save JSON
            json_file = os.path.join(DATA_DIR, 'tga_alerts.json')
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            
            # Save CSV
            csv_file = os.path.join(DATA_DIR, 'tga_alerts.csv')
            if all_articles:
                # Flatten the data for CSV
                csv_data = []
                for article in all_articles:
                    csv_row = {
                        'url': article.get('url', ''),
                        'title': article.get('title', ''),
                        'published_date': article.get('published_date', ''),
                        'scraped_date': article.get('scraped_date', ''),
                        'alert_type': article.get('alert_type', ''),
                        'summary': article.get('summary', ''),
                        'content': article.get('content', ''),
                        'topics': '; '.join(article.get('topics', [])),
                        'image_url': article.get('image_url', ''),
                        'related_links': json.dumps(article.get('related_links', [])),
                        'pdf_links': '; '.join(article.get('pdf_links', [])),
                        'content_hash': article.get('content_hash', '')
                    }
                    csv_data.append(csv_row)
                
                df = pd.DataFrame(csv_data)
                df.to_csv(csv_file, index=False, encoding='utf-8')
            
            logger.info(f"Saved {len(all_articles)} articles ({len(self.new_articles)} new)")
            
        except Exception as e:
            logger.error(f"Error saving data: {e}")
    
    def test_pagination(self, max_test_pages=5):
        """Test pagination to see if we can access multiple pages"""
        logger.info("Testing pagination...")
        
        if not self._initialize_session():
            return
        
        sort_params = self._set_sort_to_latest()
        
        for page_num in range(max_test_pages):
            current_page_display = page_num + 1
            logger.info(f"Testing page {current_page_display}")
            
            params = sort_params.copy()
            if page_num > 0:
                params['page'] = page_num
            
            logger.info(f"URL: {ALERTS_URL} with params: {params}")
            
            response = self._get_page(ALERTS_URL, params=params)
            if not response:
                logger.error(f"Failed to get page {current_page_display}")
                break
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Count articles on page
            article_elements = soup.find_all('article', class_='node--alert')
            logger.info(f"Page {current_page_display}: Found {len(article_elements)} article elements")
            
            # Check pagination
            pagination = soup.find('nav', class_='health-pager')
            if pagination:
                next_button = pagination.find('a', {'rel': 'next'})
                current_page_elem = pagination.find('span', text=str(current_page_display))
                logger.info(f"Page {current_page_display}: Next button exists: {next_button is not None}")
                logger.info(f"Page {current_page_display}: Current page indicator found: {current_page_elem is not None}")
            else:
                logger.info(f"Page {current_page_display}: No pagination found")
                break
    
    def scrape_alerts(self):
        """Main scraping function"""
        logger.info("Starting TGA alerts scraping...")
        
        # Initialize session
        if not self._initialize_session():
            return
        
        # Set sort parameters
        sort_params = self._set_sort_to_latest()
        
        page_num = 0  # Start from 0 for first page
        total_new_articles = 0
        
        while page_num < MAX_PAGES:
            current_page_display = page_num + 1  # For display purposes
            logger.info(f"Scraping page {current_page_display}")
            
            # Get page with sort parameters
            params = sort_params.copy()
            
            # Page 1 has no page parameter, subsequent pages use page=1, page=2, etc.
            if page_num > 0:
                params['page'] = page_num  # This will be 1, 2, 3, etc. for pages 2, 3, 4, etc.
            
            logger.info(f"Requesting URL: {ALERTS_URL} with params: {params}")
            
            response = self._get_page(ALERTS_URL, params=params)
            if not response:
                logger.error(f"Failed to get page {current_page_display}")
                break
            
            # Debug: Save page content for inspection
            if DEBUG_MODE and current_page_display <= 2:
                debug_file = os.path.join(DATA_DIR, f'debug_page_{current_page_display}.html')
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.debug(f"Saved debug page content to {debug_file}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract article links
            articles = self._extract_article_links(soup)
            
            if not articles:
                logger.info(f"No articles found on page {current_page_display}")
                # Still check if there are more pages in case of extraction issues
                pagination = soup.find('nav', class_='health-pager')
                next_button = pagination.find('a', {'rel': 'next'}) if pagination else None
                if not next_button:
                    logger.info("No next page available, stopping")
                    break
                else:
                    logger.info("No articles extracted but next page exists, continuing")
                    page_num += 1
                    continue
            
            logger.info(f"Found {len(articles)} NEW articles on page {current_page_display}")
            
            # If FORCE_RESCRAPE is False, we might have 0 new articles but should continue to other pages
            if not FORCE_RESCRAPE and len(articles) == 0:
                logger.info(f"No new articles on page {current_page_display}, but continuing to check more pages")
                page_num += 1
                
                # Check if we have pagination and more pages
                pagination = soup.find('nav', class_='health-pager')
                next_button = pagination.find('a', {'rel': 'next'}) if pagination else None
                if not next_button:
                    logger.info("No next page available, stopping")
                    break
                continue
            
            # Extract content from each article
            page_new_articles = 0
            for article_info in articles:
                article_data = self._extract_article_content(article_info)
                if article_data:
                    self.new_articles.append(article_data)
                    total_new_articles += 1
                    page_new_articles += 1
                    logger.info(f"Scraped: {article_data['title']}")
            
            logger.info(f"Page {current_page_display}: {page_new_articles} new articles extracted")
            
            page_num += 1
            
            # Check if we have pagination and if there are more pages
            pagination = soup.find('nav', class_='health-pager')
            if not pagination:
                logger.info("No pagination found, stopping")
                break
                
            # Check if there's a "Next" button to determine if more pages exist
            next_button = pagination.find('a', {'rel': 'next'})
            if not next_button and page_num >= MAX_PAGES:
                logger.info("No next page available or max pages reached")
                break
        
        # Save data
        self._save_data()
        
        logger.info(f"Scraping completed. Total new articles: {total_new_articles}")
        return total_new_articles

def main():
    """Main function"""
    try:
        scraper = TGAAlertsScraper()
        
        # Uncomment the line below to test pagination first
        # scraper.test_pagination(5)
        # return
        
        new_articles_count = scraper.scrape_alerts()
        
        print(f"\nScraping Summary:")
        print(f"New articles scraped: {new_articles_count}")
        print(f"Data saved to: {DATA_DIR}/tga_alerts.json and {DATA_DIR}/tga_alerts.csv")
        print(f"Log file: {DATA_DIR}/scraper.log")
        
    except Exception as e:
        logger.error(f"Main function error: {e}")
        raise

if __name__ == "__main__":
    main()