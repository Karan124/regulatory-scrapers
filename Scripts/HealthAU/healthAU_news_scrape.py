#!/usr/bin/env python3
"""
Australian Department of Health News Scraper
============================================

A production-grade scraper for extracting news articles from:
https://www.health.gov.au/news

Features:
- Full pagination support
- PDF content extraction
- Anti-bot detection handling
- Deduplication for incremental runs
- Internal link following for sparse articles
- Comprehensive logging
"""

import requests
from bs4 import BeautifulSoup
import json
import logging
import os
import time
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Set
import hashlib
import PyPDF2
import io
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# We will not use undetected_chromedriver for this solution as the standard driver with proper options is more stable.

# Try to import webdriver-manager for automatic driver management
try:
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

# Configuration
BASE_URL = "https://www.health.gov.au"
NEWS_URL = f"{BASE_URL}/news"
DATA_DIR = "data"
MAX_PAGE = 1
OUTPUT_JSON = os.path.join(DATA_DIR, "healthAU_news.json")
OUTPUT_LOG = os.path.join(DATA_DIR, "healthAU_news.log")
SPARSE_CONTENT_THRESHOLD = 200
REQUEST_DELAY = 2
PDF_DOWNLOAD_TIMEOUT = 30

os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(OUTPUT_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HealthAUScraper:
    def __init__(self):
        self.session = requests.Session()
        self.scraped_articles = set()
        self.existing_data = self.load_existing_data()
        self.driver = None
        self.setup_session()
        
    def setup_session(self):
        """Configure session with anti-bot detection headers"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def setup_selenium(self):
        """Setup a stable Selenium driver by matching the installed Chrome version."""
        if self.driver is not None:
            return

        if not HAS_WEBDRIVER_MANAGER:
            logger.error("webdriver-manager is not installed. Cannot proceed with Selenium.")
            return

        try:
            logger.info("Initializing stable Selenium driver...")
            
            options = Options()
            # Essential stability options for automated environments (like Linux)
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--headless=new") # Modern headless mode
            options.add_argument("--window-size=1920,1080")
            
            # Stealth-related options (modern alternatives)
            options.add_argument('--disable-blink-features=AutomationControlled')
            
            # Note: The deprecated options 'excludeSwitches' and 'useAutomationExtension' are removed
            # as they cause crashes in modern Selenium versions.

            logger.info("Using webdriver-manager to install/cache the driver that matches your browser...")
            
            # This is the key step: ChromeDriverManager().install() detects the installed
            # browser and downloads the corresponding driver version automatically.
            service = Service(ChromeDriverManager().install())
            
            self.driver = webdriver.Chrome(service=service, options=options)
            
            # Apply script to hide webdriver flag from navigator
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Selenium driver initialized successfully.")

        except Exception as e:
            logger.error(f"A critical error occurred during Selenium setup: {e}", exc_info=True)
            self.driver = None # Ensure driver is None on failure

        if self.driver is None:
            logger.warning("No Selenium driver available. Content requiring JavaScript will not be accessible.")
    
    def _add_stealth_scripts(self):
        # This function is kept for consistency but the main stealth script is now in setup_selenium
        pass
    
    def load_existing_data(self) -> Dict:
        """Load existing JSON data to avoid duplicates"""
        if os.path.exists(OUTPUT_JSON):
            try:
                with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for article in data.get('articles', []):
                        if 'url' in article:
                            self.scraped_articles.add(article['url'])
                    logger.info(f"Loaded {len(self.scraped_articles)} existing articles")
                    return data
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
        return {"articles": [], "scraping_metadata": {}}
    
    def get_page_content(self, url: str, use_selenium: bool = False) -> Optional[BeautifulSoup]:
        """Get page content with fallback to Selenium if needed"""
        try:
            if use_selenium and self.driver is None:
                self.setup_selenium()
            
            if use_selenium and self.driver:
                self.driver.get(url)
                time.sleep(2)
                content = self.driver.page_source
                soup = BeautifulSoup(content, 'html.parser')
            else:
                self.session.headers['Referer'] = BASE_URL
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
            
            time.sleep(REQUEST_DELAY)
            return soup
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            if not use_selenium:
                logger.info(f"Retrying with Selenium for {url}")
                return self.get_page_content(url, use_selenium=True)
        except Exception as e:
            logger.error(f"Error getting content from {url}: {e}")
        
        return None
    
    def extract_pdf_content(self, pdf_url: str) -> str:
        """Download and extract text content from PDF"""
        try:
            logger.info(f"Downloading PDF: {pdf_url}")
            response = self.session.get(pdf_url, timeout=PDF_DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text_content = []
            for page_num, page in enumerate(pdf_reader.pages):
                try:
                    text = page.extract_text()
                    if text and text.strip():
                        text_content.append(text)
                except Exception as e:
                    logger.warning(f"Error extracting text from PDF page {page_num}: {e}")
            
            full_text = "\n".join(text_content)
            cleaned_text = self.clean_pdf_text(full_text)
            
            logger.info(f"Successfully extracted {len(cleaned_text)} characters from PDF")
            return cleaned_text
            
        except Exception as e:
            logger.error(f"Error extracting PDF content from {pdf_url}: {e}")
            return ""
    
    def clean_pdf_text(self, text: str) -> str:
        """Clean and normalize PDF text content"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', text)
        text = re.sub(r'Page \d+ of \d+', '', text)
        text = re.sub(r'^\d+\s*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)
        text = text.replace('"', '"').replace('"', '"')
        text = text.replace('–', '-').replace('—', '-')
        return text.strip()
    
    def extract_article_metadata(self, soup: BeautifulSoup, url: str) -> Dict:
        """Extract basic article metadata"""
        metadata = {
            'url': url, 'scraped_date': datetime.now().isoformat(), 'headline': '',
            'introduction': '', 'theme_or_topic': '', 'published_date': '',
            'image_url': '', 'related_links': [], 'main_text_content': '', 'pdf_text_content': ''
        }
        headline_selectors = ['h1', 'h2', 'h3', '.au-display-md a span', '.node--h_news_article h1', 'article h1']
        for selector in headline_selectors:
            headline_elem = soup.select_one(selector)
            if headline_elem and headline_elem.get_text(strip=True):
                metadata['headline'] = headline_elem.get_text(strip=True)
                break
        intro_elem = soup.select_one('#block-page-title-block .au-introduction')
        if intro_elem:
            metadata['introduction'] = intro_elem.get_text(strip=True)
        date_selectors = ['time[datetime]', '.health-field__item time', '.health-metadata time']
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                date_val = date_elem.get('datetime') or date_elem.get_text(strip=True)
                if date_val:
                    metadata['published_date'] = date_val
                    break
        img_selectors = ['article img', '.health-field img', '.node--h_news_article img']
        for selector in img_selectors:
            img_elem = soup.select_one(selector)
            if img_elem and img_elem.get('src'):
                img_url = img_elem.get('src')
                if img_url.startswith('/'):
                    img_url = urljoin(BASE_URL, img_url)
                metadata['image_url'] = img_url
                break
        tag_selectors = ['.health-field--tags a', '.au-tags a', '.health-field__item a']
        topics = []
        for selector in tag_selectors:
            tag_elems = soup.select(selector)
            for tag_elem in tag_elems:
                topic = tag_elem.get_text(strip=True)
                if topic and topic not in topics:
                    topics.append(topic)
        metadata['theme_or_topic'] = ', '.join(topics)
        return metadata
    
    def extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract main text content from article"""
        content_selectors = ['article .health-field__item', '.node--h_news_article .health-field__item', '.main-content p', 'article p', '.content p']
        text_content = []
        for selector in content_selectors:
            elements = soup.select(selector)
            for elem in elements:
                if any(cls in elem.get('class', []) for cls in ['health-metadata', 'health-pager', 'health-field--tags']):
                    continue
                text = elem.get_text(strip=True)
                if text and len(text) > 20:
                    text_content.append(text)
        list_items = soup.select('article li, .main-content li')
        for item in list_items:
            text = item.get_text(strip=True)
            if text:
                text_content.append(f"• {text}")
        return '\n\n'.join(text_content)
    
    def extract_internal_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract internal links from article content"""
        internal_links = []
        content_links = soup.select('article a, .main-content a, .health-field__item a')
        for link in content_links:
            href = link.get('href')
            if href:
                if href.startswith('/'):
                    href = urljoin(BASE_URL, href)
                if BASE_URL in href and href not in internal_links:
                    if not any(exclude in href for exclude in ['/topics/', '/about-us/', '/contact-us/', '#', 'javascript:', 'mailto:', 'tel:']):
                        internal_links.append(href)
        return internal_links
    
    def extract_pdf_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract PDF links from page"""
        pdf_links = []
        pdf_selectors = ['a[href$=".pdf"]', 'a[href*=".pdf"]', '.health-file__link']
        for selector in pdf_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href and '.pdf' in href:
                    if href.startswith('/'):
                        href = urljoin(BASE_URL, href)
                    if href not in pdf_links:
                        pdf_links.append(href)
        return pdf_links
    
    def process_article(self, article_url: str) -> Optional[Dict]:
        """Process a single article and extract all content"""
        if article_url in self.scraped_articles:
            logger.info(f"Skipping already scraped article: {article_url}")
            return None
        
        logger.info(f"Processing article: {article_url}")
        soup = self.get_page_content(article_url)
        if not soup:
            logger.error(f"Failed to get content for {article_url}")
            return None
        
        article_data = self.extract_article_metadata(soup, article_url)
        main_content = self.extract_main_content(soup)
        article_data['main_text_content'] = main_content
        internal_links = self.extract_internal_links(soup)
        article_data['related_links'] = internal_links
        pdf_links = self.extract_pdf_links(soup)
        pdf_contents = []
        for pdf_url in pdf_links:
            pdf_content = self.extract_pdf_content(pdf_url)
            if pdf_content:
                pdf_contents.append(pdf_content)
        
        word_count = len(main_content.split())
        if word_count < SPARSE_CONTENT_THRESHOLD and internal_links:
            logger.info(f"Article content is sparse ({word_count} words). Following internal links.")
            for link_url in internal_links[:3]:
                logger.info(f"Processing internal link: {link_url}")
                link_soup = self.get_page_content(link_url)
                if link_soup:
                    link_content = self.extract_main_content(link_soup)
                    if link_content:
                        article_data['main_text_content'] += f"\n\n[From {link_url}]\n{link_content}"
                    link_pdf_links = self.extract_pdf_links(link_soup)
                    for pdf_url in link_pdf_links:
                        if pdf_url not in pdf_links:
                            pdf_content = self.extract_pdf_content(pdf_url)
                            if pdf_content:
                                pdf_contents.append(pdf_content)
        
        article_data['pdf_text_content'] = '\n\n'.join(pdf_contents)
        self.scraped_articles.add(article_url)
        logger.info(f"Successfully processed article: {article_data['headline']}")
        return article_data
    
    def get_article_links_from_page(self, page_url: str) -> List[str]:
        """Extract article links from a listing page"""
        soup = self.get_page_content(page_url)
        if not soup:
            return []
        article_links = []
        link_selectors = ['.health-listing h3 a', '.au-display-md a', '.health-listing a[href*="/news/"]']
        for selector in link_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    if href.startswith('/'):
                        href = urljoin(BASE_URL, href)
                    if '/news/' in href and href not in article_links:
                        article_links.append(href)
        return article_links
    
    def get_total_pages(self, soup: BeautifulSoup) -> int:
        """Extract total number of pages from pagination"""
        try:
            last_page_link = soup.select_one('.pager__item--last a')
            if last_page_link:
                href = last_page_link.get('href', '')
                match = re.search(r'page=(\d+)', href)
                if match:
                    return int(match.group(1)) + 1
            page_links = soup.select('.pager__item a')
            page_numbers = []
            for link in page_links:
                href = link.get('href', '')
                match = re.search(r'page=(\d+)', href)
                if match:
                    page_numbers.append(int(match.group(1)))
            if page_numbers:
                return max(page_numbers) + 1
        except Exception as e:
            logger.error(f"Error extracting total pages: {e}")
        return 1
    
    def scrape_all_articles(self) -> List[Dict]:
        """Main scraping function"""
        logger.info("Starting article scraping process")
        new_articles = []
        first_page_soup = self.get_page_content(NEWS_URL)
        if not first_page_soup:
            logger.error("Failed to access news listing page")
            return new_articles
        
        total_pages = self.get_total_pages(first_page_soup)
        logger.info(f"Found {total_pages} total pages")
        
        if MAX_PAGE:
            total_pages = min(total_pages, MAX_PAGE)
            logger.info(f"Limiting to {total_pages} pages")
        
        for page_num in range(total_pages):
            if page_num == 0:
                soup = first_page_soup
                page_url = NEWS_URL
            else:
                page_url = f"{NEWS_URL}?page={page_num}"
                soup = self.get_page_content(page_url)
            
            if not soup:
                logger.error(f"Failed to get page {page_num + 1}")
                continue
            
            logger.info(f"Processing page {page_num + 1}/{total_pages}")
            article_links = self.get_article_links_from_page(page_url)
            logger.info(f"Found {len(article_links)} articles on page {page_num + 1}")
            
            for article_url in article_links:
                try:
                    article_data = self.process_article(article_url)
                    if article_data:
                        new_articles.append(article_data)
                except Exception as e:
                    logger.error(f"Error processing article {article_url}: {e}")
                    continue
        
        logger.info(f"Scraped {len(new_articles)} new articles")
        return new_articles
    
    def save_results(self, new_articles: List[Dict]):
        """Save results to JSON file"""
        all_articles = self.existing_data.get('articles', []) + new_articles
        metadata = {'last_scraped': datetime.now().isoformat(), 'total_articles': len(all_articles), 'new_articles_count': len(new_articles), 'scraper_version': '1.0.0'}
        final_data = {'articles': all_articles, 'scraping_metadata': metadata}
        try:
            with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Successfully saved {len(all_articles)} articles to {OUTPUT_JSON}")
        except Exception as e:
            logger.error(f"Error saving results: {e}")
    
    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Selenium driver closed")
            except Exception as e:
                logger.warning(f"Error closing Selenium driver: {e}")
        self.session.close()
        logger.info("Session closed")
    
    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting Australian Health Department news scraper")
            new_articles = self.scrape_all_articles()
            self.save_results(new_articles)
            logger.info("Scraping completed successfully")
        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error during scraping: {e}")
        finally:
            self.cleanup()

def main():
    """Entry point for the scraper"""
    scraper = HealthAUScraper()
    scraper.run()

if __name__ == "__main__":
    main()