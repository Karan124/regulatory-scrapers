#!/usr/bin/env python3
"""
RBNZ News Scraper - Simplified and Fixed
Scrapes all news articles from the Reserve Bank of New Zealand website
with working pagination.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import PyPDF2
from io import BytesIO

# Optional Selenium imports for JavaScript handling
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# Configuration
CONFIG = {
    'BASE_URL': 'https://www.rbnz.govt.nz',
    'NEWS_URL': 'https://www.rbnz.govt.nz/news-and-events/news',
    'USER_AGENT': 'rbnz-approved-agent/rg-11701',
    'RATE_LIMIT': 292,  # requests per hour
    'REQUEST_DELAY': 3600 / 292,  # seconds between requests
    'MAX_PAGE': 2,  # Set to None for full scrape, or integer for limited pages
    'OUTPUT_DIR': './data',
    'OUTPUT_FILE': './data/rbnz_news.json',
    'LOG_FILE': './scrape.log',
    'SCRAPED_URLS_FILE': './data/scraped_urls.json'
}

class RBNZScraper:
    def __init__(self, max_pages: Optional[int] = None, use_selenium: bool = False):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': CONFIG['USER_AGENT']})
        self.scraped_urls: Set[str] = self._load_scraped_urls()
        self.max_pages = max_pages or CONFIG['MAX_PAGE']
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.setup_logging()
        self.setup_directories()
        
        if self.use_selenium:
            self.setup_selenium()
        
    def setup_selenium(self):
        """Setup Selenium WebDriver for JavaScript handling"""
        if not SELENIUM_AVAILABLE:
            self.logger.warning("Selenium not available. Install with: pip install selenium webdriver-manager")
            self.use_selenium = False
            return
            
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument(f'--user-agent={CONFIG["USER_AGENT"]}')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        
        self.logger.info("Selenium WebDriver configured")
        
    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(CONFIG['LOG_FILE']),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_directories(self):
        """Create necessary directories"""
        Path(CONFIG['OUTPUT_DIR']).mkdir(exist_ok=True)
        
    def _load_scraped_urls(self) -> Set[str]:
        """Load previously scraped URLs for deduplication"""
        try:
            if os.path.exists(CONFIG['SCRAPED_URLS_FILE']):
                with open(CONFIG['SCRAPED_URLS_FILE'], 'r') as f:
                    return set(json.load(f))
        except Exception as e:
            self.logger.warning(f"Could not load scraped URLs: {e}")
        return set()
        
    def _save_scraped_urls(self):
        """Save scraped URLs to file"""
        try:
            with open(CONFIG['SCRAPED_URLS_FILE'], 'w') as f:
                json.dump(list(self.scraped_urls), f, indent=2)
        except Exception as e:
            self.logger.error(f"Could not save scraped URLs: {e}")
            
    def _rate_limit(self):
        """Implement rate limiting"""
        time.sleep(CONFIG['REQUEST_DELAY'])

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        
        try:
            # Remove extra whitespace and normalize
            text = re.sub(r'\s+', ' ', text.strip())
            # Remove HTML entities
            text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
            # Normalize quotes
            text = text.replace('"', '"').replace('"', '"').replace('„', '"')
            text = text.replace(''', "'").replace(''', "'").replace('`', "'")
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', '', text)
            # Remove excessive punctuation
            text = re.sub(r'\.{3,}', '...', text)
            # Normalize dashes
            text = text.replace('–', '-').replace('—', '-')
        except Exception as e:
            self.logger.warning(f"Text cleaning failed: {e}")
            text = ' '.join(text.split())
            
        return text

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF files"""
        try:
            self._rate_limit()
            response = self.session.get(pdf_url, timeout=60)
            response.raise_for_status()
            
            pdf_reader = PyPDF2.PdfReader(BytesIO(response.content))
            text_content = []
            
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_content.append(self._clean_text(page_text))
                    
            return "\n\n".join(text_content)
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF {pdf_url}: {e}")
            return ""

    def _get_rbnz_page_url(self, page_number: int) -> str:
        """Get the correct RBNZ URL for a specific page"""
        base_url = CONFIG['NEWS_URL']
        
        if page_number == 1:
            return f"{base_url}#sort=%40computedz95xpublisheddate%20descending"
        else:
            first_param = (page_number - 1) * 10
            return f"{base_url}#first={first_param}&sort=%40computedz95xpublisheddate%20descending"

    def _extract_article_links_from_page(self, driver) -> List[str]:
        """Extract all article links from the current page"""
        try:
            # Wait for results to load
            WebDriverWait(driver, 20).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, ".coveo-list-layout.CoveoResult")) > 0
            )
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            links = []
            
            # Find all Coveo result containers
            results = soup.find_all('div', class_='coveo-list-layout CoveoResult')
            
            for result in results:
                # Look for the main article link
                link_elem = result.find('a', class_='CoveoResultLink')
                if link_elem and link_elem.get('href'):
                    url = link_elem['href']
                    if '/hub/news/' in url:
                        if url.startswith('/'):
                            url = urljoin(CONFIG['BASE_URL'], url)
                        links.append(url)
            
            return links
            
        except Exception as e:
            self.logger.error(f"Error extracting links: {e}")
            return []

    def _scrape_all_pages_selenium(self) -> List[str]:
        """Scrape all pages using Selenium with direct URL navigation"""
        if not self.use_selenium:
            return []
            
        all_links = []
        
        try:
            driver = webdriver.Chrome(
                service=webdriver.chrome.service.Service(ChromeDriverManager().install()),
                options=self.chrome_options
            )
            
            page = 1
            while page <= self.max_pages:
                url = self._get_rbnz_page_url(page)
                self.logger.info(f"Scraping page {page}: {url}")
                
                driver.get(url)
                time.sleep(8)  # Give Coveo time to load
                
                # Extract links from this page
                page_links = self._extract_article_links_from_page(driver)
                
                if not page_links:
                    self.logger.info(f"No links found on page {page} - reached end")
                    break
                
                # Check if we're getting new links
                new_links = [link for link in page_links if link not in all_links]
                
                if not new_links and page > 1:
                    self.logger.info(f"No new links on page {page} - reached end")
                    break
                
                all_links.extend(new_links)
                self.logger.info(f"Page {page}: found {len(page_links)} links ({len(new_links)} new). Total: {len(all_links)}")
                
                # Log first few URLs to verify they're different
                if new_links:
                    self.logger.info(f"Sample new URLs: {new_links[:2]}")
                
                page += 1
            
            driver.quit()
            
            # Remove any remaining duplicates
            unique_links = list(dict.fromkeys(all_links))
            self.logger.info(f"Selenium scraping completed. Total unique links: {len(unique_links)}")
            return unique_links
            
        except Exception as e:
            self.logger.error(f"Selenium scraping failed: {e}")
            try:
                driver.quit()
            except:
                pass
            return []

    def _extract_article_content(self, article_url: str) -> Optional[Dict]:
        """Extract content from a single article"""
        if article_url in self.scraped_urls:
            return None
            
        try:
            self._rate_limit()
            response = self.session.get(article_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract basic information
            headline = ""
            headline_elem = soup.find('h1', class_='hero__heading')
            if headline_elem:
                headline = self._clean_text(headline_elem.get_text())
            
            published_date = ""
            date_elem = soup.find('time')
            if date_elem:
                published_date = date_elem.get('datetime', '') or self._clean_text(date_elem.get_text())
                
            description = ""
            desc_elem = soup.find('p', class_='hero__description')
            if desc_elem:
                description = self._clean_text(desc_elem.get_text())
            
            # Extract main content
            content_sections = []
            article_content = soup.find('div', id='article-content')
            if article_content:
                for elem in article_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li']):
                    text = self._clean_text(elem.get_text())
                    if text and len(text) > 10:
                        content_sections.append(text)
                        
            content_text = "\n\n".join(content_sections)
            if description and description not in content_text:
                content_text = description + "\n\n" + content_text
                
            # Extract tags
            news_theme = []
            for tag in soup.find_all('span', class_='tag'):
                theme = self._clean_text(tag.get_text())
                if theme and theme not in news_theme:
                    news_theme.append(theme)
                    
            # Extract related links
            related_links = []
            if article_content:
                for link in article_content.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('/'):
                        href = urljoin(CONFIG['BASE_URL'], href)
                    link_text = self._clean_text(link.get_text())
                    if link_text:
                        related_links.append({
                            'text': link_text,
                            'url': href
                        })
                        
            # Extract image
            associated_image_url = ""
            img_elem = soup.find('img')
            if img_elem and img_elem.get('src'):
                img_url = img_elem['src']
                if img_url.startswith('/'):
                    img_url = urljoin(CONFIG['BASE_URL'], img_url)
                associated_image_url = img_url
                
            # Extract PDF content
            pdf_text = ""
            pdf_links = [link['url'] for link in related_links if link['url'].lower().endswith('.pdf')]
            for pdf_url in set(pdf_links):
                text = self._extract_pdf_text(pdf_url)
                if text:
                    pdf_text += f"\n\n--- PDF CONTENT ---\n\n{text}"
                    
            # Extract tables
            tables_data = []
            for table in soup.find_all('table'):
                table_text = []
                for row in table.find_all('tr'):
                    row_text = []
                    for cell in row.find_all(['td', 'th']):
                        cell_text = self._clean_text(cell.get_text())
                        if cell_text:
                            row_text.append(cell_text)
                    if row_text:
                        table_text.append(" | ".join(row_text))
                if table_text:
                    tables_data.append("\n".join(table_text))
                    
            tables_and_charts_data = "\n\n--- TABLE ---\n\n".join(tables_data)
            
            # Mark as scraped
            self.scraped_urls.add(article_url)
            
            return {
                'url': article_url,
                'headline': headline,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'news_theme': news_theme,
                'content_text': content_text,
                'related_links': related_links,
                'associated_image_url': associated_image_url,
                'pdf_text': pdf_text,
                'tables_and_charts_data': tables_and_charts_data
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting content from {article_url}: {e}")
            return None

    def scrape_all_articles(self) -> List[Dict]:
        """Main scraping method"""
        self.logger.info(f"Starting RBNZ news scraping (max_pages: {self.max_pages})")
        
        # Get all article URLs
        if self.use_selenium:
            self.logger.info("Using Selenium to get article URLs")
            article_urls = self._scrape_all_pages_selenium()
        else:
            self.logger.error("Non-Selenium scraping not implemented - use --use-selenium")
            return []
        
        if not article_urls:
            self.logger.warning("No article URLs found")
            return []
            
        self.logger.info(f"Found {len(article_urls)} total article URLs")
        
        # Extract content from each article
        articles = []
        for i, url in enumerate(article_urls, 1):
            self.logger.info(f"Processing article {i}/{len(article_urls)}: {url}")
            
            article_data = self._extract_article_content(url)
            if article_data:
                articles.append(article_data)
                self.logger.info(f"✓ Scraped: {article_data['headline'][:60]}...")
            else:
                self.logger.warning(f"✗ Failed to scrape: {url}")
                
        self.logger.info(f"Scraping completed. Total articles: {len(articles)}")
        return articles
        
    def save_results(self, articles: List[Dict]):
        """Save scraped articles to JSON file"""
        try:
            # Load existing articles
            existing_articles = []
            if os.path.exists(CONFIG['OUTPUT_FILE']):
                with open(CONFIG['OUTPUT_FILE'], 'r', encoding='utf-8') as f:
                    existing_articles = json.load(f)
                    
            # Merge with new articles
            existing_urls = {article.get('url') for article in existing_articles}
            new_articles = [article for article in articles if article.get('url') not in existing_urls]
            
            all_articles = existing_articles + new_articles
            
            # Save combined results
            with open(CONFIG['OUTPUT_FILE'], 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Saved {len(new_articles)} new articles. Total: {len(all_articles)}")
            
            # Save scraped URLs
            self._save_scraped_urls()
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            
    def run(self):
        """Main execution method"""
        start_time = datetime.now()
        self.logger.info(f"RBNZ scraper started at {start_time}")
        
        try:
            articles = self.scrape_all_articles()
            if articles:
                self.save_results(articles)
            else:
                self.logger.warning("No articles were scraped")
                
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
            
        end_time = datetime.now()
        duration = end_time - start_time
        self.logger.info(f"Scraping completed in {duration}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='RBNZ News Scraper')
    parser.add_argument('--max-pages', type=int, default=2,
                       help='Maximum number of pages to scrape')
    parser.add_argument('--use-selenium', action='store_true',
                       help='Use Selenium WebDriver (required)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--test-url', type=str,
                       help='Test scraping a specific article URL')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.test_url:
        scraper = RBNZScraper(use_selenium=args.use_selenium)
        result = scraper._extract_article_content(args.test_url)
        if result:
            print(f"✓ Successfully scraped: {result['headline']}")
            print(f"  Date: {result['published_date']}")
            print(f"  Content length: {len(result['content_text'])} chars")
        else:
            print("✗ Failed to scrape article")
        return
        
    if not args.use_selenium:
        print("Error: --use-selenium is required for RBNZ scraping")
        return
        
    scraper = RBNZScraper(max_pages=args.max_pages, use_selenium=args.use_selenium)
    scraper.run()


if __name__ == "__main__":
    main()