#!/usr/bin/env python3
"""
NTC News Scraper - Daily Orchestrator Version
Optimized for daily runs with 3-page limit and enhanced orchestrator compatibility
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import sys
import time
import random
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
import re
from typing import List, Dict, Optional, Set
import hashlib

# Stealth libraries with orchestrator-friendly imports
try:
    from fake_useragent import UserAgent
except ImportError:
    # Fallback user agent for orchestrator environments
    class UserAgent:
        @property
        def random(self):
            return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    HTTPAdapter = None
    Retry = None

class NTCNewsScraperDaily:
    def __init__(self):
        self.base_url = "https://www.ntc.gov.au"
        self.news_url = "https://www.ntc.gov.au/news"
        self.session = requests.Session()
        self.ua = UserAgent()
        self.data_folder = "data"
        self.json_file = os.path.join(self.data_folder, "ntc_all_news.json")
        self.csv_file = os.path.join(self.data_folder, "ntc_all_news.csv")
        self.log_file = os.path.join(self.data_folder, "ntc_scraper.log")
        
        # ORCHESTRATOR OPTIMIZATION: Limit pages for daily runs
        self.max_pages = 3
        self.request_delay_min = 1
        self.request_delay_max = 3
        
        # Statistics for orchestrator monitoring
        self.stats = {
            'pages_processed': 0,
            'articles_found': 0,
            'articles_scraped': 0,
            'articles_skipped': 0,
            'errors': 0
        }
        
        # Setup logging
        self.setup_logging()
        
        # Setup session with stealth features
        self.setup_session()
        
        # Create data folder if it doesn't exist
        os.makedirs(self.data_folder, exist_ok=True)
        
        # Load existing data for deduplication
        self.existing_articles = self.load_existing_data()
        
    def setup_logging(self):
        """Setup logging configuration with orchestrator-friendly format"""
        # ORCHESTRATOR OPTIMIZATION: Clear log file for each run to prevent bloat
        log_mode = 'w'  # Overwrite instead of append for daily runs
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, mode=log_mode, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Log run configuration for orchestrator monitoring
        self.logger.info("="*60)
        self.logger.info("NTC News Scraper - Daily Orchestrator Version")
        self.logger.info(f"Max pages to scrape: {self.max_pages}")
        self.logger.info(f"Output directory: {self.data_folder}")
        self.logger.info("="*60)
        
    def is_orchestrator_environment(self):
        """Check if running in orchestrator environment"""
        orchestrator_indicators = [
            'ORCHESTRATOR_ENV',
            'DOCKER_CONTAINER',
            'CI',
            'GITHUB_ACTIONS',
            'JENKINS_URL'
        ]
        
        for indicator in orchestrator_indicators:
            if os.environ.get(indicator):
                return True
        
        # Check if running in headless environment
        if not os.environ.get('DISPLAY') and os.name != 'nt':
            return True
            
        return False
        
    def setup_session(self):
        """Setup session with stealth features and realistic headers"""
        # Setup retry strategy if available
        if HTTPAdapter and Retry:
            retry_strategy = Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"],
                backoff_factor=1
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        else:
            self.logger.warning("Retry strategy not available, using basic session")
        
        # Set realistic headers
        self.update_headers()
        
    def update_headers(self):
        """Update session headers with realistic browser headers"""
        headers = {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Charset': 'utf-8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
        }
        self.session.headers.update(headers)
        
    def random_delay(self, min_delay=None, max_delay=None):
        """Add random delay to mimic human behavior"""
        min_delay = min_delay or self.request_delay_min
        max_delay = max_delay or self.request_delay_max
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
        
    def visit_homepage(self):
        """Visit homepage to establish session and collect cookies"""
        self.logger.info("Visiting homepage to establish session...")
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            self.logger.info(f"Homepage visited successfully. Status: {response.status_code}")
            self.random_delay()
            return True
        except Exception as e:
            self.logger.error(f"Error visiting homepage: {e}")
            self.stats['errors'] += 1
            return False
            
    def load_existing_data(self) -> Set[str]:
        """Load existing articles for deduplication"""
        existing_urls = set()
        if os.path.exists(self.json_file):
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_urls = {article.get('url', '') for article in data}
                self.logger.info(f"Loaded {len(existing_urls)} existing articles for deduplication")
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                self.stats['errors'] += 1
        return existing_urls
        
    def create_article_hash(self, title: str, date: str) -> str:
        """Create a unique hash for an article based on title and date"""
        content = f"{title}{date}".encode('utf-8')
        return hashlib.md5(content).hexdigest()
        
    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Get page content with error handling"""
        try:
            self.logger.debug(f"Fetching: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            self.random_delay()
            return soup
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            self.stats['errors'] += 1
            return None
            
    def extract_links_from_content(self, content_soup) -> List[str]:
        """Extract all links from article content"""
        links = []
        if content_soup:
            for link in content_soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('http'):
                    links.append(href)
                elif href.startswith('/'):
                    links.append(urljoin(self.base_url, href))
        return links
        
    def extract_article_content(self, article_url: str) -> Dict:
        """Extract full article content from article page"""
        soup = self.get_page_content(article_url)
        if not soup:
            return {}
            
        content_data = {
            'content_text': '',
            'content_links': [],
            'images': [],
            'tables': [],
            'charts_graphs': []
        }
        
        # Try multiple selectors based on the actual NTC HTML structure
        content_selectors = [
            # Primary NTC structure
            'div.views-field-body div.field-content',
            'div.feature-article div.field-content', 
            'div.view-content div.field-content',
            # Alternative structures
            'div.field--name-body div.field__item',
            'div.field--name-body',
            'div.field-content',
            'article div.content',
            'main .region-content',
            # Fallback to broader selectors
            'article',
            'main',
            '.layout-content'
        ]
        
        article_content = None
        for selector in content_selectors:
            article_content = soup.select_one(selector)
            if article_content:
                # Check if this content actually has substantial text
                test_text = article_content.get_text(strip=True)
                if len(test_text) > 50:  # Ensure we have meaningful content
                    self.logger.debug(f"Found content using selector: {selector}")
                    break
                else:
                    article_content = None  # Reset if content is too short
        
        # If still no content, try to find content in the main view-content area
        if not article_content:
            view_content = soup.find('div', class_='view-content')
            if view_content:
                # Look for any div with substantial content
                content_divs = view_content.find_all('div', recursive=True)
                for div in content_divs:
                    if div.find('p') or div.find('ul') or div.find('ol'):  # Contains paragraph or list content
                        text_content = div.get_text(strip=True)
                        if len(text_content) > 100:
                            article_content = div
                            self.logger.debug("Found content using view-content search")
                            break
        
        if article_content:
            # Extract text content - focus on meaningful content elements
            content_elements = article_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'blockquote', 'strong'])
            
            # Clean and filter text content
            text_parts = []
            for elem in content_elements:
                elem_text = elem.get_text(strip=True)
                
                # Skip very short content, navigation elements, and metadata
                if (len(elem_text) < 5 or  
                    elem_text.lower() in ['null', 'read more', 'publish date'] or
                    elem_text.lower().startswith(('published:', 'last updated:', 'tags:', 'category:'))):
                    continue
                
                # For list items, add bullet point
                if elem.name == 'li':
                    elem_text = f"• {elem_text}"
                
                text_parts.append(elem_text)
            
            # Join all text parts
            content_text = ' '.join(text_parts)
            
            # Clean up extra whitespace
            content_text = re.sub(r'\s+', ' ', content_text).strip()
            
            content_data['content_text'] = content_text
            
            # Extract links
            content_data['content_links'] = self.extract_links_from_content(article_content)
            
            # Extract images
            images = article_content.find_all('img')
            for img in images:
                img_src = img.get('src', '')
                if img_src:
                    if img_src.startswith('/'):
                        img_src = urljoin(self.base_url, img_src)
                    # Skip common UI icons
                    if not any(icon in img_src.lower() for icon in ['icon', 'arrow', 'chevron', 'button']):
                        content_data['images'].append({
                            'src': img_src,
                            'alt': img.get('alt', ''),
                            'title': img.get('title', '')
                        })
            
            # Extract tables
            tables = article_content.find_all('table')
            for i, table in enumerate(tables):
                table_data = []
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    if row_data and any(cell for cell in row_data):  # Ensure row has content
                        table_data.append(row_data)
                if table_data:
                    content_data['tables'].append({
                        'table_id': i + 1,
                        'data': table_data
                    })
            
            # Look for chart/graph containers
            chart_selectors = [
                '.chart', '.graph', '.visualization', '.highcharts-container',
                '[id*="chart"]', '[class*="chart"]', '[class*="graph"]'
            ]
            
            for selector in chart_selectors:
                charts = article_content.select(selector)
                for chart in charts:
                    chart_text = chart.get_text(strip=True)
                    if len(chart_text) > 10:  # Only include if has meaningful content
                        content_data['charts_graphs'].append({
                            'type': 'chart_container',
                            'html': str(chart)[:500],  # Limit HTML length
                            'text': chart_text[:200]
                        })
        else:
            self.logger.warning(f"No article content found for URL: {article_url}")
            # As a last resort, try to extract from the entire page body
            body = soup.find('body')
            if body:
                # Look for the largest text block
                all_text_elements = body.find_all(['p', 'div'])
                longest_text = ""
                for elem in all_text_elements:
                    elem_text = elem.get_text(strip=True)
                    if len(elem_text) > len(longest_text) and len(elem_text) > 100:
                        longest_text = elem_text
                
                if longest_text:
                    content_data['content_text'] = longest_text
                    self.logger.debug("Used fallback: extracted longest text block")
        
        return content_data
        
    def parse_news_article(self, article_element) -> Optional[Dict]:
        """Parse individual news article from the listing page"""
        try:
            article_data = {}
            
            # Extract title and URL
            title_link = article_element.find('a', class_='title')
            if not title_link:
                return None
                
            article_data['title'] = title_link.get_text(strip=True)
            
            # Extract URL
            href = title_link.get('href', '')
            article_data['url'] = urljoin(self.base_url, href) if href else ''
            
            # Check if already exists
            if article_data['url'] in self.existing_articles:
                self.logger.debug(f"Skipping existing article: {article_data['title'][:50]}...")
                self.stats['articles_skipped'] += 1
                return None
            
            # Extract description
            description_elem = article_element.find('p', class_='description')
            article_data['description'] = description_elem.get_text(strip=True) if description_elem else ''
            
            # Extract date
            date_elem = article_element.find('p', class_='date')
            if date_elem:
                # Remove "Publish date" text and extract just the date
                date_text = date_elem.get_text(strip=True)
                date_text = re.sub(r'Publish date\s*', '', date_text, flags=re.IGNORECASE)
                article_data['published_date'] = date_text.strip()
            else:
                article_data['published_date'] = ''
            
            # Initialize category and type fields
            article_data['category'] = ''
            article_data['type'] = ''
            article_data['tags'] = []
            
            # Try to determine type from title or content
            title_lower = article_data['title'].lower()
            if 'newsletter' in title_lower:
                article_data['type'] = 'Newsletter'
            elif 'media release' in title_lower or 'press release' in title_lower:
                article_data['type'] = 'Media release'
            elif 'update' in title_lower:
                article_data['type'] = 'Update'
            elif 'announcement' in title_lower:
                article_data['type'] = 'Announcement'
            else:
                article_data['type'] = 'Article'
            
            # Add scraping metadata
            article_data['scraped_date'] = datetime.now().isoformat()
            article_data['article_hash'] = self.create_article_hash(
                article_data['title'], 
                article_data['published_date']
            )
            
            self.stats['articles_found'] += 1
            return article_data
            
        except Exception as e:
            self.logger.error(f"Error parsing news article: {e}")
            self.stats['errors'] += 1
            return None
            
    def scrape_all_news(self) -> List[Dict]:
        """Scrape news articles with limited pagination for daily runs"""
        all_articles = []
        
        # Visit homepage first to establish session
        if not self.visit_homepage():
            self.logger.error("Failed to establish session")
            return []
        
        # Start with the main news page
        soup = self.get_page_content(self.news_url)
        if not soup:
            self.logger.error("Failed to get initial news page")
            return []
        
        # ORCHESTRATOR OPTIMIZATION: Limit to max_pages for daily runs
        self.logger.info(f"Scraping maximum {self.max_pages} pages for daily run")
        
        # Scrape each page up to the limit
        for page_num in range(self.max_pages):
            if page_num == 0:
                page_url = self.news_url
            else:
                page_url = f"{self.news_url}?page={page_num}"
            
            self.logger.info(f"Scraping page {page_num + 1}/{self.max_pages}: {page_url}")
            
            if page_num > 0:  # Don't re-fetch the first page
                soup = self.get_page_content(page_url)
                if not soup:
                    self.logger.error(f"Failed to get content for page {page_num + 1}")
                    continue
            
            self.stats['pages_processed'] += 1
            
            # Find all article elements
            articles = soup.find_all('article', class_='node--type-article')
            if not articles:
                self.logger.warning(f"No articles found on page {page_num + 1}")
                continue
            
            self.logger.info(f"Found {len(articles)} articles on page {page_num + 1}")
            
            new_articles_count = 0
            page_articles = []
            
            for article_elem in articles:
                article = self.parse_news_article(article_elem)
                if article:
                    page_articles.append(article)
                    new_articles_count += 1
                    
            self.logger.info(f"Page {page_num + 1}: Found {new_articles_count} new articles")
            
            # Extract full content for new articles
            for article_data in page_articles:
                if article_data['url']:
                    self.logger.info(f"Extracting content for: {article_data['title'][:50]}...")
                    content_data = self.extract_article_content(article_data['url'])
                    article_data.update(content_data)
                    self.stats['articles_scraped'] += 1
                    all_articles.append(article_data)
                    
                    # Add delay between article requests
                    if len(page_articles) > 1:
                        self.random_delay()
            
            # ORCHESTRATOR OPTIMIZATION: Early termination for efficiency
            if new_articles_count == 0 and page_num > 0:
                self.logger.info("No new articles found, considering early termination for daily run")
                # For daily runs, if we hit a page with no new articles, we might want to stop
                # But continue for at least 2 pages to ensure we don't miss anything
                if page_num >= 1:  # Stop after checking at least 2 pages with no new content
                    self.logger.info("Stopping early due to no new content found")
                    break
            
            # Delay between pages (except for the last page)
            if page_num < self.max_pages - 1:
                self.random_delay(2, 4)
        
        self.logger.info(f"Daily scraping completed. Total new articles: {len(all_articles)}")
        return all_articles
        
    def merge_with_existing_data(self, new_articles: List[Dict]) -> List[Dict]:
        """Merge new articles with existing data"""
        existing_data = []
        
        if os.path.exists(self.json_file):
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                self.stats['errors'] += 1
        
        # Combine and sort by scraped_date (newest first)
        all_data = existing_data + new_articles
        all_data.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
        
        return all_data
        
    def save_data(self, articles: List[Dict]):
        """Save articles to JSON and CSV files"""
        if not articles:
            self.logger.info("No new articles to save")
            return 0
            
        # Merge with existing data
        all_articles = self.merge_with_existing_data(articles)
        
        # Save JSON
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved {len(all_articles)} total articles to {self.json_file}")
        except Exception as e:
            self.logger.error(f"Error saving JSON: {e}")
            self.stats['errors'] += 1
        
        # Save CSV
        try:
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                if all_articles:
                    # Flatten complex fields for CSV
                    csv_articles = []
                    for article in all_articles:
                        csv_article = article.copy()
                        # Convert lists and dicts to strings
                        for key, value in csv_article.items():
                            if isinstance(value, (list, dict)):
                                csv_article[key] = json.dumps(value, ensure_ascii=False)
                        csv_articles.append(csv_article)
                    
                    fieldnames = csv_articles[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(csv_articles)
                    
            self.logger.info(f"Saved {len(all_articles)} total articles to {self.csv_file}")
        except Exception as e:
            self.logger.error(f"Error saving CSV: {e}")
            self.stats['errors'] += 1
            
        return len(all_articles)
            
    def print_summary(self) -> int:
        """Print summary of scraping results and return exit code"""
        self.logger.info("\n" + "="*60)
        self.logger.info("SCRAPING SUMMARY")
        self.logger.info("="*60)
        self.logger.info(f"Pages processed: {self.stats['pages_processed']}/{self.max_pages}")
        self.logger.info(f"Articles found: {self.stats['articles_found']}")
        self.logger.info(f"Articles scraped: {self.stats['articles_scraped']}")
        self.logger.info(f"Articles skipped: {self.stats['articles_skipped']}")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info("="*60)
        
        # ORCHESTRATOR OPTIMIZATION: Stdout for orchestrator monitoring
        print(f"NTC News Scraper completed: {self.stats['articles_scraped']} new articles")
        
        if self.stats['errors'] > 0:
            print("WARNINGS: Some errors occurred - check log file")
            return 1
        elif self.stats['articles_scraped'] == 0:
            print("No new articles found")
            return 0
        else:
            print("SUCCESS")
            return 0
            
    def run(self):
        """Main execution method - ORCHESTRATOR COMPATIBLE"""
        self.logger.info("Starting NTC news scraper (daily version)...")
        
        try:
            articles = self.scrape_all_news()
            total_articles = self.save_data(articles)
            exit_code = self.print_summary()
            
            return exit_code
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
            print(f"FATAL ERROR: {e}")
            return 1


def main():
    """Main function to run the scraper - ORCHESTRATOR COMPATIBLE"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape NTC news articles (daily version)')
    parser.add_argument('--max-pages', type=int, default=3,
                        help='Maximum number of pages to scrape (default: 3)')
    
    args = parser.parse_args()
    
    scraper = NTCNewsScraperDaily()
    scraper.max_pages = args.max_pages
    
    # ORCHESTRATOR COMPATIBILITY: Return proper exit code
    exit_code = scraper.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()