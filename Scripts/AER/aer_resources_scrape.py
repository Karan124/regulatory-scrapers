#!/usr/bin/env python3
"""
Australian Energy Regulator (AER) Web Scraper
Extracts guidelines, reviews, schemes, and models from AER website
Designed for LLM analysis with robust deduplication and bot detection handling
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import time
import hashlib
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Set
import re
import PyPDF2
import io
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium_stealth import stealth
from webdriver_manager.chrome import ChromeDriverManager


class AERScraper:
    """
    Robust web scraper for Australian Energy Regulator resources
    """
    
    def __init__(self, run_mode='daily'):
        """
        Initialize AER scraper
        
        Args:
            run_mode (str): 'full' for complete scrape, 'daily' for incremental updates
        """
        self.base_url = "https://www.aer.gov.au"
        self.target_url = "https://www.aer.gov.au/industry/registers/resources"
        self.data_dir = "data"
        self.output_file = os.path.join(self.data_dir, "aer_resources.json")
        self.log_file = os.path.join(self.data_dir, "scraper.log")
        self.run_mode = run_mode
        
        # Initialize logging
        self.setup_logging()
        
        # Create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Load existing data for deduplication
        self.existing_data = self.load_existing_data()
        self.existing_urls = {item.get('url', '') for item in self.existing_data}
        self.existing_hashes = {item.get('content_hash', '') for item in self.existing_data}
        
        # Session for HTTP requests
        self.session = requests.Session()
        self.setup_session()
        
        # Browser driver for JavaScript-heavy pages
        self.driver = None
        
        # Counters for logging
        self.stats = {
            'total_processed': 0,
            'new_items': 0,
            'duplicates_skipped': 0,
            'errors': 0,
            'pdfs_extracted': 0,
            'run_mode': run_mode
        }
    
    def setup_logging(self):
        """Configure logging system"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_session(self):
        """Configure HTTP session with browser-like headers"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
    
    def setup_driver(self):
        """Setup Chrome driver with stealth mode for bot detection avoidance"""
        if self.driver is None:
            options = Options()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins-discovery')
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            options.add_argument('--no-first-run')
            options.add_argument('--no-service-autorun')
            options.add_argument('--password-store=basic')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Add user agent
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            try:
                # Use webdriver-manager to handle ChromeDriver installation
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=options)
                
                # Apply stealth settings
                stealth(self.driver,
                       languages=["en-US", "en"],
                       vendor="Google Inc.",
                       platform="Win32",
                       webgl_vendor="Intel Inc.",
                       renderer="Intel Iris OpenGL Engine",
                       fix_hairline=True,
                       )
                
                self.logger.info("Chrome driver with stealth mode initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize Chrome driver: {e}")
                # Fallback: try without webdriver-manager
                try:
                    self.driver = webdriver.Chrome(options=options)
                    stealth(self.driver,
                           languages=["en-US", "en"],
                           vendor="Google Inc.",
                           platform="Win32",
                           webgl_vendor="Intel Inc.",
                           renderer="Intel Iris OpenGL Engine",
                           fix_hairline=True,
                           )
                    self.logger.info("Chrome driver initialized with fallback method")
                except Exception as e2:
                    self.logger.error(f"Fallback Chrome driver initialization failed: {e2}")
                    raise
    
    def load_existing_data(self) -> List[Dict]:
        """Load existing scraped data for deduplication"""
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                return []
        return []
    
    def save_data(self, data: List[Dict]):
        """Save scraped data to JSON file"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Data saved to {self.output_file}")
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
    
    def generate_content_hash(self, content: str) -> str:
        """Generate hash for content deduplication"""
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def make_request(self, url: str, use_driver: bool = False) -> Optional[BeautifulSoup]:
        """Make HTTP request with bot detection handling"""
        try:
            if use_driver:
                if self.driver is None:
                    self.setup_driver()
                
                self.driver.get(url)
                time.sleep(2)  # Wait for page to load
                html = self.driver.page_source
                return BeautifulSoup(html, 'html.parser')
            else:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            if "403" in str(e) and not use_driver:
                self.logger.info(f"Retrying with browser driver for {url}")
                return self.make_request(url, use_driver=True)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for {url}: {e}")
            return None
    
    def extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF file"""
        try:
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(response.content))
            text_content = []
            
            for page in pdf_reader.pages:
                text_content.append(page.extract_text())
            
            # Clean and format extracted text
            full_text = '\n'.join(text_content)
            full_text = re.sub(r'\s+', ' ', full_text).strip()
            
            self.stats['pdfs_extracted'] += 1
            self.logger.info(f"Successfully extracted text from PDF: {pdf_url}")
            return full_text
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF text from {pdf_url}: {e}")
            return ""
    
    def extract_resource_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract resource page links from listing page"""
        links = []
        
        # Try multiple selectors to find resource cards
        selectors_to_try = [
            # Original selector
            'div.card.card--publication.card--vertical',
            # More specific selector
            'div.node.node--type-resource',
            # Broader selector
            'div.card',
            # Even broader
            '.views-layout__item'
        ]
        
        resource_cards = []
        for selector in selectors_to_try:
            resource_cards = soup.select(selector)
            if resource_cards:
                self.logger.info(f"Found {len(resource_cards)} cards using selector: {selector}")
                break
        
        if not resource_cards:
            self.logger.warning("No resource cards found with any selector")
            # Debug: log the page structure
            main_content = soup.find('div', id='main-content')
            if main_content:
                self.logger.info("Found main-content div")
                view_content = main_content.find('div', class_='view-content')
                if view_content:
                    self.logger.info("Found view-content div")
                    # Log first few div elements to understand structure
                    divs = view_content.find_all('div', limit=5)
                    for i, div in enumerate(divs):
                        classes = div.get('class', [])
                        self.logger.info(f"Div {i}: classes = {classes}")
                else:
                    self.logger.warning("No view-content div found")
            else:
                self.logger.warning("No main-content div found")
            return links
        
        # Extract links from found cards
        for card in resource_cards:
            # Try different link selectors
            link_selectors = [
                'a.stretched-link',
                'a[href*="/industry/registers/resources/"]',
                'h3 a',
                '.card__title a',
                'a'
            ]
            
            link_elem = None
            for link_selector in link_selectors:
                link_elem = card.select_one(link_selector)
                if link_elem and link_elem.get('href'):
                    break
            
            if link_elem and link_elem.get('href'):
                href = link_elem['href']
                # Only process links that look like resource pages
                if '/industry/registers/resources/' in href:
                    full_url = urljoin(self.base_url, href)
                    links.append(full_url)
                    self.logger.debug(f"Found resource link: {full_url}")
        
        self.logger.info(f"Extracted {len(links)} resource links")
        return links
    
    def get_pagination_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract pagination URLs"""
        pagination_urls = []
        
        # Find pagination section
        pagination = soup.find('nav', {'aria-labelledby': 'pagination-heading'})
        if not pagination:
            # Try alternative pagination selectors
            pagination = soup.find('div', class_='pagination__wrapper') or soup.find('ul', class_='pagination')
        
        if pagination:
            page_links = pagination.find_all('a', class_='page-link')
            self.logger.info(f"Found {len(page_links)} pagination links")
            
            for link in page_links:
                href = link.get('href')
                if href and href.startswith('?page='):
                    page_num = href.split('=')[1]
                    try:
                        # Validate it's a number
                        int(page_num)
                        full_url = self.target_url + href
                        pagination_urls.append(full_url)
                    except ValueError:
                        continue
            
            # Also check for the "last" page to get total pages
            last_link = pagination.find('a', title=lambda x: x and 'last page' in x.lower())
            if last_link:
                last_href = last_link.get('href', '')
                if '?page=' in last_href:
                    try:
                        last_page_num = int(last_href.split('=')[1])
                        self.logger.info(f"Total pages detected: {last_page_num + 1}")  # +1 because pages are 0-indexed
                        
                        # Generate all page URLs for full mode
                        if self.run_mode == 'full':
                            all_page_urls = []
                            for page_num in range(last_page_num + 1):
                                if page_num == 0:
                                    continue  # Skip page 0, we already have the base URL
                                page_url = f"{self.target_url}?page={page_num}"
                                all_page_urls.append(page_url)
                            return all_page_urls
                    except (ValueError, IndexError):
                        pass
        else:
            self.logger.warning("No pagination section found")
        
        return list(set(pagination_urls))  # Remove duplicates
    
    def extract_resource_details(self, url: str) -> Optional[Dict]:
        """Extract detailed information from a resource page"""
        soup = self.make_request(url)
        if not soup:
            return None
        
        try:
            # Extract title
            title_elem = soup.find('h1')
            title = title_elem.get_text(strip=True) if title_elem else ""
            
            # Extract main content
            content_blocks = []
            
            # Main body content
            body_elem = soup.find('div', class_='field--name-field-body')
            if body_elem:
                content_blocks.append(body_elem.get_text(strip=True))
            
            # Other content blocks
            block_containers = soup.find_all('div', class_='views-element-container block')
            for container in block_containers:
                block_text = container.get_text(strip=True)
                if block_text:
                    content_blocks.append(block_text)
            
            main_content = '\n\n'.join(content_blocks)
            
            # Extract dates
            date_published = ""
            date_initiated = ""
            
            date_fields = soup.find_all('div', class_='field--label-inline')
            for field in date_fields:
                label = field.find('div', class_='field__label')
                if label:
                    label_text = label.get_text(strip=True)
                    if 'Effective date' in label_text:
                        date_elem = field.find('time')
                        if date_elem:
                            date_published = date_elem.get('datetime', '')
                    elif 'Date initiated' in label_text:
                        date_elem = field.find('time')
                        if date_elem:
                            date_initiated = date_elem.get('datetime', '')
            
            # Extract related links within content
            related_links = []
            content_links = soup.find_all('a', href=True)
            for link in content_links:
                href = link.get('href')
                if href and href.startswith('/'):
                    full_url = urljoin(self.base_url, href)
                    link_text = link.get_text(strip=True)
                    if link_text:
                        related_links.append({
                            'url': full_url,
                            'text': link_text
                        })
            
            # Extract related content section
            related_content = []
            related_section = soup.find('section', class_='page__related')
            if related_section:
                related_cards = related_section.find_all('div', class_='card')
                for card in related_cards:
                    card_title = card.find('h3')
                    card_link = card.find('a', class_='stretched-link')
                    if card_title and card_link:
                        related_content.append({
                            'title': card_title.get_text(strip=True),
                            'url': urljoin(self.base_url, card_link['href'])
                        })
            
            # Extract and process PDF files
            pdf_texts = []
            pdf_links = soup.find_all('a', href=True)
            processed_pdfs = set()
            
            for link in pdf_links:
                href = link.get('href')
                if href and href.endswith('.pdf'):
                    pdf_url = urljoin(self.base_url, href)
                    pdf_hash = self.generate_content_hash(pdf_url)
                    
                    if pdf_hash not in processed_pdfs:
                        processed_pdfs.add(pdf_hash)
                        pdf_text = self.extract_pdf_text(pdf_url)
                        if pdf_text:
                            pdf_texts.append({
                                'url': pdf_url,
                                'text': pdf_text
                            })
            
            # Extract metadata
            sectors = []
            sector_items = soup.find_all('div', class_='field__item')
            for item in sector_items:
                if 'field__item-electricity' in item.get('class', []):
                    sectors.append('Electricity')
                elif 'field__item-gas' in item.get('class', []):
                    sectors.append('Gas')
            
            status = ""
            status_elem = soup.find('div', class_='field--name-field-status')
            if status_elem:
                status_item = status_elem.find('div', class_='field__item')
                if status_item:
                    status = status_item.get_text(strip=True)
            
            # Generate content hash for deduplication
            content_hash = self.generate_content_hash(f"{title}{main_content}")
            
            return {
                'url': url,
                'title': title,
                'main_content': main_content,
                'date_published': date_published,
                'date_initiated': date_initiated,
                'date_scraped': datetime.now().isoformat(),
                'sectors': sectors,
                'status': status,
                'related_links': related_links,
                'related_content': related_content,
                'pdf_texts': pdf_texts,
                'content_hash': content_hash
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting details from {url}: {e}")
            self.stats['errors'] += 1
            return None
    
    def should_process_resource(self, resource_url: str, date_published: str = "") -> bool:
        """
        Determine if a resource should be processed based on run mode
        
        Args:
            resource_url (str): URL of the resource
            date_published (str): Published date of the resource
            
        Returns:
            bool: True if resource should be processed
        """
        # Always process if it's a full run
        if self.run_mode == 'full':
            return True
        
        # For daily runs, check if URL is new
        if resource_url in self.existing_urls:
            return False
        
        # For daily runs, also check if content is recent (last 7 days)
        if date_published and self.run_mode == 'daily':
            try:
                from datetime import datetime, timedelta
                pub_date = datetime.fromisoformat(date_published.replace('Z', '+00:00'))
                cutoff_date = datetime.now() - timedelta(days=7)
                
                # Process if published within last 7 days
                if pub_date < cutoff_date:
                    self.logger.info(f"Skipping old resource (daily mode): {resource_url}")
                    return False
            except Exception as e:
                self.logger.warning(f"Could not parse date {date_published}: {e}")
        
        return True
    
    def get_daily_run_limit(self) -> int:
        """Get the pagination limit for daily runs"""
        if self.run_mode == 'full':
            return None  # No limit for full runs
        else:
            return 3  # Only process first 3 pages for daily runs

    def is_duplicate(self, item: Dict) -> bool:
        """Check if item is a duplicate"""
        url = item.get('url', '')
        content_hash = item.get('content_hash', '')
        
        # In full mode, still check for duplicates but be more lenient
        if self.run_mode == 'full':
            return url in self.existing_urls and content_hash in self.existing_hashes
        
        # In daily mode, be stricter about duplicates
        if url in self.existing_urls or content_hash in self.existing_hashes:
            return True
        return False
    
    def scrape_all_resources(self):
        """Main scraping function"""
        self.logger.info(f"Starting AER resource scraping in {self.run_mode} mode")
        
        # Start with homepage visit to establish session
        self.make_request(self.base_url)
        time.sleep(2)
        
        # Get initial page
        soup = self.make_request(self.target_url)
        if not soup:
            self.logger.error("Failed to access main resources page")
            return
        
        # Collect pagination URLs with mode-specific limits
        all_pagination_urls = [self.target_url]
        pagination_urls = self.get_pagination_urls(soup)
        
        # Apply pagination limits based on run mode
        page_limit = self.get_daily_run_limit()
        if page_limit and len(pagination_urls) > page_limit - 1:  # -1 because we have the main page
            pagination_urls = pagination_urls[:page_limit - 1]
            self.logger.info(f"Limited to {page_limit} pages for {self.run_mode} mode")
        
        all_pagination_urls.extend(pagination_urls)
        
        self.logger.info(f"Found {len(all_pagination_urls)} pages to process in {self.run_mode} mode")
        
        # Collect all resource URLs
        all_resource_urls = []
        for page_url in all_pagination_urls:
            self.logger.info(f"Processing page: {page_url}")
            page_soup = self.make_request(page_url)
            if page_soup:
                resource_links = self.extract_resource_links(page_soup)
                all_resource_urls.extend(resource_links)
                time.sleep(1)  # Rate limiting
        
        # Remove duplicates
        all_resource_urls = list(set(all_resource_urls))
        self.logger.info(f"Found {len(all_resource_urls)} unique resources to process")
        
        # Pre-filter resources for daily mode
        if self.run_mode == 'daily':
            filtered_urls = []
            for url in all_resource_urls:
                if url not in self.existing_urls:
                    filtered_urls.append(url)
            
            self.logger.info(f"Daily mode: {len(filtered_urls)} new URLs to process (filtered from {len(all_resource_urls)})")
            all_resource_urls = filtered_urls
        
        # Process each resource
        new_items = []
        for i, resource_url in enumerate(all_resource_urls, 1):
            self.logger.info(f"Processing resource {i}/{len(all_resource_urls)}: {resource_url}")
            
            resource_data = self.extract_resource_details(resource_url)
            if resource_data:
                self.stats['total_processed'] += 1
                
                # Additional filtering based on run mode
                if not self.should_process_resource(resource_url, resource_data.get('date_published', '')):
                    continue
                
                if self.is_duplicate(resource_data):
                    self.stats['duplicates_skipped'] += 1
                    self.logger.info(f"Skipping duplicate: {resource_url}")
                else:
                    new_items.append(resource_data)
                    self.existing_urls.add(resource_url)
                    self.existing_hashes.add(resource_data['content_hash'])
                    self.stats['new_items'] += 1
                    self.logger.info(f"Added new resource: {resource_data['title']}")
            
            # Rate limiting
            time.sleep(2)
        
        # Combine with existing data and save
        if self.run_mode == 'full':
            # For full runs, replace existing data
            all_data = new_items + [item for item in self.existing_data if item.get('url') not in {new_item.get('url') for new_item in new_items}]
        else:
            # For daily runs, append to existing data
            all_data = self.existing_data + new_items
        
        self.save_data(all_data)
        
        # Log final statistics
        self.logger.info(f"Scraping completed in {self.run_mode} mode")
        self.logger.info(f"Statistics: {self.stats}")
    
    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
        self.session.close()
    
    def run(self):
        """Main execution method"""
        try:
            self.scrape_all_resources()
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error during scraping: {e}")
        finally:
            self.cleanup()


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='AER Web Scraper')
    parser.add_argument('--mode', choices=['full', 'daily'], default='daily',
                       help='Run mode: full (complete scrape) or daily (incremental)')
    parser.add_argument('--force', action='store_true',
                       help='Force full scrape even in daily mode')
    
    args = parser.parse_args()
    
    # Override mode if force flag is used
    run_mode = 'full' if args.force else args.mode
    
    scraper = AERScraper(run_mode=run_mode)
    scraper.run()


if __name__ == "__main__":
    main()