#!/usr/bin/env python3
"""
ASIC Regulatory Resources Comprehensive Scraper

This script scrapes all regulatory resources from ASIC's Regulatory Resources Search
for analysis by Large Language Models (LLMs). It handles all 6 resource types with
proper stealth mechanisms, deduplication, and incremental updates.

Resource Types:
1. Regulatory Guides
2. Information Sheets  
3. Reports
4. Consultations
5. Forms
6. Instruments

Features:
- Stealth browsing with undetected-chromedriver
- PDF text extraction with deduplication
- Accordion/hidden content extraction
- Pagination handling with "Load More" buttons
- Incremental updates (daily runs)
- Comprehensive logging
- Data cleaning for LLM analysis
"""

import asyncio
import json
import logging
import hashlib
import re
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
from urllib.parse import urljoin, urlparse
import os

# Third-party imports
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    from bs4 import BeautifulSoup
    import PyPDF2
    import fitz  # PyMuPDF
    import pandas as pd
    from fake_useragent import UserAgent
    import subprocess
except ImportError as e:
    print(f"Required dependency missing: {e}")
    print("Install with: pip install selenium beautifulsoup4 PyPDF2 PyMuPDF pandas fake-useragent openpyxl")
    exit(1)


class ASICResourceScraper:
    """Main scraper class for ASIC regulatory resources"""
    
    def __init__(self, max_pages: int = None, data_dir: str = "data"):
        self.base_url = "https://www.asic.gov.au"
        self.search_url = f"{self.base_url}/regulatory-resources/regulatory-resources-search/"
        self.max_pages = max_pages  # None for full scrape, integer for limited pages
        self.data_dir = Path(data_dir)
        self.output_file = self.data_dir / "asic_regulatory_resources.json"
        self.log_file = self.data_dir / "asic_resources_scraper.log"
        
        # Create data directory
        self.data_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize data storage
        self.scraped_data: List[Dict] = []
        self.processed_urls: Set[str] = set()
        self.processed_pdfs: Set[str] = set()  # PDF checksums
        
        # Load existing data for incremental updates
        self._load_existing_data()
        
        # Initialize browser
        self.driver = None
        self.session = requests.Session()
        self._setup_session()
        
        # Resource type handlers
        self.resource_handlers = {
            'regulatory-guide': self._scrape_regulatory_guide,
            'information-sheet': self._scrape_information_sheet,
            'report': self._scrape_report,
            'consultation-paper': self._scrape_consultation,
            'form': self._scrape_form,
            'instrument': self._scrape_instrument
        }
    
    def _setup_logging(self):
        """Setup comprehensive logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("ASIC Scraper initialized")
    
    def _setup_session(self):
        """Setup requests session with stealth headers"""
        ua = UserAgent()
        self.session.headers.update({
            'User-Agent': ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _load_existing_data(self):
        """Load existing scraped data for incremental updates"""
        if self.output_file.exists():
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    self.scraped_data = existing_data if isinstance(existing_data, list) else []
                    
                # Build processed URLs set
                for item in self.scraped_data:
                    if 'url' in item:
                        self.processed_urls.add(item['url'])
                    if 'pdf_checksums' in item:
                        self.processed_pdfs.update(item['pdf_checksums'])
                        
                self.logger.info(f"Loaded {len(self.scraped_data)} existing records")
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                self.scraped_data = []
    
    def _init_driver(self):
        """Initialize Chrome driver with stealth options and Linux compatibility"""
        self.driver = self._setup_driver()
        if not self.driver:
            raise Exception("Failed to initialize Chrome driver")
    
    def _setup_driver(self) -> Optional[webdriver.Chrome]:
        """Setup Chrome driver with stealth options and Linux compatibility"""
        chrome_options = Options()
        
        # Essential stability options for Linux/WSL
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=TranslateUI")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        # Stealth options
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Set realistic window size
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Updated user agent to match your Chrome version
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
        
        # Enable headless for faster execution
        chrome_options.add_argument("--headless=new")
        
        # Memory and performance optimizations
        chrome_options.add_argument("--max_old_space_size=4096")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        
        # Network optimizations
        chrome_options.add_argument("--aggressive-cache-discard")
        chrome_options.add_argument("--disable-background-networking")
        
        # Let Selenium find Chrome automatically via PATH
        self.logger.info("Using system default Chrome binary (auto-detection)")
        
        try:
            # Try to find ChromeDriver
            possible_chromedriver_paths = [
                "/usr/bin/chromedriver",
                "/usr/local/bin/chromedriver",
                "/snap/bin/chromedriver"
            ]
            
            chromedriver_path = None
            for path in possible_chromedriver_paths:
                if os.path.exists(path):
                    chromedriver_path = path
                    self.logger.info(f"Found ChromeDriver at: {path}")
                    break
            
            # Initialize driver with improved service configuration
            service_kwargs = {}
            if chromedriver_path:
                service_kwargs['executable_path'] = chromedriver_path
            
            # Add service arguments for better stability
            service_kwargs['service_args'] = [
                '--verbose',
                '--whitelisted-ips=',
                '--disable-dev-shm-usage'
            ]
            
            service = Service(**service_kwargs)
            
            # Initialize driver with explicit service and options
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(30)
            
            # Execute script to remove webdriver property
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info(f"Chrome driver initialized successfully")
            if chromedriver_path:
                self.logger.info(f"Using ChromeDriver: {chromedriver_path}")
            else:
                self.logger.info("Using ChromeDriver from PATH")
            
            return driver
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            
            # Detailed troubleshooting
            self.logger.error("Troubleshooting information:")
            
            # Check if chromedriver is accessible
            try:
                import subprocess
                result = subprocess.run(['chromedriver', '--version'], capture_output=True, text=True, timeout=5)
                self.logger.info(f"ChromeDriver version: {result.stdout.strip()}")
            except Exception as cmd_e:
                self.logger.error(f"Cannot run chromedriver command: {cmd_e}")
            
            # Check Chrome version
            try:
                result = subprocess.run(['google-chrome', '--version'], capture_output=True, text=True, timeout=5)
                self.logger.info(f"Chrome version: {result.stdout.strip()}")
            except Exception as chrome_e:
                logger_msg = f"Cannot run chrome command: {chrome_e}"
                try:
                    # Try alternative Chrome commands
                    result = subprocess.run(['chromium-browser', '--version'], capture_output=True, text=True, timeout=5)
                    self.logger.info(f"Chromium version: {result.stdout.strip()}")
                except:
                    self.logger.error(logger_msg)
            
            return None
    
    def _simulate_human_behavior(self):
        """Simulate human browsing behavior"""
        time.sleep(1 + (time.time() % 2))  # Random delay 1-3 seconds
        
        # Random scroll
        if self.driver:
            self.driver.execute_script("window.scrollTo(0, Math.random() * 500);")
            time.sleep(0.5)
    
    def scrape_all_resources(self):
        """Main method to scrape all regulatory resources"""
        try:
            self.logger.info("Starting comprehensive ASIC regulatory resources scrape")
            
            # Navigate to search page
            self.driver.get(self.search_url)
            self._simulate_human_behavior()
            
            # Wait for page to load and check what we got
            time.sleep(3)
            self.logger.info(f"Navigated to: {self.driver.current_url}")
            self.logger.info(f"Page title: {self.driver.title}")
            
            # Try to find the actual search interface
            search_elements = self.driver.find_elements(By.CSS_SELECTOR, "#filter-search-input, .search-container, .asic-textbox")
            if search_elements:
                self.logger.info(f"Found search interface with {len(search_elements)} elements")
            else:
                self.logger.warning("No search interface found, checking page structure...")
                # Print page source snippet for debugging
                page_source = self.driver.page_source[:1000]
                self.logger.info(f"Page source snippet: {page_source}")
            
            # Get all resource entries
            resource_entries = self._get_all_resource_entries()
            
            if not resource_entries:
                self.logger.warning("No resource entries found")
                # Try alternative approach - look for any ASIC regulatory content
                self._try_alternative_scraping()
                return
            
            self.logger.info(f"Found {len(resource_entries)} total resources to process")
            
            # Process each resource
            processed_count = 0
            for entry in resource_entries:
                try:
                    if self._should_process_entry(entry):
                        self._process_resource_entry(entry)
                        processed_count += 1
                        
                        # Rate limiting
                        if processed_count % 10 == 0:
                            self.logger.info(f"Processed {processed_count} resources...")
                            time.sleep(2)
                            
                except Exception as e:
                    self.logger.error(f"Error processing entry: {e}")
                    continue
            
            self.logger.info(f"Completed processing {processed_count} resources")
            
        except Exception as e:
            self.logger.error(f"Error in main scrape process: {e}")
            raise
        finally:
            self._cleanup()
    
    def _try_alternative_scraping(self):
        """Try alternative methods to find ASIC regulatory content"""
        self.logger.info("Attempting alternative scraping methods...")
        
        # Try different ASIC URLs
        alternative_urls = [
            f"{self.base_url}/regulatory-resources/",
            f"{self.base_url}/regulatory-resources/find-a-document/",
            f"{self.base_url}/regulatory-resources/find-a-document/regulatory-guides/",
            f"{self.base_url}/regulatory-resources/regulatory-resources-search/",
        ]
        
        for url in alternative_urls:
            try:
                self.logger.info(f"Trying alternative URL: {url}")
                self.driver.get(url)
                time.sleep(2)
                
                # Check if this page has resource listings
                resource_links = self.driver.find_elements(By.CSS_SELECTOR, 
                    "a[href*='regulatory-guide'], a[href*='information-sheet'], a[href*='report'], a[href*='consultation'], a[href*='form']")
                
                if resource_links:
                    self.logger.info(f"Found {len(resource_links)} potential resource links at {url}")
                    # Extract and process these links
                    self._process_alternative_links(resource_links)
                    return
                    
            except Exception as e:
                self.logger.warning(f"Error trying {url}: {e}")
                continue
        
        self.logger.error("No alternative scraping methods worked")
    
    def _process_alternative_links(self, links):
        """Process links found through alternative scraping"""
        entries = []
        
        for link in links[:10]:  # Limit to first 10 for testing
            try:
                href = link.get_attribute('href')
                text = link.text.strip()
                
                if href and text:
                    entry = {
                        'type': self._guess_resource_type(href),
                        'title': text,
                        'url': href,
                        'scraped_date': datetime.now().isoformat()
                    }
                    entries.append(entry)
                    
            except Exception as e:
                self.logger.warning(f"Error processing alternative link: {e}")
                continue
        
        # Process the entries
        for entry in entries:
            try:
                self._process_resource_entry(entry)
            except Exception as e:
                self.logger.error(f"Error processing alternative entry: {e}")
                continue
    
    def _guess_resource_type(self, url: str) -> str:
        """Guess resource type from URL"""
        url_lower = url.lower()
        if 'regulatory-guide' in url_lower:
            return 'regulatory-guide'
        elif 'information-sheet' in url_lower:
            return 'information-sheet'
        elif 'report' in url_lower:
            return 'report'
        elif 'consultation' in url_lower:
            return 'consultation-paper'
        elif 'form' in url_lower:
            return 'form'
        elif 'instrument' in url_lower:
            return 'instrument'
        else:
            return 'unknown'
    
    def _get_all_resource_entries(self) -> List[Dict]:
        """Get all resource entries by handling pagination"""
        all_entries = []
        page_count = 0
        
        while True:
            # Check if we've reached max pages limit
            if self.max_pages and page_count >= self.max_pages:
                self.logger.info(f"Reached max pages limit: {self.max_pages}")
                break
            
            # Get current page entries
            entries = self._extract_current_page_entries()
            if not entries:
                self.logger.info("No more entries found")
                break
                
            all_entries.extend(entries)
            page_count += 1
            self.logger.info(f"Page {page_count}: Found {len(entries)} entries")
            
            # Try to load more
            if not self._click_load_more():
                self.logger.info("No more pages to load")
                break
                
            self._simulate_human_behavior()
        
        # Remove duplicates based on URL
        unique_entries = {}
        for entry in all_entries:
            url = entry.get('url')
            if url and url not in unique_entries:
                unique_entries[url] = entry
        
        return list(unique_entries.values())
    
    def _extract_current_page_entries(self) -> List[Dict]:
        """Extract resource entries from current page"""
        entries = []
        
        try:
            # Based on the HTML structure provided, wait for the filter results
            selectors_to_try = [
                # Main resource items based on provided HTML
                "li.regulatory-guide, li.information-sheet, li.report, li.consultation-paper, li.form, li.instrument",
                # Fallback selectors
                ".filter-results li",
                "#filter-results li", 
                ".search-results li",
                "ul li a[href*='regulatory']",
                "li[class*='regulatory'], li[class*='information'], li[class*='report'], li[class*='consultation'], li[class*='form'], li[class*='instrument']"
            ]
            
            resource_items = []
            for i, selector in enumerate(selectors_to_try):
                try:
                    # Wait for content to load
                    wait_time = 10 if i == 0 else 3  # Longer wait for first selector
                    WebDriverWait(self.driver, wait_time).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    resource_items = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if resource_items:
                        self.logger.info(f"Found {len(resource_items)} items with selector: {selector}")
                        break
                except TimeoutException:
                    self.logger.info(f"Selector {selector} timed out")
                    continue
            
            if not resource_items:
                self.logger.warning("No resource items found with any selector")
                # Final fallback - look for the results container and extract any links
                results_container = self.driver.find_elements(By.CSS_SELECTOR, "#filter-summary")
                if results_container:
                    self.logger.info("Found filter summary container, looking for any regulatory links...")
                    all_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/regulatory-resources/'], a[href*='/rg-'], a[href*='/cs-'], a[href*='/info-'], a[href*='/fs']")
                    if all_links:
                        self.logger.info(f"Found {len(all_links)} potential regulatory links")
                        resource_items = all_links[:20]  # Limit for testing
            
            for item in resource_items:
                try:
                    # Skip hidden items
                    if not item.is_displayed():
                        continue
                    
                    entry = self._parse_resource_item(item)
                    if entry:
                        entries.append(entry)
                        
                except Exception as e:
                    self.logger.warning(f"Error parsing resource item: {e}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error extracting page entries: {e}")
        
        return entries
    
    def _parse_resource_item(self, item) -> Optional[Dict]:
        """Parse individual resource item from HTML"""
        try:
            # Handle different item types - list items vs direct links
            if item.tag_name == 'a':
                # Direct link
                href = item.get_attribute('href')
                title_text = item.text.strip()
                resource_type = self._guess_resource_type(href)
                
                return {
                    'type': resource_type,
                    'doc_id': self._extract_doc_id(title_text),
                    'title': title_text,
                    'url': href,
                    'published_date': None,
                    'topics': [],
                    'scraped_date': datetime.now().isoformat()
                }
            
            # Original list item parsing
            # Get resource type from class
            classes = item.get_attribute('class').split()
            resource_type = None
            for cls in classes:
                if cls in ['regulatory-guide', 'information-sheet', 'report', 'consultation-paper', 'form', 'instrument']:
                    resource_type = cls
                    break
            
            # Try to find link within item
            link_elements = item.find_elements(By.CSS_SELECTOR, "a")
            if not link_elements:
                return None
                
            link_element = link_elements[0]
            href = link_element.get_attribute('href')
            title_text = link_element.text.strip()
            
            # If no resource type from class, guess from URL
            if not resource_type:
                resource_type = self._guess_resource_type(href)
            
            # Extract document ID (e.g., RG-140, FS07, CS-027)
            doc_id = self._extract_doc_id(title_text)
            
            # Extract date
            published_date = None
            try:
                date_elements = item.find_elements(By.CSS_SELECTOR, ".filter-date-span, .date, .published")
                if date_elements:
                    date_text = date_elements[0].text.strip()
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', date_text)
                    published_date = date_match.group(1) if date_match else None
            except:
                pass
            
            # Extract topics/tags
            topics = []
            try:
                tag_elements = item.find_elements(By.CSS_SELECTOR, ".filter-tags button, .tags button, .topics button")
                topics = [tag.get_attribute('data-text') or tag.text.strip() for tag in tag_elements if tag.get_attribute('data-text') or tag.text.strip()]
            except:
                pass
            
            return {
                'type': resource_type,
                'doc_id': doc_id,
                'title': title_text,
                'url': href,
                'published_date': published_date,
                'topics': topics,
                'scraped_date': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.warning(f"Error parsing resource item: {e}")
            return None
    
    def _extract_doc_id(self, title_text: str) -> Optional[str]:
        """Extract document ID from title text"""
        doc_id_patterns = [
            r'\b([A-Z]{1,3}-?\d+)\b',  # RG-140, FS07, CS-027
            r'\b(\d{3})\b',            # Three digit numbers like 912
            r'\b(INFO-\d+)\b',         # INFO-009
        ]
        
        for pattern in doc_id_patterns:
            match = re.search(pattern, title_text)
            if match:
                return match.group(1)
        
        return None
    
    def _click_load_more(self) -> bool:
        """Click load more button if available"""
        try:
            load_more_btn = self.driver.find_element(By.ID, "doc-load-more")
            if load_more_btn.is_displayed() and load_more_btn.is_enabled():
                self.driver.execute_script("arguments[0].click();", load_more_btn)
                
                # Wait for new content to load
                time.sleep(3)
                return True
        except NoSuchElementException:
            pass
        except Exception as e:
            self.logger.warning(f"Error clicking load more: {e}")
        
        return False
    
    def _should_process_entry(self, entry: Dict) -> bool:
        """Check if entry should be processed (for incremental updates)"""
        url = entry.get('url')
        if not url:
            return False
        
        # Skip if already processed
        if url in self.processed_urls:
            return False
        
        # For daily runs, only process recent items
        if self.max_pages and self.max_pages <= 3:
            published_date = entry.get('published_date')
            if published_date:
                try:
                    # Parse date and check if it's within last 30 days
                    date_obj = datetime.strptime(published_date, '%d/%m/%Y')
                    if datetime.now() - date_obj > timedelta(days=30):
                        return False
                except:
                    pass
        
        return True
    
    def _process_resource_entry(self, entry: Dict):
        """Process individual resource entry"""
        url = entry['url']
        resource_type = entry['type']
        
        self.logger.info(f"Processing {resource_type}: {entry.get('title', 'Unknown')}")
        
        try:
            # Navigate to resource page
            if not url.startswith('http'):
                url = urljoin(self.base_url, url)
            
            self.driver.get(url)
            self._simulate_human_behavior()
            
            # Call appropriate handler
            handler = self.resource_handlers.get(resource_type)
            if handler:
                content_data = handler(url)
                if content_data:
                    # Merge with entry data
                    entry.update(content_data)
                    entry['url'] = url
                    
                    # Add to processed sets
                    self.processed_urls.add(url)
                    
                    # Store the entry
                    self.scraped_data.append(entry)
                    
                    self.logger.info(f"Successfully processed: {entry.get('title', 'Unknown')}")
                else:
                    self.logger.warning(f"No content extracted for: {url}")
            else:
                self.logger.warning(f"No handler for resource type: {resource_type}")
                
        except Exception as e:
            self.logger.error(f"Error processing resource {url}: {e}")
    
    def _scrape_regulatory_guide(self, url: str) -> Dict:
        """Scrape regulatory guide content"""
        content_data = {
            'content_text': '',
            'relocated_url': None,
            'pdf_links': [],
            'pdf_content': [],
            'pdf_checksums': [],
            'related_links': [],
            'is_withdrawn': False,
            'original_page_content': ''
        }
        
        try:
            # First, extract original page content for debugging
            original_content = self._extract_main_content()
            content_data['original_page_content'] = original_content
            
            # Check for withdrawal and relocation messages
            page_text = self.driver.page_source.lower()
            
            # Look for withdrawal indicators
            if any(phrase in page_text for phrase in ['withdrawn', 'relocated', 'see now']):
                content_data['is_withdrawn'] = True
                self.logger.info(f"Detected withdrawn/relocated guide at {url}")
                
                # Enhanced relocation link detection
                relocation_url = self._find_relocation_link(url)
                
                if relocation_url:
                    content_data['relocated_url'] = relocation_url
                    
                    # Navigate to new URL and scrape content
                    try:
                        self.logger.info(f"Following relocation from {url} to: {relocation_url}")
                        
                        # Validate the relocation URL makes sense
                        if not self._validate_relocation_url(url, relocation_url):
                            self.logger.warning(f"Relocation URL seems incorrect: {relocation_url}")
                        
                        self.driver.get(relocation_url)
                        self._simulate_human_behavior()
                        
                        # Wait for new page to load
                        time.sleep(3)
                        
                        # Verify we're on the correct page
                        current_url = self.driver.current_url
                        page_title = self.driver.title
                        self.logger.info(f"Navigated to: {current_url}")
                        self.logger.info(f"Page title: {page_title}")
                        
                        # Extract content from relocated page
                        relocated_content = self._extract_main_content()
                        content_data['content_text'] = relocated_content
                        
                        # Validate content makes sense for the original guide
                        if self._validate_relocated_content(url, relocated_content):
                            self.logger.info(f"Successfully extracted relevant content from relocated page: {len(relocated_content)} characters")
                        else:
                            self.logger.warning(f"Relocated content may not be relevant to original guide")
                        
                        # Extract other content from relocated page
                        related_links = self._extract_related_links()
                        content_data['related_links'] = related_links
                        
                        pdf_info = self._extract_pdf_content()
                        content_data.update(pdf_info)
                        
                    except Exception as nav_e:
                        self.logger.error(f"Error navigating to relocated URL {relocation_url}: {nav_e}")
                        # Fall back to extracting what we can from current page
                        content_data['content_text'] = original_content
                else:
                    # No relocation link found, extract current page content
                    self.logger.warning(f"No relocation link found for withdrawn guide: {url}")
                    content_data['content_text'] = original_content
            else:
                # Not withdrawn/relocated, extract normally
                content_data['content_text'] = original_content
                
                # Extract related links
                related_links = self._extract_related_links()
                content_data['related_links'] = related_links
                
                # Extract PDF links and content
                pdf_info = self._extract_pdf_content()
                content_data.update(pdf_info)
            
        except Exception as e:
            self.logger.error(f"Error scraping regulatory guide {url}: {e}")
            # Ensure we at least get some content
            try:
                content_data['content_text'] = self._extract_main_content()
            except:
                pass
        
        return content_data
    
    def _find_relocation_link(self, original_url: str) -> Optional[str]:
        """Find the relocation link with improved detection"""
        # Try very specific selectors first for known patterns
        specific_selectors = [
            # Look for the exact link from RG-154
            "//a[@id='menur77m']",
            "//a[contains(@href, 'certificates-issued-by-a-qualified-accountant')]",
            "//a[contains(@title, 'Certificates issued by a qualified accountant')]",
            "//a[contains(text(), 'Certificates issued by a qualified accountant')]",
        ]
        
        # Try specific selectors first
        for selector in specific_selectors:
            try:
                relocation_links = self.driver.find_elements(By.XPATH, selector)
                for link in relocation_links:
                    href = link.get_attribute('href')
                    link_text = link.text.strip()
                    link_id = link.get_attribute('id')
                    link_title = link.get_attribute('title')
                    
                    self.logger.info(f"Found specific link - ID: '{link_id}', Text: '{link_text}', Title: '{link_title}', href: '{href}'")
                    
                    if href and href != original_url:
                        # Make URL absolute if needed
                        if not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        
                        # For specific selectors, validate less strictly
                        if self._is_specific_relocation_link(href, link_text, link_title):
                            self.logger.info(f"Selected specific relocation link: {href}")
                            return href
                            
            except Exception as e:
                self.logger.warning(f"Error with specific selector {selector}: {e}")
                continue
        
        # Fallback to general selectors
        general_selectors = [
            "//p[contains(text(), 'See now')]/a[1]",
            "//a[contains(@href, '/regulatory-resources/financial-services/')]",
            "//a[contains(@href, '/regulatory-resources/') and not(contains(@href, '/find-a-document/')) and not(contains(@href, '/consultation'))]",
        ]
        
        for selector in general_selectors:
            try:
                relocation_links = self.driver.find_elements(By.XPATH, selector)
                for link in relocation_links:
                    href = link.get_attribute('href')
                    link_text = link.text.strip()
                    
                    if href and href != original_url:
                        # Make URL absolute if needed
                        if not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        
                        self.logger.info(f"Found general relocation link: {href} (text: '{link_text}')")
                        
                        # Validate this looks like a real relocation
                        if self._is_valid_relocation_link(href, link_text):
                            return href
                            
            except Exception as e:
                self.logger.warning(f"Error with general selector {selector}: {e}")
                continue
        
        return None
    
    def _is_specific_relocation_link(self, href: str, link_text: str, link_title: str) -> bool:
        """Validate specific relocation links with less strict requirements"""
        # Must be within ASIC domain
        if not ('asic.gov.au' in href):
            return False
            
        # Skip obviously wrong links
        invalid_patterns = [
            'javascript:',
            'mailto:',
            '#'
        ]
        
        for pattern in invalid_patterns:
            if pattern in href.lower():
                return False
        
        # For specific links, we're more trusting
        return True
    
    def _validate_relocated_content(self, original_url: str, content: str) -> bool:
        """Validate if relocated content is relevant to the original guide"""
        if not content or len(content) < 100:
            return False
        
        # Extract document ID from original URL  
        doc_id_match = re.search(r'rg-(\d+)', original_url.lower())
        if doc_id_match:
            doc_id = doc_id_match.group(1)
            content_lower = content.lower()
            
            # For RG-154, content should be about certificates/qualified accountants
            if doc_id == '154':
                expected_topics = [
                    'certificate', 
                    'qualified accountant', 
                    'accountant',
                    'gross income',
                    'net assets',
                    '250,000',
                    '2.5 million',
                    'professional bodies',
                    'disclosure document',
                    'corporations act'
                ]
                
                topic_matches = [topic for topic in expected_topics if topic in content_lower]
                
                # Red flags - content that suggests we're on the wrong page
                wrong_topics = [
                    'legislative instrument', 
                    'consultation', 
                    'sunset', 
                    'remake',
                    'comments close',
                    'seeking feedback',
                    'proposal to remake'
                ]
                
                wrong_matches = [topic for topic in wrong_topics if topic in content_lower]
                
                if wrong_matches:
                    self.logger.error(f"Content contains wrong topics for RG-154: {wrong_matches}")
                    self.logger.error(f"Content preview: {content[:500]}...")
                    return False
                
                if topic_matches:
                    self.logger.info(f"Content contains expected topics for RG-154: {topic_matches}")
                    return True
                else:
                    self.logger.warning(f"Content does not contain expected topics for RG-154")
                    self.logger.warning(f"Content preview: {content[:500]}...")
                    return False
        
        return True  # Default to allowing it
    
    def _scrape_information_sheet(self, url: str) -> Dict:
        """Scrape information sheet content with accordion extraction"""
        content_data = {
            'content_text': '',
            'accordion_content': [],
            'pdf_links': [],
            'pdf_content': [],
            'pdf_checksums': [],
            'related_links': []
        }
        
        try:
            # Extract main content
            content_data['content_text'] = self._extract_main_content()
            
            # Extract accordion/hidden content
            accordion_content = self._extract_accordion_content()
            content_data['accordion_content'] = accordion_content
            
            # Extract related links
            related_links = self._extract_related_links()
            content_data['related_links'] = related_links
            
            # Extract PDF content
            pdf_info = self._extract_pdf_content()
            content_data.update(pdf_info)
            
        except Exception as e:
            self.logger.error(f"Error scraping information sheet {url}: {e}")
        
        return content_data
    
    def _scrape_report(self, url: str) -> Dict:
        """Scrape report content"""
        content_data = {
            'content_text': '',
            'pdf_links': [],
            'pdf_content': [],
            'pdf_checksums': [],
            'related_links': []
        }
        
        try:
            # Extract main content
            content_data['content_text'] = self._extract_main_content()
            
            # Extract related links
            related_links = self._extract_related_links()
            content_data['related_links'] = related_links
            
            # Extract PDF content
            pdf_info = self._extract_pdf_content()
            content_data.update(pdf_info)
            
        except Exception as e:
            self.logger.error(f"Error scraping report {url}: {e}")
        
        return content_data
    
    def _scrape_consultation(self, url: str) -> Dict:
        """Scrape consultation content"""
        content_data = {
            'content_text': '',
            'pdf_links': [],
            'pdf_content': [],
            'pdf_checksums': [],
            'submission_deadline': None,
            'submission_email': None,
            'related_links': []
        }
        
        try:
            # Extract main content
            content_data['content_text'] = self._extract_main_content()
            
            # Extract submission details
            submission_info = self._extract_submission_details()
            content_data.update(submission_info)
            
            # Extract related links
            related_links = self._extract_related_links()
            content_data['related_links'] = related_links
            
            # Extract PDF content
            pdf_info = self._extract_pdf_content()
            content_data.update(pdf_info)
            
        except Exception as e:
            self.logger.error(f"Error scraping consultation {url}: {e}")
        
        return content_data
    
    def _scrape_form(self, url: str) -> Dict:
        """Scrape form content (text only, no form files)"""
        content_data = {
            'content_text': '',
            'form_fields': {},
            'tables': [],
            'related_links': []
        }
        
        try:
            # Extract main content
            content_data['content_text'] = self._extract_main_content()
            
            # Extract comprehensive form field information
            form_fields = self._extract_form_fields()
            content_data['form_fields'] = form_fields
            
            # Extract tables
            tables = self._extract_tables()
            content_data['tables'] = tables
            
            # Extract related links
            related_links = self._extract_related_links()
            content_data['related_links'] = related_links
            
        except Exception as e:
            self.logger.error(f"Error scraping form {url}: {e}")
        
        return content_data
    
    def _scrape_instrument(self, url: str) -> Dict:
        """Scrape instrument content from legislation.gov.au"""
        content_data = {
            'content_text': '',
            'legislative_instrument_content': '',
            'explanatory_statement_content': '',
            'combined_iframe_content': '',
            'pdf_content': '',
            'pdf_checksum': None,
            'is_legislation_gov_au': False,
            'document_types_extracted': []
        }
        
        try:
            # Check if this is a legislation.gov.au URL
            if 'legislation.gov.au' not in url:
                content_data['content_text'] = self._extract_main_content()
                return content_data
            
            content_data['is_legislation_gov_au'] = True
            self.logger.info(f"Processing legislation.gov.au instrument: {url}")
            
            # Extract main HTML content (navigation/header)
            content_data['content_text'] = self._extract_main_content()
            
            # Extract iframe content (both document types)
            iframe_content = self._extract_legislation_iframe_content()
            if iframe_content:
                content_data['combined_iframe_content'] = iframe_content
                
                # Split combined content into parts
                parts = self._split_instrument_content(iframe_content)
                content_data['legislative_instrument_content'] = parts.get('legislative_instrument', '')
                content_data['explanatory_statement_content'] = parts.get('explanatory_statement', '')
                content_data['document_types_extracted'] = list(parts.keys())
                
                self.logger.info(f"Extracted {len(parts)} document types from instrument")
            
            # Fallback: try PDF download if iframe extraction failed
            if not iframe_content:
                self.logger.info("Iframe extraction failed, trying PDF download fallback")
                pdf_content = self._extract_instrument_pdf_fallback()
                if pdf_content:
                    content_data['pdf_content'] = pdf_content
                    content_data['pdf_checksum'] = hashlib.md5(pdf_content.encode()).hexdigest()
            
        except Exception as e:
            self.logger.error(f"Error scraping instrument {url}: {e}")
        
        return content_data
    
    def _extract_legislation_iframe_content(self) -> str:
        """Extract content from legislation.gov.au iframe - both Legislative instrument and Explanatory statement"""
        try:
            all_content = {}
            
            # Document types to extract
            content_types = [
                ("Legislative instrument", "legislative_instrument"),
                ("Explanatory statement", "explanatory_statement")
            ]
            
            for display_name, content_key in content_types:
                self.logger.info(f"Extracting {display_name} content...")
                
                # Select the document type from dropdown
                if self._select_instrument_document_type(display_name):
                    # Extract content for this document type
                    content = self._extract_iframe_content()
                    if content:
                        all_content[content_key] = content
                        self.logger.info(f"Successfully extracted {len(content)} characters for {display_name}")
                    else:
                        self.logger.warning(f"No content found for {display_name}")
                else:
                    self.logger.warning(f"Could not select {display_name} from dropdown")
            
            # Combine all content
            if all_content:
                combined_content = ""
                for content_type, content in all_content.items():
                    combined_content += f"\n\n=== {content_type.replace('_', ' ').title()} ===\n\n"
                    combined_content += content
                
                self.logger.info(f"Combined content from {len(all_content)} document types: {len(combined_content)} total characters")
                return combined_content
            
            # Fallback: try to get any iframe content without switching
            self.logger.info("Fallback: extracting current iframe content without dropdown selection")
            iframe_content = self._extract_iframe_content()
            return iframe_content or ""
            
        except Exception as e:
            self.logger.warning(f"Error extracting legislation iframe content: {e}")
            return ""
    
    def _select_instrument_document_type(self, target_type: str) -> bool:
        """Select document type from dropdown (Legislative instrument or Explanatory statement)"""
        try:
            self.logger.info(f"Attempting to select '{target_type}' from dropdown")
            
            # Find and click the dropdown toggle button
            dropdown_selectors = [
                "button#FRL_COMPONENT_Select",
                "button[id*='FRL_COMPONENT']",
                "button.dropdown-toggle",
                "[ngbdropdowntoggle]"
            ]
            
            dropdown_button = None
            for selector in dropdown_selectors:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for button in buttons:
                        # Check if this looks like the document type dropdown
                        aria_describedby = button.get_attribute('aria-describedby') or ''
                        button_id = button.get_attribute('id') or ''
                        
                        if 'FRL_COMPONENT' in aria_describedby + button_id:
                            dropdown_button = button
                            self.logger.info(f"Found dropdown button: {button_id}")
                            break
                    if dropdown_button:
                        break
                except:
                    continue
            
            if not dropdown_button:
                self.logger.warning("Could not find dropdown button")
                return False
            
            # Check if dropdown is already expanded
            is_expanded = dropdown_button.get_attribute('aria-expanded') == 'true'
            
            if not is_expanded:
                # Click to open dropdown
                self.logger.info("Opening dropdown menu")
                self.driver.execute_script("arguments[0].click();", dropdown_button)
                time.sleep(1)
            
            # Find and click the target option
            option_selectors = [
                "button.dropdown-item",
                "[ngbdropdownitem]",
                ".dropdown-menu button"
            ]
            
            target_selected = False
            for selector in option_selectors:
                try:
                    options = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for option in options:
                        option_text = option.text.strip()
                        self.logger.info(f"Found dropdown option: '{option_text}'")
                        
                        if target_type.lower() in option_text.lower():
                            self.logger.info(f"Clicking option: '{option_text}'")
                            self.driver.execute_script("arguments[0].click();", option)
                            time.sleep(2)  # Wait for content to load
                            target_selected = True
                            break
                    
                    if target_selected:
                        break
                except Exception as e:
                    self.logger.warning(f"Error with option selector {selector}: {e}")
                    continue
            
            if target_selected:
                self.logger.info(f"Successfully selected '{target_type}'")
                return True
            else:
                self.logger.warning(f"Could not find option for '{target_type}'")
                return False
                
        except Exception as e:
            self.logger.error(f"Error selecting document type '{target_type}': {e}")
            return False
    
    def _extract_iframe_content(self) -> str:
        """Extract content from iframe (handles blob URLs)"""
        try:
            # Look for the main content iframe
            iframe_selectors = [
                "iframe#epubFrame",
                "iframe[name='epubFrame']", 
                "iframe[title*='Document']",
                "iframe[src*='blob:']",
                "iframe"
            ]
            
            iframe_element = None
            for selector in iframe_selectors:
                try:
                    iframes = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for iframe in iframes:
                        # Check if this looks like a content iframe
                        iframe_id = iframe.get_attribute('id')
                        iframe_name = iframe.get_attribute('name') 
                        iframe_title = iframe.get_attribute('title')
                        
                        # Prioritize content iframes
                        if any(indicator in str(iframe_id).lower() + str(iframe_name).lower() + str(iframe_title).lower() 
                               for indicator in ['epub', 'document', 'content', 'text']):
                            iframe_element = iframe
                            self.logger.info(f"Selected content iframe: {iframe_id or iframe_name}")
                            break
                    
                    if iframe_element:
                        break
                except:
                    continue
            
            if not iframe_element:
                self.logger.warning("No content iframe found")
                return ""
            
            # Switch to iframe and extract content
            self.logger.info("Switching to iframe to extract content")
            self.driver.switch_to.frame(iframe_element)
            
            # Wait a moment for iframe content to load
            time.sleep(2)
            
            # Try to extract text from iframe
            iframe_content = ""
            
            # Try multiple strategies to get iframe content
            content_strategies = [
                # Strategy 1: Look for main content containers
                lambda: self._extract_from_iframe_selectors([
                    "main", "article", ".content", "#content", 
                    ".document-content", ".legislation-content", 
                    ".text-content", "body"
                ]),
                
                # Strategy 2: Get all visible text
                lambda: self.driver.find_element(By.TAG_NAME, "body").text.strip(),
                
                # Strategy 3: Get page source and parse
                lambda: self._extract_text_from_html_source(self.driver.page_source)
            ]
            
            for i, strategy in enumerate(content_strategies):
                try:
                    self.logger.info(f"Trying iframe content extraction strategy {i+1}")
                    content = strategy()
                    if content and len(content) > 200:  # Substantial content
                        iframe_content = content
                        self.logger.info(f"Strategy {i+1} successful: extracted {len(content)} characters")
                        break
                except Exception as e:
                    self.logger.warning(f"Strategy {i+1} failed: {e}")
                    continue
            
            # Switch back to main content
            self.driver.switch_to.default_content()
            
            if iframe_content:
                return self._clean_text(iframe_content)
            else:
                self.logger.warning("No substantial content found in iframe")
                return ""
                
        except Exception as e:
            # Make sure we switch back to main content
            try:
                self.driver.switch_to.default_content()
            except:
                pass
            self.logger.error(f"Error extracting iframe content: {e}")
            return ""
    
    def _extract_from_iframe_selectors(self, selectors: list) -> str:
        """Try to extract content using multiple selectors within iframe"""
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    content = elements[0].text.strip()
                    if len(content) > 200:  # Substantial content
                        self.logger.info(f"Found iframe content using selector: {selector}")
                        return content
            except:
                continue
        return ""
    
    def _extract_text_from_html_source(self, html_source: str) -> str:
        """Extract text from HTML source using basic parsing"""
        try:
            # Remove script and style elements
            html_source = re.sub(r'<script[^>]*>.*?</script>', '', html_source, flags=re.DOTALL | re.IGNORECASE)
            html_source = re.sub(r'<style[^>]*>.*?</style>', '', html_source, flags=re.DOTALL | re.IGNORECASE)
            
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', ' ', html_source)
            
            # Clean up whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text
        except Exception as e:
            self.logger.warning(f"Error extracting text from HTML: {e}")
            return ""
    
    def _extract_instrument_pdf_fallback(self) -> str:
        """Fallback method to extract PDF content if iframe extraction fails"""
        try:
            # Try to construct PDF URL from current page URL
            current_url = self.driver.current_url
            if '/latest/text' in current_url:
                pdf_url = current_url.replace('/latest/text', '/latest/text.pdf')
                self.logger.info(f"Trying constructed PDF URL: {pdf_url}")
                
                # Test if PDF URL is accessible and download
                try:
                    response = self.session.head(pdf_url, timeout=10)
                    if response.status_code == 200:
                        self.logger.info(f"Constructed PDF URL is accessible")
                        return self._download_and_extract_pdf(pdf_url)
                except:
                    pass
            
            # Look for PDF download buttons
            pdf_button_selectors = [
                "button[title*='Download PDF']",
                "button.btn-pdf-volume-download",
                ".btn-circle[title*='PDF']",
                "a[href*='.pdf']"
            ]
            
            for selector in pdf_button_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and '.pdf' in href:
                            self.logger.info(f"Found PDF download link: {href}")
                            return self._download_and_extract_pdf(href)
                except:
                    continue
            
            self.logger.warning("No PDF fallback method worked")
            return ""
            
        except Exception as e:
            self.logger.warning(f"Error in PDF fallback extraction: {e}")
            return ""
    
    def _split_instrument_content(self, combined_content: str) -> dict:
        """Split combined content back into parts"""
        parts = {}
        
        try:
            # Look for section markers
            legislative_marker = "=== Legislative Instrument ==="
            explanatory_marker = "=== Explanatory Statement ==="
            
            if legislative_marker in combined_content:
                sections = combined_content.split(legislative_marker)
                if len(sections) > 1:
                    legislative_section = sections[1]
                    
                    if explanatory_marker in legislative_section:
                        leg_parts = legislative_section.split(explanatory_marker)
                        parts['legislative_instrument'] = leg_parts[0].strip()
                        if len(leg_parts) > 1:
                            parts['explanatory_statement'] = leg_parts[1].strip()
                    else:
                        parts['legislative_instrument'] = legislative_section.strip()
            
            elif explanatory_marker in combined_content:
                sections = combined_content.split(explanatory_marker)
                if len(sections) > 1:
                    parts['explanatory_statement'] = sections[1].strip()
            
            # If no markers found, treat entire content as legislative instrument
            if not parts and combined_content.strip():
                parts['legislative_instrument'] = combined_content.strip()
            
        except Exception as e:
            self.logger.warning(f"Error splitting combined content: {e}")
            # Fallback: return entire content as legislative instrument
            if combined_content.strip():
                parts['legislative_instrument'] = combined_content.strip()
        
        return parts
    
    def _extract_main_content(self) -> str:
        """Extract main text content from page"""
        try:
            # Wait for main content to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "main"))
            )
            
            # Try multiple selectors for main content
            content_selectors = [
                "main .asic-page__article",
                "main",
                ".asic-container .asic-page__main",
                ".content-wrapper",
                ".page-content"
            ]
            
            content_text = ""
            for selector in content_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        content_text = elements[0].text.strip()
                        break
                except:
                    continue
            
            if not content_text:
                # Fallback to body text
                content_text = self.driver.find_element(By.TAG_NAME, "body").text.strip()
            
            # Clean the text
            return self._clean_text(content_text)
            
        except Exception as e:
            self.logger.warning(f"Error extracting main content: {e}")
            return ""
    
    def _extract_accordion_content(self) -> List[Dict]:
        """Extract hidden/accordion content"""
        accordion_data = []
        
        try:
            # Find accordion buttons
            accordion_buttons = self.driver.find_elements(By.CSS_SELECTOR, 
                "button[aria-expanded], button[aria-controls], .accordion-button, .collapsible-button")
            
            for button in accordion_buttons:
                try:
                    # Get button text before clicking
                    button_text = self._clean_text(button.text.strip())
                    
                    # Skip buttons with no meaningful text
                    if not button_text or len(button_text) < 2:
                        continue
                    
                    # Check if button is collapsed
                    expanded = button.get_attribute('aria-expanded')
                    if expanded == 'false' or not expanded:
                        try:
                            # Click to expand
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(0.5)
                        except Exception as click_e:
                            self.logger.warning(f"Could not click accordion button: {click_e}")
                            continue
                    
                    # Get associated content
                    content_text = ""
                    controls = button.get_attribute('aria-controls')
                    if controls:
                        try:
                            content_element = self.driver.find_element(By.ID, controls)
                            content_text = self._clean_text(content_element.text.strip())
                        except Exception as content_e:
                            self.logger.warning(f"Could not find content element: {content_e}")
                    
                    # Only add if we have meaningful content
                    if button_text and (content_text and len(content_text) > 10):
                        accordion_data.append({
                            'button_text': button_text,
                            'content': content_text
                        })
                    
                except Exception as e:
                    self.logger.warning(f"Error processing accordion item: {e}")
                    continue
            
        except Exception as e:
            self.logger.warning(f"Error extracting accordion content: {e}")
        
        return accordion_data
    
    def _extract_submission_details(self) -> Dict:
        """Extract submission deadline and email for consultations"""
        details = {
            'submission_deadline': None,
            'submission_email': None
        }
        
        try:
            page_text = self.driver.page_source
            
            # Extract deadline
            deadline_patterns = [
                r'by\s+(\d{1,2}/\d{1,2}/\d{4})',
                r'close[s]?\s+(\d{1,2}\s+\w+\s+\d{4})',
                r'(\d{1,2}\s+\w+\s+\d{4})'
            ]
            
            for pattern in deadline_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    details['submission_deadline'] = match.group(1)
                    break
            
            # Extract email
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', page_text)
            if email_match:
                details['submission_email'] = email_match.group(1)
            
        except Exception as e:
            self.logger.warning(f"Error extracting submission details: {e}")
        
        return details
    
    def _extract_form_fields(self) -> Dict:
        """Extract form field information from the comprehensive table structure"""
        fields = {}
        
        try:
            # Look for the main form display table with different possible selectors
            form_tables = self.driver.find_elements(By.CSS_SELECTOR, 
                "table.formdisplay, .asic-table, table[summary*='Search results'], table[class*='form'], table[border='0'][cellspacing='0'][cellpadding='0']")
            
            if not form_tables:
                # Fallback to any table
                form_tables = self.driver.find_elements(By.CSS_SELECTOR, "table")
            
            for table in form_tables:
                try:
                    # Skip if this is clearly not a form table
                    table_text = table.text.strip()
                    if not any(keyword in table_text.lower() for keyword in ['description', 'purpose', 'form', 'lodging', 'fees']):
                        continue
                    
                    # Get all rows including those in thead and tbody
                    rows = table.find_elements(By.CSS_SELECTOR, "tr")
                    
                    for row in rows:
                        try:
                            cells = row.find_elements(By.CSS_SELECTOR, "td, th")
                            if len(cells) < 2:
                                continue
                            
                            # Skip header rows that just contain form number
                            if len(cells) >= 1 and 'Form ' in cells[0].text and len(cells[0].text.strip()) < 20:
                                continue
                            
                            # Check if first cell contains a field label (bold text or strong tag)
                            first_cell = cells[0]
                            field_name_elements = first_cell.find_elements(By.CSS_SELECTOR, "strong")
                            
                            if field_name_elements:
                                # This is a field definition row
                                field_name = self._clean_text(field_name_elements[0].text.strip())
                                
                                if not field_name or field_name.lower() in ['form', 'fs08', 'fs71', '207z']:
                                    continue
                                
                                # Determine how to extract the value based on cell structure
                                if len(cells) == 4:
                                    # Four-column layout - check if this is a dual-field row
                                    second_cell = cells[1]
                                    third_cell = cells[2]
                                    fourth_cell = cells[3]
                                    
                                    # Check if third cell has a strong tag (another field name)
                                    third_cell_strong = third_cell.find_elements(By.CSS_SELECTOR, "strong")
                                    
                                    if third_cell_strong and second_cell.text.strip():
                                        # This is a dual-field row (e.g., Lodging Period | Late Fees)
                                        first_field_value = self._extract_cell_content(second_cell)
                                        second_field_name = self._clean_text(third_cell_strong[0].text.strip())
                                        second_field_value = self._extract_cell_content(fourth_cell)
                                        
                                        if first_field_value:
                                            fields[field_name] = first_field_value
                                        if second_field_name and second_field_value:
                                            fields[second_field_name] = second_field_value
                                    else:
                                        # Single field spanning multiple columns
                                        field_value = ""
                                        for i in range(1, len(cells)):
                                            cell_content = self._extract_cell_content(cells[i])
                                            if cell_content:
                                                field_value += cell_content + " "
                                        
                                        field_value = field_value.strip()
                                        if field_value:
                                            fields[field_name] = field_value
                                
                                elif len(cells) >= 2:
                                    # Two or three column layout
                                    field_value = ""
                                    for i in range(1, len(cells)):
                                        cell_content = self._extract_cell_content(cells[i])
                                        if cell_content:
                                            field_value += cell_content + " "
                                    
                                    field_value = field_value.strip()
                                    if field_value:
                                        fields[field_name] = field_value
                        
                        except Exception as row_e:
                            self.logger.warning(f"Error processing table row: {row_e}")
                            continue
                
                except Exception as table_e:
                    self.logger.warning(f"Error processing form table: {table_e}")
                    continue
            
            # Clean up and validate fields
            cleaned_fields = {}
            for key, value in fields.items():
                clean_key = key.replace(':', '').strip()
                clean_value = value.replace('\n', ' ').strip()
                
                # Only include meaningful fields
                if (clean_key and clean_value and 
                    len(clean_key) > 1 and len(clean_value) > 1 and
                    clean_key.lower() not in ['form', 'fs08', 'fs71', '207z', '', ' ']):
                    cleaned_fields[clean_key] = clean_value
            
            return cleaned_fields
            
        except Exception as e:
            self.logger.warning(f"Error extracting form fields: {e}")
            return {}
    
    def _extract_cell_content(self, cell) -> str:
        """Extract content from a table cell, preserving links and structure"""
        try:
            # Get all text content
            text_content = self._clean_text(cell.text.strip())
            
            # If cell contains links, also capture link information
            links = cell.find_elements(By.CSS_SELECTOR, "a")
            if links:
                link_info = []
                for link in links:
                    link_text = self._clean_text(link.text.strip())
                    link_href = link.get_attribute('href')
                    if link_text and link_href:
                        link_info.append(f"{link_text} ({link_href})")
                
                if link_info:
                    # Combine text content with link information
                    if text_content:
                        return f"{text_content} [Links: {'; '.join(link_info)}]"
                    else:
                        return f"[Links: {'; '.join(link_info)}]"
            
            return text_content
            
        except Exception as e:
            self.logger.warning(f"Error extracting cell content: {e}")
            return ""
    
    def _extract_related_links(self) -> List[Dict]:
        """Extract related links section"""
        related_links = []
        
        try:
            # Look for "Related links" heading
            headings = self.driver.find_elements(By.XPATH, 
                "//h2[contains(text(), 'Related links')] | //h3[contains(text(), 'Related links')] | //h2[contains(text(), 'Related Links')] | //h3[contains(text(), 'Related Links')]")
            
            for heading in headings:
                try:
                    # Find the next sibling element (usually ul or div)
                    next_element = heading.find_element(By.XPATH, "following-sibling::*[1]")
                    
                    # Extract links from the related section
                    links = next_element.find_elements(By.CSS_SELECTOR, "a")
                    
                    for link in links:
                        try:
                            href = link.get_attribute('href')
                            text = self._clean_text(link.text.strip())
                            title = link.get_attribute('title') or text
                            
                            if href and text:
                                related_links.append({
                                    'url': href,
                                    'text': text,
                                    'title': title
                                })
                        except Exception as link_e:
                            self.logger.warning(f"Error processing related link: {link_e}")
                            continue
                
                except Exception as section_e:
                    self.logger.warning(f"Error processing related links section: {section_e}")
                    continue
            
            # Also check for related information in form tables
            if not related_links:
                related_info_cells = self.driver.find_elements(By.XPATH, 
                    "//td[contains(strong/text(), 'Related Information')] | //td[contains(text(), 'Related Information')]")
                
                for cell in related_info_cells:
                    try:
                        links = cell.find_elements(By.CSS_SELECTOR, "a")
                        for link in links:
                            try:
                                href = link.get_attribute('href')
                                text = self._clean_text(link.text.strip())
                                title = link.get_attribute('title') or text
                                
                                if href and text:
                                    related_links.append({
                                        'url': href,
                                        'text': text,
                                        'title': title
                                    })
                            except Exception as link_e:
                                continue
                    except Exception as cell_e:
                        continue
            
        except Exception as e:
            self.logger.warning(f"Error extracting related links: {e}")
        
        return related_links
    
    def _extract_tables(self) -> List[Dict]:
        """Extract table data from page"""
        tables = []
        
        try:
            table_elements = self.driver.find_elements(By.CSS_SELECTOR, "table")
            
            for i, table in enumerate(table_elements):
                try:
                    table_data = {
                        'table_id': i + 1,
                        'headers': [],
                        'rows': []
                    }
                    
                    # Extract headers
                    header_elements = table.find_elements(By.CSS_SELECTOR, "th")
                    table_data['headers'] = [self._clean_text(th.text.strip()) for th in header_elements]
                    
                    # Extract rows
                    row_elements = table.find_elements(By.CSS_SELECTOR, "tr")
                    for row in row_elements:
                        cells = row.find_elements(By.CSS_SELECTOR, "td")
                        if cells:
                            row_data = [self._clean_text(cell.text.strip()) for cell in cells]
                            table_data['rows'].append(row_data)
                    
                    if table_data['headers'] or table_data['rows']:
                        tables.append(table_data)
                        
                except Exception as e:
                    self.logger.warning(f"Error processing table {i}: {e}")
                    continue
            
        except Exception as e:
            self.logger.warning(f"Error extracting tables: {e}")
        
        return tables
    
    def _extract_pdf_content(self) -> Dict:
        """Extract and process PDF content from links"""
        pdf_data = {
            'pdf_links': [],
            'pdf_content': [],
            'pdf_checksums': []
        }
        
        try:
            # Find PDF links
            pdf_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href$='.pdf'], a[href*='.pdf']")
            
            for link in pdf_links:
                try:
                    pdf_url = link.get_attribute('href')
                    if not pdf_url:
                        continue
                    
                    # Make URL absolute
                    if not pdf_url.startswith('http'):
                        pdf_url = urljoin(self.base_url, pdf_url)
                    
                    pdf_data['pdf_links'].append(pdf_url)
                    
                    # Download and extract PDF content
                    pdf_content = self._download_and_extract_pdf(pdf_url)
                    if pdf_content:
                        # Calculate checksum
                        checksum = hashlib.md5(pdf_content.encode()).hexdigest()
                        
                        # Only process if not already processed
                        if checksum not in self.processed_pdfs:
                            pdf_data['pdf_content'].append({
                                'url': pdf_url,
                                'content': pdf_content,
                                'checksum': checksum
                            })
                            pdf_data['pdf_checksums'].append(checksum)
                            self.processed_pdfs.add(checksum)
                        else:
                            self.logger.info(f"PDF already processed: {pdf_url}")
                    
                except Exception as e:
                    self.logger.warning(f"Error processing PDF link: {e}")
                    continue
            
        except Exception as e:
            self.logger.warning(f"Error extracting PDF content: {e}")
        
        return pdf_data
    
    def _extract_legislation_pdf(self) -> Optional[str]:
        """Extract PDF content from legislation.gov.au iframe"""
        try:
            # Look for PDF iframe
            iframe_elements = self.driver.find_elements(By.CSS_SELECTOR, "iframe[src*='.pdf']")
            
            for iframe in iframe_elements:
                try:
                    pdf_url = iframe.get_attribute('src')
                    if pdf_url:
                        return self._download_and_extract_pdf(pdf_url)
                except:
                    continue
            
            # Also check for direct PDF links
            pdf_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='.pdf']")
            for link in pdf_links:
                try:
                    pdf_url = link.get_attribute('href')
                    if pdf_url:
                        return self._download_and_extract_pdf(pdf_url)
                except:
                    continue
            
        except Exception as e:
            self.logger.warning(f"Error extracting legislation PDF: {e}")
        
        return None
    
    def _download_and_extract_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text content"""
        try:
            # Download PDF
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            # Extract text using PyMuPDF (more reliable than PyPDF2)
            pdf_content = ""
            
            try:
                # Try PyMuPDF first
                pdf_document = fitz.open(stream=response.content, filetype="pdf")
                
                for page_num in range(len(pdf_document)):
                    page = pdf_document.load_page(page_num)
                    text = page.get_text()
                    pdf_content += text + "\n"
                
                pdf_document.close()
                
            except Exception as e:
                self.logger.warning(f"PyMuPDF failed for {pdf_url}, trying PyPDF2: {e}")
                
                # Fallback to PyPDF2
                try:
                    from io import BytesIO
                    pdf_reader = PyPDF2.PdfReader(BytesIO(response.content))
                    
                    for page in pdf_reader.pages:
                        text = page.extract_text()
                        pdf_content += text + "\n"
                        
                except Exception as e2:
                    self.logger.error(f"Both PDF extraction methods failed for {pdf_url}: {e2}")
                    return None
            
            # Clean and return content
            if pdf_content.strip():
                return self._clean_text(pdf_content)
            else:
                self.logger.warning(f"No text extracted from PDF: {pdf_url}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error downloading/extracting PDF {pdf_url}: {e}")
            return None
    
    def _clean_text(self, text: str) -> str:
        """Clean text for LLM analysis"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove unnecessary characters but preserve structure
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        # Normalize line breaks
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def _save_data(self):
        """Save scraped data to JSON file"""
        try:
            # Sort data by scraped date (newest first)
            self.scraped_data.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
            
            # Save to file
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.scraped_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(self.scraped_data)} records to {self.output_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            raise
    
    def _cleanup(self):
        """Cleanup resources"""
        try:
            if self.driver:
                self.driver.quit()
            
            # Save data before exit
            self._save_data()
            
            self.logger.info("Cleanup completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


class ExcelExtractor:
    """Helper class for extracting data from Excel files"""
    
    @staticmethod
    def extract_excel_content(file_path: str) -> Dict:
        """Extract content from Excel file"""
        try:
            # Read all sheets
            excel_data = pd.read_excel(file_path, sheet_name=None)
            
            content = {
                'sheets': {},
                'summary': f"Excel file with {len(excel_data)} sheets"
            }
            
            for sheet_name, df in excel_data.items():
                # Convert to text representation
                sheet_content = {
                    'name': sheet_name,
                    'rows': df.shape[0],
                    'columns': df.shape[1],
                    'column_names': df.columns.tolist(),
                    'data': df.to_string(index=False)
                }
                content['sheets'][sheet_name] = sheet_content
            
            return content
            
        except Exception as e:
            logging.error(f"Error extracting Excel content: {e}")
            return {}


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ASIC Regulatory Resources Scraper')
    parser.add_argument('--max-pages', type=int, default=None, 
                       help='Maximum pages to scrape (None for all pages)')
    parser.add_argument('--data-dir', type=str, default='data',
                       help='Directory to store data files')
    parser.add_argument('--incremental', action='store_true',
                       help='Run incremental update (max 3 pages)')
    parser.add_argument('--test', action='store_true',
                       help='Run test mode (single page)')
    
    args = parser.parse_args()
    
    # Set max pages for different run types
    if args.test:
        max_pages = 1
    elif args.incremental:
        max_pages = 3
    else:
        max_pages = args.max_pages
    
    # Initialize and run scraper
    scraper = ASICResourceScraper(max_pages=max_pages, data_dir=args.data_dir)
    
    try:
        scraper._init_driver()  # Initialize driver first
        scraper.scrape_all_resources()
        print(f"Scraping completed successfully. Check {scraper.output_file} for results.")
        
    except KeyboardInterrupt:
        scraper.logger.info("Scraping interrupted by user")
        scraper._save_data()
        
    except Exception as e:
        scraper.logger.error(f"Scraping failed: {e}")
        scraper._save_data()
        raise
    
    finally:
        scraper._cleanup()


if __name__ == "__main__":
    main()


# Additional utility functions for data analysis

def analyze_scraped_data(data_file: str = "data/asic_regulatory_resources.json"):
    """Analyze scraped data and provide statistics"""
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"\n=== ASIC Data Analysis ===")
        print(f"Total records: {len(data)}")
        
        # Count by type
        type_counts = {}
        topic_counts = {}
        
        for item in data:
            resource_type = item.get('type', 'Unknown')
            type_counts[resource_type] = type_counts.get(resource_type, 0) + 1
            
            for topic in item.get('topics', []):
                topic_counts[topic] = topic_counts.get(topic, 0) + 1
        
        print(f"\nResource Types:")
        for rtype, count in sorted(type_counts.items()):
            print(f"  {rtype}: {count}")
        
        print(f"\nTop 10 Topics:")
        for topic, count in sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {topic}: {count}")
        
        # Content statistics
        total_content_length = sum(len(item.get('content_text', '')) for item in data)
        pdf_count = sum(len(item.get('pdf_content', [])) for item in data)
        
        print(f"\nContent Statistics:")
        print(f"  Total content length: {total_content_length:,} characters")
        print(f"  Total PDFs processed: {pdf_count}")
        
        return data
        
    except Exception as e:
        print(f"Error analyzing data: {e}")
        return None


def export_to_csv(data_file: str = "data/asic_regulatory_resources.json", 
                 output_file: str = "data/asic_summary.csv"):
    """Export summary data to CSV for analysis"""
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Create summary records
        summary_records = []
        for item in data:
            record = {
                'doc_id': item.get('doc_id', ''),
                'type': item.get('type', ''),
                'title': item.get('title', ''),
                'published_date': item.get('published_date', ''),
                'scraped_date': item.get('scraped_date', ''),
                'url': item.get('url', ''),
                'topics': '; '.join(item.get('topics', [])),
                'content_length': len(item.get('content_text', '')),
                'pdf_count': len(item.get('pdf_content', [])),
                'has_relocated': bool(item.get('relocated_url')),
                'submission_deadline': item.get('submission_deadline', ''),
                'submission_email': item.get('submission_email', '')
            }
            summary_records.append(record)
        
        # Save to CSV
        df = pd.DataFrame(summary_records)
        df.to_csv(output_file, index=False)
        
        print(f"Summary data exported to {output_file}")
        return df
        
    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        return None


# Configuration for different run types
SCRAPER_CONFIGS = {
    'full': {
        'max_pages': None,
        'description': 'Full historical scrape of all resources'
    },
    'daily': {
        'max_pages': 3,
        'description': 'Daily incremental update (recent items only)'
    },
    'test': {
        'max_pages': 1,
        'description': 'Test run with single page'
    }
}


def run_scheduled_scrape(config_name: str = 'daily'):
    """Run scraper with predefined configuration"""
    config = SCRAPER_CONFIGS.get(config_name, SCRAPER_CONFIGS['daily'])
    
    print(f"Running {config_name} scrape: {config['description']}")
    
    scraper = ASICResourceScraper(
        max_pages=config['max_pages'],
        data_dir='data'
    )
    
    try:
        scraper.scrape_all_resources()
        
        # Analyze results
        analyze_scraped_data()
        
        # Export summary
        export_to_csv()
        
        print(f"\n{config_name.title()} scrape completed successfully!")
        
    except Exception as e:
        print(f"Scrape failed: {e}")
        raise


# Example usage and testing
if __name__ == "__main__":
    # For testing - uncomment one of these:
    
    # Full scrape (initial run)
    # run_scheduled_scrape('full')
    
    # Daily update
    # run_scheduled_scrape('daily')
    
    # Test run
    # run_scheduled_scrape('test')
    
    # Analysis only
    # analyze_scraped_data()
    
    # Regular main execution
    main()