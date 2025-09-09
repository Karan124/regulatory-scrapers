#!/usr/bin/env python3
"""
Production-Grade Australian Legislation Scraper v5.0

High-performance scraper with optimized data collection and performance fixes.
Built for production environments with proper error handling and monitoring.

Author: Claude
Version: 5.0.0
"""

import json
import logging
import hashlib
import time
import sys
import signal
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, quote
from dataclasses import dataclass, asdict
import traceback
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


@dataclass
class LegislationItem:
    """Streamlined data structure optimized for LRM consumption"""
    identifier: str
    title: str
    type: str
    registration_date: str
    url: str
    content: str
    explanatory_content: str = ""
    scrape_timestamp: str = ""
    content_length: int = 0


class PerformanceMonitor:
    """Monitor and log performance metrics"""
    
    def __init__(self, logger):
        self.logger = logger
        self.timers = {}
        self.counters = {}
        
    def start_timer(self, name: str):
        self.timers[name] = time.time()
        
    def end_timer(self, name: str) -> float:
        if name in self.timers:
            duration = time.time() - self.timers[name]
            self.logger.info(f"⏱️ {name}: {duration:.2f}s")
            return duration
        return 0
        
    def increment(self, name: str):
        self.counters[name] = self.counters.get(name, 0) + 1
        
    def get_stats(self) -> Dict:
        return {
            'counters': self.counters.copy(),
            'active_timers': list(self.timers.keys())
        }


class ProductionLegislationScraper:
    """High-performance production-grade scraper"""
    
    BASE_URL = "https://www.legislation.gov.au"
    
    # Use the working URL format you provided
    SEARCH_URL_TEMPLATE = (
        f"{BASE_URL}/search/status(InForce)/pointintime(Latest)/"
        "collection(NotifiableInstrument,Act,LegislativeInstrument)/"
        "sort(searchcontexts%2Ffulltextversion%2Fregisteredat%20desc)"
    )
    
    def __init__(self, max_pages: Optional[int] = 5, enable_headless: bool = True, 
                 incremental_mode: bool = True, days_lookback: int = 7,
                 max_workers: int = 3, page_timeout: int = 15):
        """
        Initialize high-performance scraper
        
        Args:
            max_pages: Maximum pages to process
            enable_headless: Run Chrome in headless mode
            incremental_mode: Only process recent items
            days_lookback: Days to look back when no previous sync
            max_workers: Max concurrent workers for content extraction
            page_timeout: Page load timeout in seconds
        """
        self.max_pages = max_pages or 10
        self.enable_headless = enable_headless
        self.incremental_mode = incremental_mode
        self.days_lookback = days_lookback
        self.max_workers = max_workers
        self.page_timeout = page_timeout
        
        # Performance optimizations
        self.fast_mode = True  # Skip unnecessary waits
        self.batch_size = 20   # Process items in batches
        
        # State management
        self.driver = None
        self.content_drivers = []  # Pool of drivers for content extraction
        self.shutdown_requested = False
        self.processed_count = 0
        self.skipped_count = 0
        self.error_count = 0
        self.driver_lock = threading.Lock()
        
        # Setup directories
        self.data_dir = Path("data")
        self.logs_dir = Path("logs")
        self.state_dir = Path("state")
        
        for directory in [self.data_dir, self.logs_dir, self.state_dir]:
            directory.mkdir(exist_ok=True)
        
        self.setup_logging()
        self.setup_signal_handlers()
        
        # Performance monitoring
        self.perf = PerformanceMonitor(self.logger)
        
        # Load existing data for duplicate detection
        self.existing_identifiers = self.load_existing_identifiers()
        self.last_sync_time = self.load_last_sync_time()
        
        self.logger.info("🚀 Production Legislation Scraper v5.0 initialized")
        self.logger.info(f"📊 Configuration: {self.max_pages} pages, {self.max_workers} workers, {self.page_timeout}s timeout")
        self.logger.info(f"💾 Loaded {sum(len(ids) for ids in self.existing_identifiers.values())} existing identifiers")
        
    def setup_logging(self):
        """Setup high-performance logging"""
        log_file = self.logs_dir / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Use a more efficient formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', 
                                    datefmt='%H:%M:%S')
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # File handler with buffering
        file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Suppress noisy libraries
        logging.getLogger('selenium').setLevel(logging.ERROR)
        logging.getLogger('urllib3').setLevel(logging.ERROR)
        
    def setup_signal_handlers(self):
        """Setup graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info("🛑 Shutdown signal received, stopping gracefully...")
            self.shutdown_requested = True
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
    def load_existing_identifiers(self) -> Dict[str, Set[str]]:
        """Load existing identifiers efficiently"""
        existing = {
            'Acts': set(),
            'Legislative Instruments': set(),
            'Notifiable Instruments': set()
        }
        
        # Load from all files since we're processing all types together now
        for leg_type in existing.keys():
            filename = f"{leg_type.lower().replace(' ', '_')}.json"
            filepath = self.data_dir / filename
            
            if filepath.exists():
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    for item_data in data.get('items', []):
                        if 'identifier' in item_data:
                            existing[leg_type].add(item_data['identifier'])
                            
                except Exception as e:
                    self.logger.error(f"❌ Error loading existing {leg_type}: {e}")
        
        return existing
        
    def load_last_sync_time(self) -> Optional[datetime]:
        """Load last synchronization timestamp"""
        sync_file = self.state_dir / "last_sync.txt"
        
        if sync_file.exists():
            try:
                sync_str = sync_file.read_text().strip()
                return datetime.fromisoformat(sync_str.replace('Z', ''))
            except Exception as e:
                self.logger.error(f"❌ Error loading last sync time: {e}")
        return None
        
    def save_last_sync_time(self):
        """Save current sync timestamp"""
        sync_file = self.state_dir / "last_sync.txt"
        
        try:
            sync_file.write_text(datetime.now().isoformat())
        except Exception as e:
            self.logger.error(f"❌ Error saving sync time: {e}")
            
    def create_optimized_driver(self, for_content: bool = False) -> webdriver.Chrome:
        """Create highly optimized Chrome driver"""
        options = Options()
        
        if self.enable_headless:
            options.add_argument("--headless=new")
            
        # Essential performance optimizations
        performance_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage", 
            "--disable-gpu",
            "--disable-extensions",
            "--disable-plugins",
            "--disable-default-apps",
            "--disable-sync",
            "--disable-translate",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI,BlinkGenPropertyTrees",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
            "--window-size=1920,1080",
            "--max_old_space_size=4096"
        ]
        
        # Content extraction drivers need images disabled for speed
        if for_content:
            performance_args.extend([
                "--disable-images",
                "--disable-javascript",  # Most content is static HTML
                "--disable-css",
                "--disable-plugins"
            ])
        
        for arg in performance_args:
            options.add_argument(arg)
            
        # Use a realistic but fast user agent
        options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Additional Chrome preferences for speed
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,
                "media_stream": 2,
            },
            "profile.managed_default_content_settings": {
                "images": 2 if for_content else 1
            }
        }
        options.add_experimental_option("prefs", prefs)
        
        try:
            # Use Service with cross-platform settings
            service = Service()
            
            # Only set creation_flags on Windows
            import platform
            if platform.system() == "Windows":
                service.creation_flags = 0x08000000  # CREATE_NO_WINDOW
            
            driver = webdriver.Chrome(service=service, options=options)
            
            # Set aggressive timeouts for performance
            driver.implicitly_wait(2)  # Reduced from default
            driver.set_page_load_timeout(self.page_timeout)
            driver.set_script_timeout(10)
            
            return driver
            
        except Exception as e:
            self.logger.error(f"❌ Driver creation failed: {e}")
            raise
            
    def setup_driver_pool(self):
        """Setup main driver and content extraction pool"""
        try:
            self.perf.start_timer("driver_setup")
            
            # Main navigation driver
            self.driver = self.create_optimized_driver(for_content=False)
            
            # Pool of content extraction drivers (lighter weight)
            self.content_drivers = []
            for i in range(self.max_workers):
                try:
                    content_driver = self.create_optimized_driver(for_content=True)
                    self.content_drivers.append(content_driver)
                    self.logger.info(f"✅ Content driver {i+1}/{self.max_workers} ready")
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to create content driver {i+1}: {e}")
                    
            self.perf.end_timer("driver_setup")
            self.logger.info(f"🏁 Driver pool ready: 1 main + {len(self.content_drivers)} content drivers")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Driver pool setup failed: {e}")
            return False
            
    def get_cutoff_date(self) -> datetime:
        """Get cutoff date for incremental processing"""
        if self.last_sync_time:
            # Use last sync time minus 1 hour buffer for safety
            return self.last_sync_time - timedelta(hours=1)
        
        # Default lookback
        return datetime.now() - timedelta(days=self.days_lookback)
        
    def should_process_item(self, identifier: str, reg_date_str: str) -> Tuple[bool, str]:
        """Fast duplicate and date filtering"""
        # Check all identifier sets for duplicates
        for leg_type, identifiers in self.existing_identifiers.items():
            if identifier in identifiers:
                return False, f"duplicate in {leg_type}"
        
        # Date-based filtering for incremental mode
        if self.incremental_mode and reg_date_str:
            try:
                reg_date = self.parse_date(reg_date_str)
                if reg_date:
                    cutoff_date = self.get_cutoff_date()
                    if reg_date <= cutoff_date:
                        return False, f"too old: {reg_date_str} <= {cutoff_date.strftime('%Y-%m-%d')}"
            except Exception as e:
                self.logger.debug(f"Date parsing failed for {reg_date_str}: {e}")
                
        return True, "new item"
        
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date strings efficiently with Australian formats"""
        if not date_str:
            return None
            
        # Clean the date string
        date_str = date_str.strip()
        
        # Common Australian date formats (most common first)
        date_formats = [
            '%d/%m/%Y',      # 05/09/2025
            '%d %B %Y',      # 5 September 2025
            '%d %b %Y',      # 5 Sep 2025
            '%Y-%m-%d',      # 2025-09-05
            '%d-%m-%Y',      # 05-09-2025
            '%d.%m.%Y',      # 05.09.2025
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
                
        self.logger.debug(f"Could not parse date: {date_str}")
        return None
        
    def scrape_legislation_list_optimized(self) -> List[Dict]:
        """Scrape complete legislation list using optimized search URL"""
        items = []
        
        try:
            self.perf.start_timer("list_scraping")
            self.logger.info("🔍 Starting optimized legislation list scraping")
            
            # Navigate directly to the pre-configured search URL
            self.logger.info("📋 Loading pre-configured search with all filters...")
            self.driver.get(self.SEARCH_URL_TEMPLATE)
            
            # Quick wait for initial load
            time.sleep(2)
            
            # Verify we're on the right page
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "datatable-row-wrapper"))
                )
                self.logger.info("✅ Search results loaded")
            except TimeoutException:
                self.logger.error("❌ Timeout waiting for search results")
                return items
            
            # Set page size to maximum for efficiency
            self.set_page_size_fast(100)
            
            # Process pages efficiently
            current_page = 1
            consecutive_old_pages = 0
            cutoff_date = self.get_cutoff_date()
            
            self.logger.info(f"📅 Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
            
            while current_page <= self.max_pages and consecutive_old_pages < 2:
                if self.shutdown_requested:
                    break
                    
                self.perf.start_timer(f"page_{current_page}")
                self.logger.info(f"📄 Processing page {current_page}/{self.max_pages}")
                
                page_items = self.extract_items_from_page_fast(cutoff_date)
                
                if not page_items:
                    consecutive_old_pages += 1
                    self.logger.info(f"⏭️ No new items on page {current_page} (consecutive: {consecutive_old_pages})")
                    
                    if consecutive_old_pages >= 2:
                        self.logger.info("🛑 Stopping: 2 consecutive pages with no new items")
                        break
                else:
                    consecutive_old_pages = 0
                    items.extend(page_items)
                    self.logger.info(f"✅ Page {current_page}: {len(page_items)} new items")
                
                self.perf.end_timer(f"page_{current_page}")
                
                # Navigate to next page efficiently
                if current_page < self.max_pages:
                    if not self.go_to_next_page_fast():
                        self.logger.info("🏁 No more pages available")
                        break
                        
                current_page += 1
                
            self.perf.end_timer("list_scraping")
            self.logger.info(f"🎯 List scraping complete: {len(items)} items found across {current_page-1} pages")
            
        except Exception as e:
            self.logger.error(f"❌ List scraping failed: {e}")
            self.logger.error(traceback.format_exc())
            
        return items
        
    def set_page_size_fast(self, size: int):
        """Set page size with minimal waiting"""
        try:
            # Find page size dropdown quickly
            page_size_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[id*='PageSize_Select']")
            
            if page_size_buttons:
                self.driver.execute_script("arguments[0].click();", page_size_buttons[0])
                time.sleep(0.5)  # Minimal wait
                
                # Select size option
                options = self.driver.find_elements(By.CSS_SELECTOR, "button.dropdown-item")
                for option in options:
                    if str(size) in option.text:
                        self.driver.execute_script("arguments[0].click();", option)
                        time.sleep(1)  # Wait for reload
                        self.logger.info(f"📏 Page size set to {size}")
                        return True
                        
        except Exception as e:
            self.logger.warning(f"⚠️ Page size setting failed: {e}")
            
        return False
        
    def extract_items_from_page_fast(self, cutoff_date: datetime) -> List[Dict]:
        """Extract items with optimized selectors and early termination"""
        items = []
        
        try:
            # Wait for content with shorter timeout
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "datatable-row-wrapper"))
                )
            except TimeoutException:
                self.logger.warning("⚠️ Timeout waiting for page content")
                return items
            
            rows = self.driver.find_elements(By.CSS_SELECTOR, "datatable-row-wrapper")
            self.logger.debug(f"Found {len(rows)} rows on page")
            
            for i, row in enumerate(rows):
                if self.shutdown_requested:
                    break
                    
                try:
                    # Fast extraction using optimized selectors
                    item_data = self.extract_item_data_fast(row)
                    if not item_data:
                        continue
                    
                    # Fast filtering
                    should_process, reason = self.should_process_item(
                        item_data['identifier'], 
                        item_data['registration_date']
                    )
                    
                    if should_process:
                        items.append(item_data)
                        self.perf.increment("items_found")
                    else:
                        self.perf.increment(f"items_skipped_{reason.split(':')[0]}")
                        
                        # Early termination for incremental mode
                        if self.incremental_mode and "too old" in reason:
                            # If we hit items that are too old, and we're processing in date order,
                            # we can stop processing this page
                            break
                            
                except Exception as e:
                    self.logger.debug(f"⚠️ Error processing row {i}: {e}")
                    self.perf.increment("row_errors")
                    continue
                    
        except Exception as e:
            self.logger.error(f"❌ Page extraction failed: {e}")
            
        return items
        
    def extract_item_data_fast(self, row_element) -> Optional[Dict]:
        """Extract item data using optimized selectors"""
        try:
            # Find legislation link using fast selectors
            link_element = None
            
            # Try most common selectors first
            fast_selectors = [
                "a[href*='/C20'], a[href*='/F20'], a[href*='/A20']",  # Direct ID patterns
                "frl-grid-cell-title-name-in-force a",                # Known structure
                ".title a, .title-name a",                             # Generic title selectors
            ]
            
            for selector in fast_selectors:
                try:
                    links = row_element.find_elements(By.CSS_SELECTOR, selector)
                    if links:
                        link_element = links[0]
                        break
                except:
                    continue
                    
            if not link_element:
                return None
                
            # Extract basic data
            title = link_element.text.strip()
            url = link_element.get_attribute('href')
            
            if not title or not url:
                return None
                
            # Make absolute URL
            if url.startswith('/'):
                url = urljoin(self.BASE_URL, url)
                
            # Extract identifier efficiently
            identifier = self.extract_identifier_fast(row_element, url)
            
            # Extract registration date efficiently
            registration_date = self.extract_registration_date_fast(row_element)
            
            # Determine type from URL pattern
            leg_type = self.determine_type_from_url(url)
            
            return {
                'title': title,
                'identifier': identifier,
                'type': leg_type,
                'registration_date': registration_date,
                'url': url
            }
            
        except Exception as e:
            self.logger.debug(f"Item extraction failed: {e}")
            return None
            
    def extract_identifier_fast(self, row_element, url: str) -> str:
        """Extract identifier using fast methods"""
        # Method 1: Look for title-id span
        try:
            id_elements = row_element.find_elements(By.CSS_SELECTOR, ".title-id, .identifier")
            if id_elements:
                identifier = id_elements[0].text.strip()
                if identifier:
                    return identifier
        except:
            pass
            
        # Method 2: Extract from URL (most reliable)
        try:
            url_parts = url.split('/')
            for part in url_parts:
                # Australian legislation ID patterns
                if re.match(r'^[A-Z]\d{4}[A-Z]\d+', part):
                    return part
        except:
            pass
            
        # Method 3: Generate from URL hash as fallback
        return hashlib.md5(url.encode()).hexdigest()[:12]
        
    def extract_registration_date_fast(self, row_element) -> str:
        """Extract registration date using optimized selectors"""
        try:
            # Try known patterns in order of likelihood
            patterns = [
                (".//span[contains(text(), 'Registered:')]", "Registered: "),
                (".//div[contains(@class, 'small') and contains(text(), 'Registered:')]", "Registered:"),
                (".//text()[contains(., 'Registered:')]", "Registered:"),
            ]
            
            for xpath, prefix in patterns:
                try:
                    elements = row_element.find_elements(By.XPATH, xpath)
                    for element in elements:
                        text = element.text.strip()
                        if prefix in text:
                            date_part = text.split(prefix)[1].strip()
                            # Clean up any extra text after the date
                            date_part = re.split(r'[,\s]*\|', date_part)[0].strip()
                            return date_part
                except:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Date extraction failed: {e}")
            
        return ""
        
    def determine_type_from_url(self, url: str) -> str:
        """Determine legislation type from URL pattern"""
        if '/C20' in url:
            return 'Acts'
        elif '/F20' in url:
            return 'Legislative Instruments'
        elif '/A20' in url:
            return 'Notifiable Instruments'
        
        # Fallback to generic determination
        if 'act' in url.lower():
            return 'Acts'
        elif 'legislative' in url.lower():
            return 'Legislative Instruments'
        else:
            return 'Notifiable Instruments'
            
    def go_to_next_page_fast(self) -> bool:
        """Navigate to next page with minimal waiting"""
        try:
            # Find next page button with fast selectors
            next_selectors = [
                "a[aria-label*='next page']:not(.disabled)",
                ".datatable-pager a[aria-label*='next']:not(.disabled)",
                "li:not(.disabled) a[aria-label*='next page']"
            ]
            
            for selector in next_selectors:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for button in buttons:
                        # Check if parent is disabled
                        parent = button.find_element(By.XPATH, "./..")
                        if "disabled" not in (parent.get_attribute("class") or ""):
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(1)  # Minimal wait for navigation
                            return True
                except:
                    continue
                    
            return False
            
        except Exception as e:
            self.logger.warning(f"⚠️ Next page navigation failed: {e}")
            return False
            
    def extract_content_parallel(self, items_list: List[Dict]) -> List[LegislationItem]:
        """Extract content using parallel processing"""
        processed_items = []
        
        if not items_list:
            return processed_items
            
        self.perf.start_timer("content_extraction")
        self.logger.info(f"🔄 Starting parallel content extraction for {len(items_list)} items")
        
        # Process in batches to manage memory
        batch_size = min(self.batch_size, len(items_list))
        
        for batch_start in range(0, len(items_list), batch_size):
            if self.shutdown_requested:
                break
                
            batch_end = min(batch_start + batch_size, len(items_list))
            batch = items_list[batch_start:batch_end]
            
            self.logger.info(f"📦 Processing batch {batch_start//batch_size + 1}: items {batch_start+1}-{batch_end}")
            
            # Process batch in parallel
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(self.content_drivers))) as executor:
                # Submit jobs
                future_to_item = {}
                for i, item_data in enumerate(batch):
                    if self.shutdown_requested:
                        break
                        
                    # Get available driver
                    driver_index = i % len(self.content_drivers)
                    driver = self.content_drivers[driver_index] if self.content_drivers else None
                    
                    if driver:
                        future = executor.submit(self.extract_single_item_content, item_data, driver)
                        future_to_item[future] = item_data
                    
                # Collect results
                for future in as_completed(future_to_item):
                    if self.shutdown_requested:
                        break
                        
                    item_data = future_to_item[future]
                    try:
                        result = future.result(timeout=60)  # 60 second timeout per item
                        if result:
                            processed_items.append(result)
                            self.processed_count += 1
                            self.perf.increment("items_processed")
                            self.logger.info(f"✅ {result.identifier}: {result.content_length} chars")
                        else:
                            self.error_count += 1
                            self.perf.increment("extraction_failures")
                            
                    except Exception as e:
                        self.error_count += 1
                        self.perf.increment("extraction_errors")
                        self.logger.error(f"❌ Failed {item_data['identifier']}: {e}")
            
            # Brief pause between batches
            if batch_end < len(items_list):
                time.sleep(0.5)
                
        self.perf.end_timer("content_extraction")
        self.logger.info(f"🎯 Content extraction complete: {len(processed_items)} items processed")
        
        return processed_items
        
    def extract_single_item_content(self, item_data: Dict, driver: webdriver.Chrome) -> Optional[LegislationItem]:
        """Extract content for a single item using dedicated driver"""
        identifier = item_data['identifier']
        
        try:
            with self.driver_lock:
                # Navigate to content page
                content_url = item_data['url']
                if not content_url.endswith('/text'):
                    content_url = content_url.replace('/asmade', '/asmade/text')
                    if not content_url.endswith('/text'):
                        content_url += '/text'
                
                driver.get(content_url)
                time.sleep(1)
                
            # Extract registration date from content page
            page_registration_date = self.extract_page_registration_date_fast(driver)
            
            # Extract content based on type
            main_content = ""
            explanatory_content = ""
            
            if item_data['type'] == "Legislative Instruments":
                main_content, explanatory_content = self.extract_legislative_instrument_content_fast(driver)
            else:
                main_content = self.extract_epub_iframe_content_fast(driver)
                
            # Use page registration date if available, otherwise use list date
            final_registration_date = page_registration_date or item_data.get('registration_date', '')
            
            # Validate content
            total_content_length = len(main_content) + len(explanatory_content)
            if total_content_length < 50:
                self.logger.warning(f"Very little content for {identifier}: {total_content_length} chars")
                return None
            
            # Create item
            item = LegislationItem(
                identifier=identifier,
                title=item_data['title'],
                type=item_data['type'],
                registration_date=final_registration_date,
                url=item_data['url'],
                content=main_content,
                explanatory_content=explanatory_content,
                scrape_timestamp=datetime.now().isoformat(),
                content_length=total_content_length
            )
            
            return item
            
        except Exception as e:
            self.logger.error(f"Content extraction failed for {identifier}: {e}")
            return None
            
    def extract_page_registration_date_fast(self, driver: webdriver.Chrome) -> str:
        """Fast registration date extraction from content page"""
        try:
            # Quick selectors for registration date
            date_selectors = [
                "frl-effective-dates .date-effective-start",
                "frl-version-info .date-effective-start",
                ".date-effective-start"
            ]
            
            for selector in date_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        date_text = elements[0].text.strip()
                        if date_text:
                            return date_text
                except:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Page date extraction failed: {e}")
            
        return ""
        
    def extract_epub_iframe_content_fast(self, driver: webdriver.Chrome) -> str:
        """Fast iframe content extraction with optimized waits"""
        try:
            # Quick iframe detection
            iframe_selectors = ["iframe#epubFrame", "iframe[name='epubFrame']"]
            iframe_element = None
            
            for selector in iframe_selectors:
                try:
                    iframes = WebDriverWait(driver, 5).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                    )
                    if iframes:
                        iframe_element = iframes[0]
                        break
                except TimeoutException:
                    continue
                    
            if not iframe_element:
                self.logger.warning("No iframe found")
                return ""
                
            # Switch to iframe
            driver.switch_to.frame(iframe_element)
            
            try:
                # Quick content extraction
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Get content using fastest method
                content = ""
                
                # Try body text first (fastest)
                try:
                    body = driver.find_element(By.TAG_NAME, "body")
                    body_text = body.text.strip()
                    
                    if body_text and len(body_text) > 100 and not self.is_navigation_content_fast(body_text):
                        content = body_text
                except:
                    pass
                    
                # Fallback to HTML parsing if needed
                if not content:
                    try:
                        page_source = driver.page_source
                        # Quick HTML cleanup
                        text_content = re.sub(r'<script[^>]*>.*?</script>', '', page_source, flags=re.DOTALL | re.IGNORECASE)
                        text_content = re.sub(r'<style[^>]*>.*?</style>', '', text_content, flags=re.DOTALL | re.IGNORECASE)
                        text_content = re.sub(r'<[^>]+>', ' ', text_content)
                        text_content = re.sub(r'\s+', ' ', text_content).strip()
                        
                        if len(text_content) > 100:
                            content = text_content
                    except:
                        pass
                        
                return self.clean_text_fast(content)
                
            finally:
                # Always switch back
                try:
                    driver.switch_to.default_content()
                except:
                    pass
                    
        except Exception as e:
            self.logger.error(f"Iframe extraction failed: {e}")
            try:
                driver.switch_to.default_content()
            except:
                pass
            return ""
            
    def extract_legislative_instrument_content_fast(self, driver: webdriver.Chrome) -> Tuple[str, str]:
        """Fast extraction for Legislative Instruments"""
        main_content = ""
        explanatory_content = ""
        
        document_types = [
            ("Legislative instrument", "main"),
            ("Explanatory statement", "explanatory")
        ]
        
        for doc_type, content_key in document_types:
            try:
                if self.select_document_type_fast(driver, doc_type):
                    time.sleep(1)  # Minimal wait
                    content = self.extract_epub_iframe_content_fast(driver)
                    
                    if content_key == "main":
                        main_content = content
                    else:
                        explanatory_content = content
                        
            except Exception as e:
                self.logger.debug(f"Error extracting {doc_type}: {e}")
                
        return main_content, explanatory_content
        
    def select_document_type_fast(self, driver: webdriver.Chrome, doc_type: str) -> bool:
        """Fast document type selection"""
        try:
            # Find dropdown quickly
            dropdown_selectors = [
                "button#FRL_COMPONENT_Select[ngbdropdowntoggle]",
                "button[id*='FRL_COMPONENT'][ngbdropdowntoggle]"
            ]
            
            dropdown_button = None
            for selector in dropdown_selectors:
                try:
                    dropdown_button = driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
                    
            if not dropdown_button:
                return True  # Assume single document type
                
            # Check if disabled
            if dropdown_button.get_attribute('disabled'):
                return True
                
            # Open and select quickly
            if dropdown_button.get_attribute('aria-expanded') != 'true':
                driver.execute_script("arguments[0].click();", dropdown_button)
                time.sleep(0.5)
                
            options = driver.find_elements(By.CSS_SELECTOR, "button[ngbdropdownitem]")
            for option in options:
                if doc_type.lower() in option.text.lower():
                    driver.execute_script("arguments[0].click();", option)
                    time.sleep(1)
                    return True
                    
            return False
            
        except Exception as e:
            self.logger.debug(f"Document type selection failed: {e}")
            return False
            
    def is_navigation_content_fast(self, text: str) -> bool:
        """Fast navigation content detection"""
        nav_indicators = ["Skip to main", "Help and resources", "Register for My Account"]
        text_start = text[:300].lower()
        return sum(1 for indicator in nav_indicators if indicator.lower() in text_start) >= 2
        
    def clean_text_fast(self, text: str) -> str:
        """Fast text cleaning"""
        if not text:
            return ""
            
        # Basic cleaning only
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        return text.strip()
        
    def save_data_optimized(self, items: List[LegislationItem]):
        """Save data efficiently by type"""
        if not items:
            return
            
        # Group items by type
        items_by_type = {}
        for item in items:
            if item.type not in items_by_type:
                items_by_type[item.type] = []
            items_by_type[item.type].append(item)
            
        # Save each type
        for leg_type, type_items in items_by_type.items():
            self.save_type_data(leg_type, type_items)
            
    def save_type_data(self, leg_type: str, items: List[LegislationItem]):
        """Save data for specific type with atomic operations"""
        filename = f"{leg_type.lower().replace(' ', '_')}.json"
        filepath = self.data_dir / filename
        backup_filepath = filepath.with_suffix('.json.backup')
        
        try:
            # Load existing data
            existing_items = {}
            if filepath.exists():
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    for item_data in data.get('items', []):
                        if 'identifier' in item_data:
                            existing_items[item_data['identifier']] = item_data
                except Exception as e:
                    self.logger.warning(f"Error loading existing {leg_type}: {e}")
            
            # Merge new items
            for item in items:
                existing_items[item.identifier] = asdict(item)
                
            # Create output
            output = {
                'metadata': {
                    'last_scraped': datetime.now().isoformat(),
                    'total_count': len(existing_items),
                    'legislation_type': leg_type,
                    'scraper_version': '5.0.0'
                },
                'items': list(existing_items.values())
            }
            
            # Atomic save
            if filepath.exists():
                filepath.replace(backup_filepath)
                
            temp_filepath = filepath.with_suffix('.json.tmp')
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
                
            temp_filepath.replace(filepath)
            
            # Update cache
            for item in items:
                self.existing_identifiers[leg_type].add(item.identifier)
            
            self.logger.info(f"Saved {len(existing_items)} {leg_type} ({len(items)} new)")
            
            # Clean up backup
            if backup_filepath.exists():
                backup_filepath.unlink()
                
        except Exception as e:
            self.logger.error(f"Error saving {leg_type}: {e}")
            if backup_filepath.exists():
                backup_filepath.replace(filepath)
                
    def generate_performance_report(self, items: List[LegislationItem], total_time: float):
        """Generate detailed performance and summary report"""
        report = {
            'performance_metrics': {
                'total_runtime_seconds': round(total_time, 2),
                'items_per_second': round(len(items) / total_time, 2) if total_time > 0 else 0,
                'scraper_version': '5.0.0',
                'configuration': {
                    'max_pages': self.max_pages,
                    'max_workers': self.max_workers,
                    'incremental_mode': self.incremental_mode,
                    'page_timeout': self.page_timeout
                }
            },
            'run_summary': {
                'scrape_timestamp': datetime.now().isoformat(),
                'new_items_processed': len(items),
                'items_skipped': self.skipped_count,
                'errors_encountered': self.error_count,
                'success_rate': round((len(items) / (len(items) + self.error_count)) * 100, 1) if (len(items) + self.error_count) > 0 else 0
            },
            'performance_counters': self.perf.get_stats(),
            'summary_by_type': {}
        }
        
        # Group by type for detailed analysis
        items_by_type = {}
        for item in items:
            if item.type not in items_by_type:
                items_by_type[item.type] = []
            items_by_type[item.type].append(item)
            
        for leg_type, type_items in items_by_type.items():
            if type_items:
                dates = [self.parse_date(item.registration_date) for item in type_items if item.registration_date]
                dates = [d for d in dates if d]
                
                report['summary_by_type'][leg_type] = {
                    'new_items': len(type_items),
                    'avg_content_length': sum(item.content_length for item in type_items) // len(type_items),
                    'total_content_chars': sum(item.content_length for item in type_items),
                    'date_range': {
                        'earliest': min(dates).strftime('%Y-%m-%d') if dates else None,
                        'latest': max(dates).strftime('%Y-%m-%d') if dates else None
                    }
                }
        
        # Save reports
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = self.logs_dir / f"performance_report_{timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            
        # Save latest report
        latest_report = Path("latest_scrape_report.json")
        with open(latest_report, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            
        # Log summary
        self.logger.info("=== PERFORMANCE SUMMARY ===")
        self.logger.info(f"Runtime: {total_time:.1f}s")
        self.logger.info(f"Processing rate: {report['performance_metrics']['items_per_second']:.1f} items/sec")
        self.logger.info(f"Success rate: {report['run_summary']['success_rate']:.1f}%")
        self.logger.info(f"New items: {len(items)}")
        self.logger.info(f"Skipped: {self.skipped_count}")
        self.logger.info(f"Errors: {self.error_count}")
        
        for leg_type, stats in report['summary_by_type'].items():
            self.logger.info(f"{leg_type}: {stats['new_items']} new items")
            
    def cleanup_resources(self):
        """Clean up all resources efficiently"""
        try:
            # Close main driver
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                    
            # Close content drivers
            for driver in self.content_drivers:
                try:
                    driver.quit()
                except:
                    pass
                    
            self.logger.info("All drivers closed")
            
            # Save sync time
            self.save_last_sync_time()
            
        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")
            
    def run(self):
        """Main execution with optimized workflow"""
        start_time = time.time()
        
        self.logger.info("=== STARTING PRODUCTION LEGISLATION SCRAPER v5.0 ===")
        self.logger.info(f"Configuration: {self.max_pages} pages, {self.max_workers} workers")
        self.logger.info(f"Mode: {'Incremental' if self.incremental_mode else 'Full'}")
        
        try:
            # Setup drivers
            if not self.setup_driver_pool():
                raise Exception("Driver pool setup failed")
                
            # Scrape legislation list (all types together)
            self.logger.info("=== PHASE 1: SCRAPING LEGISLATION LIST ===")
            items_list = self.scrape_legislation_list_optimized()
            
            if not items_list:
                self.logger.info("No new items found")
                return
                
            self.logger.info(f"Found {len(items_list)} items to process")
            
            # Extract content in parallel
            self.logger.info("=== PHASE 2: EXTRACTING CONTENT ===")
            processed_items = self.extract_content_parallel(items_list)
            
            if processed_items:
                # Save data
                self.logger.info("=== PHASE 3: SAVING DATA ===")
                self.save_data_optimized(processed_items)
                
                # Generate report
                total_time = time.time() - start_time
                self.generate_performance_report(processed_items, total_time)
                
                self.logger.info(f"=== COMPLETED SUCCESSFULLY IN {total_time:.1f}s ===")
            else:
                self.logger.warning("No items were successfully processed")
                
        except Exception as e:
            self.logger.error(f"Critical error: {e}")
            self.logger.error(traceback.format_exc())
            
        finally:
            self.cleanup_resources()


def main():
    """Main entry point with optimized argument parsing"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Production Australian Legislation Scraper v5.0 - High Performance',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Performance options
    parser.add_argument('--max-pages', type=int, default=5,
                       help='Maximum pages to process')
    parser.add_argument('--max-workers', type=int, default=3,
                       help='Maximum parallel workers for content extraction')
    parser.add_argument('--page-timeout', type=int, default=15,
                       help='Page load timeout in seconds')
    parser.add_argument('--days-lookback', type=int, default=7,
                       help='Days to look back when no previous sync')
    
    # Mode options
    parser.add_argument('--full-mode', action='store_true',
                       help='Process all items (disable incremental mode)')
    parser.add_argument('--visible', action='store_true',
                       help='Run Chrome in visible mode (for debugging)')
    
    # Quick test modes
    parser.add_argument('--test', action='store_true',
                       help='Test run: 2 pages, 1 worker')
    parser.add_argument('--fast', action='store_true',
                       help='Fast run: 3 pages, 2 workers')
    parser.add_argument('--production', action='store_true',
                       help='Production run: 10 pages, 4 workers')
    
    args = parser.parse_args()
    
    # Apply presets
    if args.test:
        args.max_pages = 2
        args.max_workers = 1
        print("TEST MODE: 2 pages, 1 worker")
    elif args.fast:
        args.max_pages = 3
        args.max_workers = 2
        print("FAST MODE: 3 pages, 2 workers")
    elif args.production:
        args.max_pages = 10
        args.max_workers = 4
        print("PRODUCTION MODE: 10 pages, 4 workers")
        
    # Configuration display
    print("\n🚀 Australian Legislation Scraper v5.0 (Production Grade)")
    print("=" * 60)
    print(f"📄 Max Pages: {args.max_pages}")
    print(f"⚡ Workers: {args.max_workers}")
    print(f"⏱️ Timeout: {args.page_timeout}s")
    print(f"📅 Lookback: {args.days_lookback} days")
    print(f"🔄 Mode: {'Full' if args.full_mode else 'Incremental'}")
    print(f"👁️ Display: {'Visible' if args.visible else 'Headless'}")
    print("=" * 60)
    
    # Run scraper
    scraper = ProductionLegislationScraper(
        max_pages=args.max_pages,
        enable_headless=not args.visible,
        incremental_mode=not args.full_mode,
        days_lookback=args.days_lookback,
        max_workers=args.max_workers,
        page_timeout=args.page_timeout
    )
    
    scraper.run()


if __name__ == "__main__":
    main()