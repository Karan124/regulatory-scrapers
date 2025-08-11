#!/usr/bin/env python3
"""
NHVR Media Release Scraper
Comprehensive scraper for National Heavy Vehicle Regulator media releases
Designed for LLM analysis with stealth measures and PDF extraction

Required packages:
pip install requests beautifulsoup4 selenium
pip install PyPDF2 PyMuPDF tabula-py
pip install pandas openpyxl  # for tabula
"""

import requests
import time
import json
import logging
import hashlib
import os
import re
import io
import tempfile
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

# PDF processing
import PyPDF2
import fitz  # PyMuPDF for better text extraction
import tabula  # For table extraction from PDFs

# Configuration
BASE_URL = "https://www.nhvr.gov.au"
MEDIA_RELEASE_URL = "https://www.nhvr.gov.au/mediarelease"
DATA_DIR = Path("data")
MAX_PAGES = 2  # Set to 3 for daily runs, higher for full scrapes
DELAY_BETWEEN_REQUESTS = 2  # seconds
PROCESSED_URLS_FILE = DATA_DIR / "processed_urls.json"

# Create directories
DATA_DIR.mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(DATA_DIR / 'nhvr_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress urllib3 warnings about connection retries
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

class NHVRScraper:
    def __init__(self):
        self.session = requests.Session()
        self.driver = None
        self.processed_urls: Set[str] = self.load_processed_urls()
        self.scraped_data: List[Dict] = []
        self.pdf_hashes: Set[str] = set()
        
        # Setup realistic headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session.headers.update(self.headers)

    def load_processed_urls(self) -> Set[str]:
        """Load previously processed URLs to avoid duplicates"""
        if PROCESSED_URLS_FILE.exists():
            try:
                with open(PROCESSED_URLS_FILE, 'r') as f:
                    return set(json.load(f))
            except Exception as e:
                logger.warning(f"Could not load processed URLs: {e}")
        return set()

    def save_processed_urls(self):
        """Save processed URLs for future runs"""
        try:
            with open(PROCESSED_URLS_FILE, 'w') as f:
                json.dump(list(self.processed_urls), f, indent=2)
        except Exception as e:
            logger.error(f"Could not save processed URLs: {e}")

    def cleanup_chrome_processes(self):
        """Kill any existing Chrome processes that might be interfering"""
        try:
            import psutil
            killed_processes = 0
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    # Look for Chrome processes
                    if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                        # Check if it's related to our scraper or automation
                        cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                        if any(keyword in cmdline.lower() for keyword in ['automation', 'webdriver', 'chromedriver', 'remote-debugging']):
                            logger.info(f"Killing Chrome process: PID {proc.info['pid']}")
                            proc.terminate()
                            killed_processes += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
            
            if killed_processes > 0:
                logger.info(f"Killed {killed_processes} Chrome processes")
                time.sleep(2)  # Give time for processes to fully terminate
            else:
                logger.info("No Chrome processes found to clean up")
                
        except ImportError:
            logger.warning("psutil not available for process cleanup - continuing anyway")
        except Exception as e:
            logger.warning(f"Error during Chrome process cleanup: {e}")

    def setup_driver(self):
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
        logger.info("Using system default Chrome binary (auto-detection)")
        
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
                    logger.info(f"Found ChromeDriver at: {path}")
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
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info(f"Chrome driver initialized successfully")
            if chromedriver_path:
                logger.info(f"Using ChromeDriver: {chromedriver_path}")
            else:
                logger.info("Using ChromeDriver from PATH")
            
            # Simulate natural browsing - visit homepage first
            logger.info("Visiting homepage to establish session...")
            self.driver.get(BASE_URL)
            time.sleep(3)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            
            # Detailed troubleshooting
            logger.error("Troubleshooting information:")
            
            # Check if chromedriver is accessible
            try:
                result = subprocess.run(['chromedriver', '--version'], capture_output=True, text=True, timeout=5)
                logger.info(f"ChromeDriver version: {result.stdout.strip()}")
            except Exception as cmd_e:
                logger.error(f"Cannot run chromedriver command: {cmd_e}")
            
            # Check Chrome version
            try:
                result = subprocess.run(['google-chrome', '--version'], capture_output=True, text=True, timeout=5)
                logger.info(f"Chrome version: {result.stdout.strip()}")
            except Exception as chrome_e:
                logger.error(f"Cannot run chrome command: {chrome_e}")
            
            return False

    def exponential_backoff_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Make request with exponential backoff retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response
                elif response.status_code == 403:
                    logger.warning(f"403 error for {url}, attempt {attempt + 1}")
                    wait_time = (2 ** attempt) + 1
                    time.sleep(wait_time)
                else:
                    logger.warning(f"HTTP {response.status_code} for {url}")
                    return None
            except Exception as e:
                logger.warning(f"Request failed for {url}: {e}, attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1
                    time.sleep(wait_time)
        return None

    def get_current_page_links(self) -> List[Tuple[str, str, str]]:
        """Extract media release links from current page"""
        try:
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
            time.sleep(DELAY_BETWEEN_REQUESTS)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            links = []
            
            # Find all media release articles
            articles = soup.find_all('article')
            
            for article in articles:
                try:
                    # Extract headline and URL
                    headline_elem = article.find('h2')
                    if headline_elem:
                        link_elem = headline_elem.find('a')
                        if link_elem:
                            headline = link_elem.get_text(strip=True)
                            relative_url = link_elem.get('href', '')
                            full_url = urljoin(BASE_URL, relative_url)
                            
                            # Extract date
                            date_elem = article.find('time')
                            pub_date = date_elem.get('datetime', '') if date_elem else ''
                            
                            links.append((full_url, headline, pub_date))
                            
                except Exception as e:
                    logger.warning(f"Error parsing article: {e}")
                    continue
                    
            logger.info(f"Found {len(links)} media releases on current page")
            return links
            
        except Exception as e:
            logger.error(f"Error getting current page links: {e}")
            return []

    def check_driver_connection(self) -> bool:
        """Check if WebDriver connection is still active and reconnect if needed"""
        try:
            # Simple test to check if driver is responsive
            self.driver.current_url
            return True
        except Exception as e:
            logger.warning(f"WebDriver connection lost: {e}")
            logger.info("Attempting to reconnect WebDriver...")
            
            try:
                # Try to quit the old driver first
                if self.driver:
                    try:
                        self.driver.quit()
                    except:
                        pass
                
                # Reinitialize the driver
                if self.setup_driver():
                    logger.info("WebDriver reconnected successfully")
                    # Navigate back to media releases page
                    self.driver.get(MEDIA_RELEASE_URL)
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                    return True
                else:
                    logger.error("Failed to reconnect WebDriver")
                    return False
                    
            except Exception as reconnect_error:
                logger.error(f"Error during WebDriver reconnection: {reconnect_error}")
                return False

    def click_next_page(self) -> bool:
        """Click the next page button (››) to navigate to the next page"""
        try:
            # Look for the next page link containing ››
            next_button = None
            
            # Try multiple selectors to find the next page button
            try:
                # Method 1: Find span with ›› and get parent link
                next_span = self.driver.find_element(By.XPATH, "//span[@aria-hidden='true' and text()='››']")
                next_button = next_span.find_element(By.XPATH, "./..")
                logger.info("Found next page button via ›› symbol")
            except:
                try:
                    # Method 2: Find link with title="Go to next page"
                    next_button = self.driver.find_element(By.XPATH, "//a[@title='Go to next page']")
                    logger.info("Found next page button via title attribute")
                except:
                    try:
                        # Method 3: Find link with rel="next"
                        next_button = self.driver.find_element(By.XPATH, "//a[@rel='next']")
                        logger.info("Found next page button via rel='next'")
                    except:
                        logger.info("No next page button found - likely reached the last page")
                        return False
            
            if next_button:
                # Get the URL this button will navigate to
                next_url = next_button.get_attribute('href')
                logger.info(f"Next page button will navigate to: {next_url}")
                
                # Check if button is clickable (not disabled)
                if next_button.is_enabled():
                    # Scroll to button to ensure it's visible
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                    time.sleep(1)
                    
                    # Click the button
                    next_button.click()
                    logger.info("Successfully clicked next page button")
                    
                    # Wait for page to load
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                    
                    # Wait for articles to load
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "article"))
                        )
                        logger.info(f"Successfully navigated to next page: {self.driver.current_url}")
                        return True
                    except TimeoutException:
                        logger.warning("Timeout waiting for articles to load after clicking next")
                        return False
                else:
                    logger.info("Next page button found but not enabled (likely last page)")
                    return False
            
            return False
            
        except Exception as e:
            logger.warning(f"Error clicking next page button: {e}")
            return False

    def extract_pdf_content(self, pdf_url: str) -> Optional[str]:
        """Download and extract content from PDF without saving to disk"""
        try:
            # Generate hash to check for duplicates
            pdf_hash = hashlib.md5(pdf_url.encode()).hexdigest()
            if pdf_hash in self.pdf_hashes:
                logger.info(f"PDF already processed: {pdf_url}")
                return None
                
            self.pdf_hashes.add(pdf_hash)
            
            # Download PDF content directly to memory
            response = self.exponential_backoff_request(pdf_url)
            if not response:
                logger.warning(f"Failed to download PDF: {pdf_url}")
                return None
                
            logger.info(f"Downloaded PDF from: {pdf_url}")
            
            # Extract text using PyMuPDF (better than PyPDF2)
            text_content = []
            tables_content = []
            
            try:
                # Create temporary file-like object in memory
                pdf_data = io.BytesIO(response.content)
                
                # Extract text with PyMuPDF
                doc = fitz.open(stream=pdf_data, filetype="pdf")
                for page_num in range(len(doc)):
                    page = doc.load_page(page_num)
                    text = page.get_text()
                    if text.strip():
                        text_content.append(text)
                doc.close()
                
                # Try to extract tables with tabula (requires temporary file)
                try:
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                        temp_file.write(response.content)
                        temp_file.flush()
                        
                        tables = tabula.read_pdf(temp_file.name, pages='all', multiple_tables=True)
                        for i, table in enumerate(tables):
                            if not table.empty:
                                table_text = f"\n--- Table {i+1} ---\n"
                                table_text += table.to_string(index=False)
                                tables_content.append(table_text)
                        
                        # Clean up temp file
                        os.unlink(temp_file.name)
                        
                except Exception as e:
                    logger.debug(f"No tables extracted from PDF: {e}")
                    
            except Exception as e:
                logger.warning(f"PyMuPDF failed for PDF, trying PyPDF2: {e}")
                # Fallback to PyPDF2
                try:
                    pdf_data.seek(0)  # Reset stream position
                    reader = PyPDF2.PdfReader(pdf_data)
                    for page in reader.pages:
                        text = page.extract_text()
                        if text.strip():
                            text_content.append(text)
                except Exception as e2:
                    logger.error(f"Both PDF extraction methods failed: {e2}")
                    return None
            
            # Combine and clean text
            all_text = '\n\n'.join(text_content + tables_content)
            
            # Clean text for LLM processing
            cleaned_text = self.clean_text_for_llm(all_text)
            
            logger.info(f"Extracted {len(cleaned_text)} characters from PDF")
            return cleaned_text
            
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_url}: {e}")
            return None

    def clean_text_for_llm(self, text: str) -> str:
        """Clean and normalize text for LLM analysis"""
        if not text:
            return ""
            
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s.,;:!?()-]', ' ', text)
        
        # Remove repeated headers/footers (simple heuristic)
        lines = text.split('\n')
        cleaned_lines = []
        seen_lines = set()
        
        for line in lines:
            line = line.strip()
            if len(line) < 5:  # Skip very short lines
                continue
            if line.lower() in seen_lines and len(line) < 50:  # Skip repeated short lines
                continue
            seen_lines.add(line.lower())
            cleaned_lines.append(line)
            
        return ' '.join(cleaned_lines).strip()

    def extract_media_release_content(self, url: str, headline: str, pub_date: str) -> Optional[Dict]:
        """Extract content from individual media release page"""
        
        if url in self.processed_urls:
            logger.info(f"Already processed: {headline}")
            return None
            
        # Check driver connection before proceeding
        if not self.check_driver_connection():
            logger.error(f"Cannot process {headline} - driver connection failed")
            return None
            
        try:
            logger.info(f"Navigating to: {url}")
            self.driver.get(url)
            time.sleep(DELAY_BETWEEN_REQUESTS)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract main content
            content_elem = soup.find('article')
            if not content_elem:
                logger.warning(f"No article content found for {url}")
                return None
                
            # Extract main text content
            main_text = ""
            content_divs = content_elem.find_all('div')
            for div in content_divs:
                if div.get_text(strip=True):
                    main_text += div.get_text(separator='\n', strip=True) + '\n\n'
                    
            # Clean main text
            main_text = self.clean_text_for_llm(main_text)
            
            # Extract related links from article body and identify PDF documents
            related_links = []
            pdf_links = []
            
            # Look for links in article content only (not the printable section)
            for link in content_elem.find_all('a', href=True):
                href = link.get('href')
                if href and not href.startswith('#'):
                    full_link = urljoin(BASE_URL, href)
                    link_text = link.get_text(strip=True)
                    
                    # Check if this is a PDF document link
                    is_pdf = (
                        href.lower().endswith('.pdf') or 
                        'pdf' in link_text.lower() or
                        '/document/' in href  # NHVR document URLs are typically PDFs
                    )
                    
                    if is_pdf:
                        pdf_links.append(full_link)
                        logger.info(f"Found PDF document in content: {link_text} -> {full_link}")
                    else:
                        related_links.append({
                            'url': full_link,
                            'text': link_text
                        })
                        
            # Process any PDF documents found in the content
            pdf_content = []
            for current_pdf_url in pdf_links:
                logger.info(f"Processing PDF document: {current_pdf_url}")
                pdf_text = self.extract_pdf_content(current_pdf_url)
                if pdf_text:
                    pdf_content.append({
                        'source_url': current_pdf_url,
                        'content': pdf_text
                    })
                    
            # Extract image URLs
            image_url = None
            img_elem = content_elem.find('img')
            if img_elem:
                img_src = img_elem.get('src')
                if img_src:
                    image_url = urljoin(BASE_URL, img_src)
                    
            # Extract theme/topic
            theme = ""
            theme_elem = soup.find('div', string=re.compile('Latest News Subject'))
            if theme_elem:
                theme_link = theme_elem.find_next('a')
                if theme_link:
                    theme = theme_link.get_text(strip=True)
                        
            # Parse publication date
            formatted_date = ""
            if pub_date:
                try:
                    dt = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                    formatted_date = dt.strftime('%Y-%m-%d')
                except:
                    # Try parsing alternative formats
                    date_text = soup.find('time')
                    if date_text:
                        formatted_date = date_text.get_text(strip=True)
                        
            # Create media release record
            media_release = {
                'headline': headline,
                'media_release_theme': theme,
                'published_date': formatted_date,
                'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'main_text_content': main_text,
                'associated_image_url': image_url,
                'related_links': related_links,
                'pdf_content': pdf_content,
                'source_url': url
            }
            
            self.processed_urls.add(url)
            logger.info(f"Successfully scraped: {headline}")
            return media_release
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None

    def scrape_all_releases(self):
        """Main scraping method using click navigation"""
        logger.info("Starting NHVR media release scraping...")
        
        if not self.setup_driver():
            logger.error("Failed to setup driver, exiting")
            return
            
        try:
            # Start with the main media release page
            logger.info("Navigating to media releases page...")
            self.driver.get(MEDIA_RELEASE_URL)
            time.sleep(DELAY_BETWEEN_REQUESTS)
            
            page_count = 1
            scraped_count = 0
            consecutive_empty_pages = 0
            
            while page_count <= MAX_PAGES:
                logger.info(f"Processing page {page_count}...")
                
                # Get links from current page using the renamed method
                try:
                    current_page_links = self.get_current_page_links()
                except Exception as e:
                    logger.error(f"Error getting links from page {page_count}: {e}")
                    break
                
                if not current_page_links:
                    consecutive_empty_pages += 1
                    logger.warning(f"No links found on page {page_count} (empty page #{consecutive_empty_pages})")
                    
                    # If we get 2 consecutive empty pages, assume we've reached the end
                    if consecutive_empty_pages >= 2:
                        logger.info("Found 2 consecutive empty pages, stopping pagination")
                        break
                else:
                    consecutive_empty_pages = 0  # Reset counter if we found links
                    logger.info(f"Processing {len(current_page_links)} media releases from page {page_count}")
                    
                # Process each media release
                for url, headline, pub_date in current_page_links:
                    try:
                        logger.info(f"Processing: {headline[:50]}...")
                        media_release = self.extract_media_release_content(url, headline, pub_date)
                        if media_release:
                            self.scraped_data.append(media_release)
                            scraped_count += 1
                    except Exception as e:
                        logger.error(f"Error processing media release {headline}: {e}")
                        # Check if it's a driver connection issue
                        if "Connection refused" in str(e) or "session" in str(e).lower():
                            logger.warning("Detected driver connection issue, attempting to recover...")
                            if not self.check_driver_connection():
                                logger.error("Cannot recover driver connection, stopping")
                                break
                            else:
                                # Retry this article
                                logger.info(f"Retrying: {headline}")
                                try:
                                    media_release = self.extract_media_release_content(url, headline, pub_date)
                                    if media_release:
                                        self.scraped_data.append(media_release)
                                        scraped_count += 1
                                except Exception as retry_error:
                                    logger.error(f"Retry failed for {headline}: {retry_error}")
                        continue
                        
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                
                # After processing all articles, navigate back to the CURRENT page's media releases list
                # to find the pagination buttons for the NEXT page
                current_page_url = MEDIA_RELEASE_URL if page_count == 1 else f"{MEDIA_RELEASE_URL}?page={page_count - 1}"
                logger.info(f"Navigating back to current page for pagination: {current_page_url}")
                try:
                    self.driver.get(current_page_url)
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                    
                    # Wait for the page to load properly
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "article"))
                    )
                except Exception as nav_error:
                    logger.error(f"Error navigating back to current page: {nav_error}")
                    break
                
                # Try to click next page button
                if page_count < MAX_PAGES:
                    logger.info(f"Attempting to navigate to page {page_count + 1}...")
                    try:
                        # Check driver connection before clicking
                        if not self.check_driver_connection():
                            logger.error("Cannot continue pagination - driver connection failed")
                            break
                            
                        if not self.click_next_page():
                            logger.info("No more pages available or unable to click next page")
                            break
                    except Exception as e:
                        logger.error(f"Error clicking next page: {e}")
                        break
                
                page_count += 1
                
            logger.info(f"Scraping completed. Total new releases: {scraped_count}")
            logger.info(f"Processed {page_count} pages")
            
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    logger.info("WebDriver closed successfully")
                except Exception as e:
                    logger.warning(f"Error closing WebDriver: {e}")
                    # Force kill any remaining Chrome processes if needed
                    try:
                        import psutil
                        for proc in psutil.process_iter(['pid', 'name']):
                            if 'chrome' in proc.info['name'].lower():
                                proc.terminate()
                    except:
                        pass

    def save_data(self):
        """Save scraped data to JSON file"""
        if not self.scraped_data:
            logger.info("No new data to save")
            return
            
        output_file = DATA_DIR / 'nhvr_media_releases.json'
        
        # Load existing data if file exists
        existing_data = []
        if output_file.exists():
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                logger.info(f"Loaded {len(existing_data)} existing records")
            except Exception as e:
                logger.warning(f"Could not load existing data: {e}")
                
        # Combine with new data
        all_data = existing_data + self.scraped_data
        
        # Save combined data
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(all_data)} total records to {output_file}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            
        # Save processed URLs
        self.save_processed_urls()

def main():
    """Main function"""
    print(f"NHVR Media Release Scraper")
    print(f"Max pages to scrape: {MAX_PAGES}")
    print(f"Output directory: {DATA_DIR}")
    print("-" * 50)
    
    scraper = NHVRScraper()
    
    try:
        scraper.scrape_all_releases()
        scraper.save_data()
        
        logger.info("Scraping completed successfully!")
        print(f"\nResults saved to:")
        print(f"- Data: {DATA_DIR / 'nhvr_media_releases.json'}")
        print(f"- Logs: {DATA_DIR / 'nhvr_scraper.log'}")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        scraper.save_data()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Ensure proper cleanup
        if scraper.driver:
            try:
                scraper.driver.quit()
                logger.info("WebDriver cleanup completed")
            except Exception as e:
                logger.warning(f"WebDriver cleanup warning: {e}")
        
        # Additional cleanup of any remaining Chrome processes
        try:
            scraper.cleanup_chrome_processes()
        except Exception as e:
            logger.warning(f"Final Chrome cleanup warning: {e}")

if __name__ == "__main__":
    main()