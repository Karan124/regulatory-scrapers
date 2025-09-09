#!/usr/bin/env python3
"""
APRA Consultations Web Scraper
Scrapes consultation data from APRA website across multiple industries
with anti-bot measures, PDF processing, deduplication support, and status update tracking.
Modified for batch mode execution without user input.
"""

import json
import logging
import os
import re
import time
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import PyPDF2
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/apra_consultations.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class APRAConsultationScraper:
    """Main scraper class for APRA consultations with status update tracking"""
    
    def __init__(self, force_fresh_scrape=False):
        """
        Initialize scraper
        
        Args:
            force_fresh_scrape (bool): If True, ignores previously seen URLs and scrapes everything fresh
        """
        self.base_url = "https://www.apra.gov.au"
        self.consultation_urls = {
            "Authorised deposit-taking institutions": "https://www.apra.gov.au/consultations/1",
            "General insurance": "https://www.apra.gov.au/consultations/2", 
            "Life Insurance": "https://www.apra.gov.au/consultations/30",
            "Private Health Insurance": "https://www.apra.gov.au/consultations/32",
            "Superannuation": "https://www.apra.gov.au/consultations/33"
        }
        
        # Create data directory
        Path("data").mkdir(exist_ok=True)
        
        # Load existing data for deduplication
        self.existing_consultations = self._load_existing_data()
        
        # Migrate existing data to new format if needed
        self._migrate_data_format()
        
        # Handle fresh scrape mode
        if force_fresh_scrape:
            self.seen_urls = set()
            logger.info("Force fresh scrape enabled - ignoring previously seen URLs")
        else:
            self.seen_urls = self._load_seen_urls()
        
        # Setup session with realistic headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Setup Selenium with improved driver configuration
        self.driver = None
        self._setup_driver()

    def _migrate_data_format(self):
        """Migrate existing data to new format with status history"""
        if not self.existing_consultations:
            return
            
        migrated = False
        current_time = datetime.now().isoformat()
        
        for industry in self.existing_consultations:
            for consultation in self.existing_consultations[industry]:
                # Add first_scraped_date if missing
                if 'first_scraped_date' not in consultation:
                    consultation['first_scraped_date'] = consultation.get('scraped_date', current_time)
                    migrated = True
                
                # Add status_history if missing
                if 'status_history' not in consultation:
                    consultation['status_history'] = []
                    # Add initial status if available
                    if 'status' in consultation:
                        consultation['status_history'].append({
                            'status': consultation['status'],
                            'timestamp': consultation.get('scraped_date', current_time),
                            'note': 'Initial status from data migration'
                        })
                    migrated = True
                
                # Add last_updated if missing
                if 'last_updated' not in consultation:
                    consultation['last_updated'] = consultation.get('scraped_date', current_time)
                    migrated = True
        
        if migrated:
            logger.info("Migrated existing data to new format with status history tracking")
            # Save migrated data
            try:
                with open('data/apra_consultations.json', 'w', encoding='utf-8') as f:
                    json.dump(self.existing_consultations, f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Failed to save migrated data: {e}")

    def _should_update_consultation(self, existing_consultation: Dict, new_consultation_data: Dict) -> bool:
        """
        Check if an existing consultation should be updated based on changes
        
        Args:
            existing_consultation: The existing consultation record
            new_consultation_data: The newly scraped consultation data
            
        Returns:
            bool: True if consultation should be updated
        """
        # Always update if status has changed
        old_status = existing_consultation.get('status', '').lower().strip()
        new_status = new_consultation_data.get('status', '').lower().strip()
        
        if old_status != new_status:
            logger.info(f"Status change detected for '{existing_consultation.get('title')}': '{old_status}' → '{new_status}'")
            return True
        
        # Check for other important field changes
        fields_to_check = ['published_date', 'closing_date', 'summary']
        
        for field in fields_to_check:
            old_value = str(existing_consultation.get(field, '')).strip()
            new_value = str(new_consultation_data.get(field, '')).strip()
            
            if old_value != new_value and new_value:  # Only update if new value is not empty
                logger.info(f"Field change detected for '{existing_consultation.get('title')}' in {field}: '{old_value}' → '{new_value}'")
                return True
        
        # Check if it's been more than 7 days since last update (for periodic refresh)
        last_updated = existing_consultation.get('last_updated')
        if last_updated:
            try:
                last_update_date = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                if datetime.now() - last_update_date > timedelta(days=7):
                    logger.info(f"Updating '{existing_consultation.get('title')}' due to periodic refresh (last updated: {last_updated})")
                    return True
            except Exception as e:
                logger.warning(f"Error parsing last_updated date: {e}")
        
        return False

    def _update_consultation_record(self, existing_consultation: Dict, new_consultation_data: Dict) -> Dict:
        """
        Update existing consultation record with new data while preserving history
        
        Args:
            existing_consultation: The existing consultation record
            new_consultation_data: The newly scraped consultation data
            
        Returns:
            Dict: Updated consultation record
        """
        current_time = datetime.now().isoformat()
        
        # Preserve critical tracking fields
        updated_consultation = existing_consultation.copy()
        
        # Update status history if status changed
        old_status = existing_consultation.get('status', '').strip()
        new_status = new_consultation_data.get('status', '').strip()
        
        if old_status.lower() != new_status.lower() and new_status:
            # Add new status to history
            status_entry = {
                'status': new_status,
                'timestamp': current_time,
                'note': f'Status changed from "{old_status}" to "{new_status}"'
            }
            updated_consultation['status_history'].append(status_entry)
            logger.info(f"Added status history entry: {status_entry}")
        
        # Update main fields with new data
        updateable_fields = [
            'title', 'published_date', 'closing_date', 'status', 'summary', 
            'body_content', 'documents', 'related_links', 'consultation_theme', 
            'associated_image_url'
        ]
        
        for field in updateable_fields:
            if field in new_consultation_data:
                new_value = new_consultation_data[field]
                # Only update if new value is not empty/None or if it's a list/dict
                if new_value or isinstance(new_value, (list, dict)):
                    updated_consultation[field] = new_value
        
        # Update timestamp fields
        updated_consultation['last_updated'] = current_time
        updated_consultation['scraped_date'] = current_time  # Keep this for backward compatibility
        
        return updated_consultation

    def _find_existing_consultation(self, industry: str, consultation_url: str) -> Optional[Dict]:
        """
        Find existing consultation record by URL
        
        Args:
            industry: Industry category
            consultation_url: URL of the consultation
            
        Returns:
            Dict or None: Existing consultation record if found
        """
        if industry not in self.existing_consultations:
            return None
            
        for consultation in self.existing_consultations[industry]:
            if consultation.get('url') == consultation_url:
                return consultation
                
        return None

    def _setup_driver(self):
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

    def _load_existing_data(self) -> Dict:
        """Load existing consultation data for deduplication"""
        try:
            with open('data/apra_consultations.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _load_seen_urls(self) -> Set[str]:
        """Load previously seen URLs for deduplication"""
        try:
            with open('data/seen_urls.json', 'r', encoding='utf-8') as f:
                seen_urls = set(json.load(f))
                logger.info(f"Loaded {len(seen_urls)} previously seen URLs")
                return seen_urls
        except FileNotFoundError:
            logger.info("No seen_urls.json found, starting with fresh URL tracking")
            return set()

    def _save_seen_urls(self):
        """Save seen URLs for future deduplication"""
        with open('data/seen_urls.json', 'w', encoding='utf-8') as f:
            json.dump(list(self.seen_urls), f, indent=2)

    def _session_walk(self):
        """Perform session walking to collect cookies"""
        try:
            logger.info("Performing session walking...")
            self.session.get(self.base_url)
            time.sleep(2)
            
            # Visit industries page
            self.session.get(f"{self.base_url}/industries")
            time.sleep(1)
            
            logger.info("Session walking completed")
        except Exception as e:
            logger.error(f"Session walking failed: {e}")

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object with improved handling"""
        if not date_str:
            return None
            
        try:
            # Clean the date string
            date_str = date_str.strip()
            
            # Handle various date formats
            formats = [
                "%d %B %Y",
                "%d %b %Y", 
                "%B %d, %Y",
                "%b %d, %Y",
                "%Y-%m-%d",
                "%d/%m/%Y",
                "%B %Y",  # Added for "May 2021" format
                "%b %Y"   # Added for "Dec 2021" format
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
                    
            # Try to handle partial dates like "May 2021" by adding day 1
            try:
                # If it's just month/year, add day 1
                if len(date_str.split()) == 2:
                    date_str_with_day = f"1 {date_str}"
                    return datetime.strptime(date_str_with_day, "1 %B %Y")
            except ValueError:
                try:
                    return datetime.strptime(date_str_with_day, "1 %b %Y")
                except ValueError:
                    pass
                    
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.error(f"Date parsing error: {e}")
            return None

    def _is_consultation_eligible(self, date_str: str, status: str) -> bool:
        """Check if consultation meets filtering criteria - FIXED to include Pending status"""
        # Always include open and pending consultations regardless of date
        if any(status_keyword in status.lower() for status_keyword in ["open", "pending"]):
            logger.debug(f"Including consultation with status: {status}")
            return True
            
        # For closed consultations, check date criteria
        if status.lower() == "closed":
            date_obj = self._parse_date(date_str)
            if date_obj:
                cutoff_date = datetime(2023, 1, 1)
                is_eligible = date_obj >= cutoff_date
                if is_eligible:
                    logger.debug(f"Including closed consultation from {date_str}")
                else:
                    logger.debug(f"Excluding closed consultation from {date_str} (before cutoff)")
                return is_eligible
                
        # For any other status, log and include for review
        logger.debug(f"Including consultation with unknown status: {status}")
        return True

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF URL with enhanced error handling"""
        try:
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            # Read PDF content
            pdf_file = io.BytesIO(response.content)
            
            try:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                
                # Check if PDF is encrypted
                if pdf_reader.is_encrypted:
                    logger.warning(f"PDF is encrypted, attempting to decrypt: {pdf_url}")
                    try:
                        # Try to decrypt with empty password (common case)
                        pdf_reader.decrypt("")
                    except Exception as decrypt_error:
                        logger.error(f"Failed to decrypt PDF {pdf_url}: {decrypt_error}")
                        return ""
                
                text = ""
                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n"
                    except Exception as page_error:
                        logger.warning(f"Error extracting text from page {page_num + 1} in {pdf_url}: {page_error}")
                        continue
                
                # Clean the text
                text = re.sub(r'\s+', ' ', text).strip()
                
                if text:
                    logger.info(f"Successfully extracted {len(text)} characters from PDF")
                else:
                    logger.warning(f"No text content extracted from PDF: {pdf_url}")
                
                return text
                
            except Exception as pdf_error:
                logger.error(f"PyPDF2 error for {pdf_url}: {pdf_error}")
                
                # Check if it's a PyCryptodome issue
                if "PyCryptodome" in str(pdf_error) or "AES" in str(pdf_error):
                    logger.error("PDF decryption failed - install PyCryptodome: pip install PyCryptodome")
                
                return ""
            
        except Exception as e:
            logger.error(f"Failed to extract PDF text from {pdf_url}: {e}")
            return ""

    def _extract_consultation_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract only consultation-specific links from consultation page"""
        links = []
        
        # Find the main consultation content area, excluding navigation and general content
        main_content = soup.find('main')
        if not main_content:
            return links
        
        # Exclude navigation areas, breadcrumbs, and general site content
        excluded_areas = main_content.find_all(['nav', 'div'], class_=lambda x: x and any(
            exclude in str(x).lower() for exclude in [
                'breadcrumb', 'nav', 'menu', 'footer', 'header', 'sidebar',
                'newsletter', 'subscribe', 'link-bar'
            ]
        ))
        
        # Remove excluded areas from consideration
        for area in excluded_areas:
            area.decompose()
        
        # Look specifically in rich-text content areas and main consultation body
        content_areas = main_content.find_all(['div', 'section'], class_=lambda x: x and any(
            include in str(x).lower() for include in ['rich-text', 'section__content', 'page__middle']
        ))
        
        if not content_areas:
            content_areas = [main_content]  # Fallback to main content
            
        for area in content_areas:
            for link in area.find_all('a', href=True):
                href = link.get('href')
                if not href:
                    continue
                    
                # Convert relative URLs to absolute
                if href.startswith('/'):
                    full_url = urljoin(base_url, href)
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue
                    
                link_text = link.get_text(strip=True)
                
                # Skip navigation and general site links
                if any(skip_text in link_text.lower() for skip_text in [
                    'home', 'industries', 'subscribe', 'newsletter', 'linkedin',
                    'search', 'frequently asked questions', 'apra website',
                    'career opportunities', 'upcoming events'
                ]):
                    continue
                
                # Skip links that go to general industry pages or main site sections
                if any(skip_url in full_url.lower() for skip_url in [
                    '/industries/', '/newsletter', '/search', '/careers',
                    '/about-apra', '/contact', '/faq'
                ]):
                    continue
                
                # Only include consultation-specific content
                consultation_keywords = [
                    'discussion paper', 'consultation paper', 'draft', 'proposal',
                    'submission', 'media release', 'background', 'supporting',
                    'letter', 'standard', 'guide', 'framework', 'review'
                ]
                
                # Check if this is consultation-related content
                is_consultation_content = (
                    any(keyword in link_text.lower() for keyword in consultation_keywords) or
                    any(keyword in href.lower() for keyword in consultation_keywords) or
                    href.endswith('.pdf')
                )
                
                if not is_consultation_content:
                    continue
                
                # Determine link type
                link_type = "other"
                if any(keyword in link_text.lower() for keyword in ['discussion paper', 'consultation paper']):
                    link_type = "consultation_paper"
                elif 'media release' in link_text.lower():
                    link_type = "media_release"
                elif 'submission' in link_text.lower():
                    link_type = "submission"
                elif any(keyword in link_text.lower() for keyword in ['background', 'supporting']):
                    link_type = "supporting_document"
                elif 'letter' in link_text.lower():
                    link_type = "letter"
                elif any(keyword in link_text.lower() for keyword in ['standard', 'guide']):
                    link_type = "regulatory_document"
                    
                links.append({
                    'type': link_type,
                    'url': full_url,
                    'text': link_text
                })
                
        return links

    def _scrape_consultation_detail(self, consultation_url: str) -> Dict:
        """Scrape detailed information from consultation page"""
        try:
            logger.info(f"Scraping consultation detail: {consultation_url}")
            
            # Use Selenium for dynamic content
            if self.driver:
                self.driver.get(consultation_url)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "main"))
                )
                html = self.driver.page_source
            else:
                response = self.session.get(consultation_url, timeout=30)
                response.raise_for_status()
                html = response.text
                
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract main content
            main_content = soup.find('main')
            if not main_content:
                logger.warning(f"No main content found for {consultation_url}")
                return {}
                
            # Extract body text (excluding navigation and metadata)
            body_text = ""
            content_sections = main_content.find_all(['div', 'section'], class_=lambda x: x and 'rich-text' in x)
            
            if content_sections:
                for section in content_sections:
                    body_text += section.get_text(separator=' ', strip=True) + "\n"
            else:
                # Fallback to extracting from main content
                body_text = main_content.get_text(separator=' ', strip=True)
                
            # Clean body text
            body_text = re.sub(r'\s+', ' ', body_text).strip()
            
            # Extract all links
            links = self._extract_consultation_links(soup, consultation_url)
            
            # Process documents (PDFs and other consultation-specific content)
            documents = []
            for link in links:
                # Skip processing if this looks like a general website link
                if link['type'] == 'other' and not any(keyword in link['text'].lower() for keyword in [
                    'paper', 'document', 'standard', 'guide', 'framework', 'review', 'draft'
                ]):
                    continue
                
                if link['url'].lower().endswith('.pdf'):
                    pdf_content = self._extract_pdf_text(link['url'])
                    if pdf_content.strip():  # Only add if PDF has content
                        documents.append({
                            'type': link['type'],
                            'url': link['url'],
                            'title': link['text'],
                            'content': pdf_content
                        })
                elif not any(ext in link['url'].lower() for ext in ['.csv', '.xlsx', '.mp3', '.zip']):
                    # For non-PDF links, only process consultation-specific pages
                    if link['type'] in ['consultation_paper', 'media_release', 'supporting_document', 'letter', 'regulatory_document']:
                        try:
                            if self.driver:
                                self.driver.get(link['url'])
                                time.sleep(2)
                                link_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                            else:
                                link_response = self.session.get(link['url'], timeout=30)
                                link_soup = BeautifulSoup(link_response.text, 'html.parser')
                                
                            link_content = ""
                            # Look for main content in consultation pages
                            content_area = (
                                link_soup.find('main') or 
                                link_soup.find('div', class_=lambda x: x and 'rich-text' in str(x)) or
                                link_soup.find('div', class_=lambda x: x and 'page__middle' in str(x))
                            )
                            
                            if content_area:
                                # Remove navigation and non-content elements
                                for unwanted in content_area.find_all(['nav', 'div'], class_=lambda x: x and any(
                                    exclude in str(x).lower() for exclude in [
                                        'breadcrumb', 'nav', 'menu', 'footer', 'header', 
                                        'newsletter', 'subscribe', 'link-bar'
                                    ]
                                )):
                                    unwanted.decompose()
                                
                                link_content = content_area.get_text(separator=' ', strip=True)
                                link_content = re.sub(r'\s+', ' ', link_content).strip()
                                
                            if link_content.strip():  # Only add if there's actual content
                                documents.append({
                                    'type': link['type'],
                                    'url': link['url'],
                                    'title': link['text'],
                                    'content': link_content
                                })
                                
                        except Exception as e:
                            logger.warning(f"Failed to extract content from {link['url']}: {e}")
            
            # Filter related_links to only consultation-specific links
            consultation_related_links = [
                {'url': link['url'], 'text': link['text']} 
                for link in links 
                if link['type'] != 'other' or any(keyword in link['text'].lower() for keyword in [
                    'paper', 'document', 'standard', 'guide', 'framework', 'review', 'draft'
                ])
            ]
            
            return {
                'body_content': body_text,
                'documents': documents,
                'related_links': consultation_related_links
            }
            
        except Exception as e:
            logger.error(f"Failed to scrape consultation detail {consultation_url}: {e}")
            return {}

    def _scrape_consultation_page(self, industry: str, url: str) -> List[Dict]:
        """Scrape consultations from a category page with status update logic"""
        consultations = []
        updated_consultations = []
        
        try:
            logger.info(f"Scraping {industry} consultations from {url}")
            
            # Use Selenium for dynamic content
            if self.driver:
                self.driver.get(url)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "views-row"))
                )
                html = self.driver.page_source
            else:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                html = response.text
                
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find consultation listings
            consultation_rows = soup.find_all('div', class_='views-row')
            
            for row in consultation_rows:
                try:
                    # Extract basic information
                    title_elem = row.find('h4', class_='listing__title')
                    if not title_elem:
                        continue
                        
                    title_link = title_elem.find('a')
                    if not title_link:
                        continue
                        
                    title = title_link.get_text(strip=True)
                    consultation_url = urljoin(self.base_url, title_link.get('href'))
                    
                    # Extract status
                    status_elem = row.find('div', class_='field-field-consultation-topic-status')
                    status = status_elem.get_text(strip=True) if status_elem else "Unknown"
                    
                    # Extract date
                    date_elem = row.find('div', class_='field-field-topic-date-')
                    date_str = date_elem.get_text(strip=True) if date_elem else ""
                    
                    # Log consultation details for debugging
                    logger.debug(f"Found consultation: '{title}' with status: '{status}' and date: '{date_str}'")
                    
                    # Check if consultation meets filtering criteria
                    if not self._is_consultation_eligible(date_str, status):
                        logger.info(f"Skipping consultation '{title}' - doesn't meet criteria (status: {status}, date: {date_str})")
                        continue
                    
                    # Check if this consultation already exists
                    existing_consultation = self._find_existing_consultation(industry, consultation_url)
                    
                    # Prepare new consultation data for comparison
                    summary_elem = row.find('div', class_='field-field-consultation-summary')
                    summary = summary_elem.get_text(strip=True) if summary_elem else ""
                    
                    new_consultation_basic_data = {
                        'title': title,
                        'status': status,
                        'published_date': date_str,
                        'summary': summary,
                        'url': consultation_url
                    }
                    
                    # Decision logic for existing vs new consultations
                    should_scrape = False
                    is_update = False
                    
                    if existing_consultation:
                        # Check if we should update this existing consultation
                        if self._should_update_consultation(existing_consultation, new_consultation_basic_data):
                            logger.info(f"Updating existing consultation: {title}")
                            should_scrape = True
                            is_update = True
                        else:
                            logger.info(f"Skipping consultation '{title}' - no changes detected")
                            continue
                    else:
                        # New consultation
                        if consultation_url in self.seen_urls:
                            logger.info(f"Skipping already seen consultation: {title}")
                            continue
                        logger.info(f"Found new consultation: {title}")
                        should_scrape = True
                        is_update = False
                    
                    if should_scrape:
                        # Scrape detailed content
                        detail_content = self._scrape_consultation_detail(consultation_url)
                        
                        # Create consultation object
                        current_time = datetime.now().isoformat()
                        
                        consultation = {
                            'title': title,
                            'industry': industry,
                            'published_date': date_str,
                            'closing_date': None,  # Would need to parse from detail page
                            'status': status,
                            'summary': summary,
                            'url': consultation_url,
                            'scraped_date': current_time,
                            'body_content': detail_content.get('body_content', ''),
                            'documents': detail_content.get('documents', []),
                            'related_links': detail_content.get('related_links', []),
                            'consultation_theme': None,  # Would need to extract from content
                            'associated_image_url': None  # Would need to extract from page
                        }
                        
                        if is_update:
                            # Update existing consultation record
                            updated_consultation = self._update_consultation_record(existing_consultation, consultation)
                            updated_consultations.append(updated_consultation)
                            logger.info(f"Successfully updated: {title} (Status: {status})")
                        else:
                            # New consultation - add tracking fields
                            consultation['first_scraped_date'] = current_time
                            consultation['last_updated'] = current_time
                            consultation['status_history'] = [{
                                'status': status,
                                'timestamp': current_time,
                                'note': 'Initial scrape'
                            }]
                            consultations.append(consultation)
                            self.seen_urls.add(consultation_url)
                            logger.info(f"Successfully scraped new: {title} (Status: {status})")
                        
                        # Add delay between requests
                        time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing consultation row: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error scraping {industry} consultations: {e}")
            
        logger.info(f"Scraped {len(consultations)} new consultations and updated {len(updated_consultations)} existing consultations from {industry}")
        return consultations, updated_consultations

    def scrape_all_consultations(self) -> Dict:
        """Scrape all consultations from all industries with update tracking"""
        all_consultations = {}
        all_updates = {}
        
        # Perform session walking
        self._session_walk()
        
        for industry, url in self.consultation_urls.items():
            try:
                consultations, updated_consultations = self._scrape_consultation_page(industry, url)
                all_consultations[industry] = consultations
                all_updates[industry] = updated_consultations
                
                logger.info(f"Scraped {len(consultations)} new and updated {len(updated_consultations)} consultations from {industry}")
                
                # Add delay between industry pages
                time.sleep(3)
                
            except Exception as e:
                logger.error(f"Failed to scrape {industry}: {e}")
                all_consultations[industry] = []
                all_updates[industry] = []
                
        return all_consultations, all_updates

    def save_results(self, consultations: Dict, updated_consultations: Dict):
        """Save results to JSON file with update handling"""
        try:
            # Start with existing data
            merged_data = self.existing_consultations.copy()
            
            # Process new consultations
            for industry, industry_consultations in consultations.items():
                if industry not in merged_data:
                    merged_data[industry] = []
                    
                # Add only new consultations
                existing_urls = {c.get('url') for c in merged_data[industry]}
                for consultation in industry_consultations:
                    if consultation['url'] not in existing_urls:
                        merged_data[industry].append(consultation)
            
            # Process updated consultations
            for industry, industry_updates in updated_consultations.items():
                if industry not in merged_data:
                    merged_data[industry] = []
                    
                # Update existing consultations
                for updated_consultation in industry_updates:
                    updated_url = updated_consultation['url']
                    
                    # Find and replace the existing consultation
                    for i, existing in enumerate(merged_data[industry]):
                        if existing.get('url') == updated_url:
                            merged_data[industry][i] = updated_consultation
                            logger.info(f"Updated consultation record: {updated_consultation.get('title')}")
                            break
                    else:
                        # If not found in existing data, add as new
                        merged_data[industry].append(updated_consultation)
                        logger.warning(f"Updated consultation not found in existing data, added as new: {updated_consultation.get('title')}")
            
            # Save to file
            with open('data/apra_consultations.json', 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, indent=2, ensure_ascii=False)
                
            # Save seen URLs
            self._save_seen_urls()
            
            # Create backup with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f'data/apra_consultations_backup_{timestamp}.json'
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, indent=2, ensure_ascii=False)
            
            logger.info("Results saved successfully")
            logger.info(f"Backup created: {backup_path}")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

    def generate_status_report(self) -> Dict:
        """Generate a report of status changes and updates"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_consultations': 0,
            'status_distribution': {},
            'recent_status_changes': [],
            'industries': {}
        }
        
        try:
            for industry, consultations in self.existing_consultations.items():
                industry_data = {
                    'total': len(consultations),
                    'status_distribution': {},
                    'recent_changes': []
                }
                
                for consultation in consultations:
                    # Count total
                    report['total_consultations'] += 1
                    
                    # Count by status
                    status = consultation.get('status', 'Unknown')
                    report['status_distribution'][status] = report['status_distribution'].get(status, 0) + 1
                    industry_data['status_distribution'][status] = industry_data['status_distribution'].get(status, 0) + 1
                    
                    # Check for recent status changes
                    status_history = consultation.get('status_history', [])
                    if len(status_history) > 1:
                        # Get the most recent status change
                        latest_change = status_history[-1]
                        try:
                            change_time = datetime.fromisoformat(latest_change['timestamp'].replace('Z', '+00:00'))
                            # If changed in last 7 days
                            if datetime.now() - change_time <= timedelta(days=7):
                                change_info = {
                                    'title': consultation.get('title'),
                                    'industry': industry,
                                    'change': latest_change,
                                    'url': consultation.get('url')
                                }
                                report['recent_status_changes'].append(change_info)
                                industry_data['recent_changes'].append(change_info)
                        except Exception as e:
                            logger.warning(f"Error processing status history: {e}")
                
                report['industries'][industry] = industry_data
            
            # Sort recent changes by timestamp
            report['recent_status_changes'].sort(key=lambda x: x['change']['timestamp'], reverse=True)
            
            # Save report
            with open('data/status_report.json', 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate status report: {e}")
            return report

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()

def main():
    """Main execution function - batch mode compatible"""
    # Default to incremental scraping (uses seen URLs to avoid duplicates)
    # Set force_fresh_scrape=True to ignore previously seen URLs and scrape everything
    force_fresh_scrape = False
    
    # Check for command line argument to force fresh scrape
    import sys
    if len(sys.argv) > 1 and sys.argv[1].lower() in ['--fresh', '-f', '--force-fresh']:
        force_fresh_scrape = True
        logger.info("Fresh scrape mode enabled via command line argument")
    
    scraper = APRAConsultationScraper(force_fresh_scrape=force_fresh_scrape)
    
    try:
        logger.info("Starting APRA consultation scraping in batch mode...")
        
        # Log scraping mode
        if force_fresh_scrape:
            logger.info("Running in FRESH SCRAPE mode - ignoring previously seen URLs")
        else:
            logger.info(f"Running in INCREMENTAL mode - found {len(scraper.seen_urls)} previously seen URLs")
        
        consultations, updated_consultations = scraper.scrape_all_consultations()
        scraper.save_results(consultations, updated_consultations)
        
        # Generate status report
        status_report = scraper.generate_status_report()
        
        # Log summary
        total_new = sum(len(industry_consultations) for industry_consultations in consultations.values())
        total_updated = sum(len(industry_updates) for industry_updates in updated_consultations.values())
        
        logger.info(f"Scraping completed. New consultations: {total_new}, Updated consultations: {total_updated}")
        
        # Log detailed summary by industry
        for industry in consultations.keys():
            new_count = len(consultations[industry])
            updated_count = len(updated_consultations[industry])
            if new_count > 0 or updated_count > 0:
                logger.info(f"  {industry}: {new_count} new, {updated_count} updated consultations")
        
        # Log status breakdown for debugging
        logger.info(f"Overall status distribution: {status_report.get('status_distribution', {})}")
        
        # Log recent status changes
        recent_changes = status_report.get('recent_status_changes', [])
        if recent_changes:
            logger.info(f"Recent status changes detected: {len(recent_changes)}")
            for change in recent_changes[:5]:  # Show first 5
                logger.info(f"  - {change['title']}: {change['change']['note']}")
        else:
            logger.info("No recent status changes detected")
        
        # Log completion time
        logger.info(f"Batch scraping completed successfully at {datetime.now().isoformat()}")
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        # Exit with error code for batch processing
        import sys
        sys.exit(1)
        
    finally:
        scraper.cleanup()

if __name__ == "__main__":
    main()