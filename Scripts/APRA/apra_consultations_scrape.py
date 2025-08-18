#!/usr/bin/env python3
"""
APRA Consultations Web Scraper
Scrapes consultation data from APRA website across multiple industries
with anti-bot measures, PDF processing, and deduplication support.
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
    """Main scraper class for APRA consultations"""
    
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
        """Scrape consultations from a category page"""
        consultations = []
        
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
                    
                    # Skip if already seen
                    if consultation_url in self.seen_urls:
                        logger.info(f"Skipping already seen consultation: {title}")
                        continue
                    
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
                    
                    # Extract summary
                    summary_elem = row.find('div', class_='field-field-consultation-summary')
                    summary = summary_elem.get_text(strip=True) if summary_elem else ""
                    
                    # Scrape detailed content
                    detail_content = self._scrape_consultation_detail(consultation_url)
                    
                    # Create consultation object
                    consultation = {
                        'title': title,
                        'industry': industry,
                        'published_date': date_str,
                        'closing_date': None,  # Would need to parse from detail page
                        'status': status,
                        'summary': summary,
                        'url': consultation_url,
                        'scraped_date': datetime.now().isoformat(),
                        'body_content': detail_content.get('body_content', ''),
                        'documents': detail_content.get('documents', []),
                        'related_links': detail_content.get('related_links', []),
                        'consultation_theme': None,  # Would need to extract from content
                        'associated_image_url': None  # Would need to extract from page
                    }
                    
                    consultations.append(consultation)
                    self.seen_urls.add(consultation_url)
                    
                    logger.info(f"Successfully scraped: {title} (Status: {status})")
                    
                    # Add delay between requests
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing consultation row: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error scraping {industry} consultations: {e}")
            
        return consultations

    def scrape_all_consultations(self) -> Dict:
        """Scrape all consultations from all industries"""
        all_consultations = {}
        
        # Perform session walking
        self._session_walk()
        
        for industry, url in self.consultation_urls.items():
            try:
                consultations = self._scrape_consultation_page(industry, url)
                all_consultations[industry] = consultations
                
                logger.info(f"Scraped {len(consultations)} consultations from {industry}")
                
                # Add delay between industry pages
                time.sleep(3)
                
            except Exception as e:
                logger.error(f"Failed to scrape {industry}: {e}")
                all_consultations[industry] = []
                
        return all_consultations

    def save_results(self, consultations: Dict):
        """Save results to JSON file"""
        try:
            # Merge with existing data
            merged_data = self.existing_consultations.copy()
            
            for industry, industry_consultations in consultations.items():
                if industry not in merged_data:
                    merged_data[industry] = []
                    
                # Add only new consultations
                existing_urls = {c.get('url') for c in merged_data[industry]}
                for consultation in industry_consultations:
                    if consultation['url'] not in existing_urls:
                        merged_data[industry].append(consultation)
            
            # Save to file
            with open('data/apra_consultations.json', 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, indent=2, ensure_ascii=False)
                
            # Save seen URLs
            self._save_seen_urls()
            
            logger.info("Results saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

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
        
        consultations = scraper.scrape_all_consultations()
        scraper.save_results(consultations)
        
        # Log summary
        total_consultations = sum(len(industry_consultations) for industry_consultations in consultations.values())
        logger.info(f"Scraping completed. Total new consultations: {total_consultations}")
        
        # Log detailed summary by industry
        for industry, industry_consultations in consultations.items():
            if industry_consultations:
                logger.info(f"  {industry}: {len(industry_consultations)} new consultations")
        
        # Log status breakdown for debugging
        status_counts = {}
        for industry, industry_consultations in consultations.items():
            for consultation in industry_consultations:
                status = consultation.get('status', 'Unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
        
        if status_counts:
            logger.info(f"Status breakdown: {status_counts}")
        else:
            logger.info("No new consultations found")
        
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