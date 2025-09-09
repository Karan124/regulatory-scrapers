#!/usr/bin/env python3
"""
Australian Government Transparency Portal Scraper

Scrapes Annual Reports, Corporate Plans, and Portfolio Budget Statements
from https://www.transparency.gov.au/publications

Features:
- Anti-bot detection avoidance with stealth techniques
- PDF extraction with table support
- Proper deduplication logic using unique IDs
- LLM-friendly structured output
- Comprehensive logging
- Session-based browsing simulation
"""

import os
import re
import json
import time
import logging
import hashlib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
from pathlib import Path

# Third-party imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from bs4 import BeautifulSoup
import PyPDF2
import pdfplumber
from fake_useragent import UserAgent


class TransparencyPortalScraper:
    """
    Production-grade scraper for Australian Government Transparency Portal
    """
    
    def __init__(self, max_pages: Optional[int] = 2, data_dir: str = "data", logs_dir: str = "logs", headless: bool = True):
        """
        Initialize the scraper
        
        Args:
            max_pages: Maximum number of pages to scrape (None for all pages)
            data_dir: Directory to save scraped data
            logs_dir: Directory to save log files
            headless: Run browser in headless mode
        """
        self.base_url = "https://www.transparency.gov.au"
        self.publications_url = f"{self.base_url}/publications"
        self.max_pages = max_pages
        self.data_dir = Path(data_dir)
        self.logs_dir = Path(logs_dir)
        self.headless = headless
        
        # Create directories
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        
        # Output file path
        self.output_file = self.data_dir / "transparency_publications.json"
        
        # Setup logging
        self.setup_logging()
        
        # Initialize session and driver
        self.session = requests.Session()
        self.driver = None
        self.setup_session()
        
        # FIXED: Deduplication tracking using unique IDs instead of URLs
        self.scraped_unique_ids: Set[str] = set()
        self.existing_data: List[Dict] = []
        self.load_existing_data()
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'new_records': 0,
            'duplicates_skipped': 0,
            'errors': 0,
            'pdf_extractions': 0,
            'pdf_pages_processed': 0,
            'pdf_size_mb_total': 0,
            'large_pdfs_skipped': 0
        }
        
        # PDF processing configuration - Set after other initialization
        self.MAX_PDF_PAGES = 2000
        self.BATCH_SIZE_PAGES = 100  
        self.MAX_PDF_SIZE_MB = 500
        self.PDF_TIMEOUT = 300
        self.MAX_PDFS_PER_PAGE = 5
        self.MAX_ANNUAL_REPORT_SECTIONS = 50  # Maximum sections to process per Annual Report
        
    def setup_logging(self):
        """Configure logging with daily log files and warning suppression"""
        log_file = self.logs_dir / f"scrape_log_{datetime.now().strftime('%Y%m%d')}.txt"
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        # Suppress PyPDF2 warnings
        logging.getLogger("PyPDF2").setLevel(logging.ERROR)
        logging.getLogger("pdfplumber").setLevel(logging.ERROR)
        
        # Suppress urllib3 warnings for connection cleanup
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== Starting Australian Government Transparency Portal Scraper ===")
        
        # Also suppress warnings at the module level
        import warnings
        warnings.filterwarnings("ignore", message=".*gray non-stroke color.*")
        warnings.filterwarnings("ignore", message=".*Cannot set.*")
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        
    def setup_session(self):
        """Setup session with realistic headers and Chrome driver"""
        ua = UserAgent()
        
        # Realistic headers
        self.session.headers.update({
            'User-Agent': ua.chrome,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Setup Chrome driver using the working function
        try:
            self.driver = self.setup_driver()
            self.logger.info("Chrome driver initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            raise
    
    def setup_driver(self):
        """Simplified Chrome WebDriver setup - let system handle Chrome detection."""
        chrome_options = Options()
        
        # Essential stability options for Linux
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Updated user agent to match current Chrome version
        ua = UserAgent()
        chrome_options.add_argument(f'--user-agent={ua.chrome}')
        
        # Stealth options
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Performance optimizations
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        
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
            
            # Initialize driver with simplified service configuration
            service_kwargs = {}
            if chromedriver_path:
                service_kwargs['executable_path'] = chromedriver_path
            
            service = Service(**service_kwargs)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(30)
            
            # Remove automation indicators
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return driver
            
        except WebDriverException as e:
            self.logger.error(f"Failed to initialize WebDriver: {e}")
            self.logger.error("Please ensure chromedriver is installed and in your PATH.")
            raise
            
    def load_existing_data(self):
        """FIXED: Load existing scraped data to avoid duplicates using unique IDs"""
        if self.output_file.exists():
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    self.existing_data = json.load(f)
                    
                # FIXED: Extract unique IDs for deduplication instead of URLs
                for item in self.existing_data:
                    if 'unique_id' in item:
                        self.scraped_unique_ids.add(item['unique_id'])
                    else:
                        # For backwards compatibility, create unique ID for existing records without one
                        unique_id = self.create_unique_id(
                            item.get('title', ''),
                            item.get('published_date', ''),
                            item.get('url', '')
                        )
                        item['unique_id'] = unique_id
                        self.scraped_unique_ids.add(unique_id)
                        
                self.logger.info(f"Loaded {len(self.existing_data)} existing records with {len(self.scraped_unique_ids)} unique IDs")
                
            except Exception as e:
                self.logger.warning(f"Could not load existing data: {e}")
                self.existing_data = []
                
    def create_unique_id(self, title: str, published_date: str, url: str) -> str:
        """Create a unique identifier for deduplication"""
        content = f"{title}_{published_date}_{url}"
        return hashlib.md5(content.encode()).hexdigest()
        
    def simulate_human_navigation(self):
        """Simulate human browsing behavior"""
        try:
            # Start from homepage
            self.logger.info("Navigating to homepage...")
            self.driver.get(self.base_url)
            time.sleep(2)
            
            # Navigate to publications page
            self.logger.info("Navigating to publications page...")
            self.driver.get(self.publications_url)
            time.sleep(3)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Navigation failed: {e}")
            return False
            
    def get_publication_listings_from_page(self) -> List[Dict]:
        """
        Get publication listings from the current page in the browser.
        
        Returns:
            List of publication metadata dictionaries.
        """
        try:
            # Wait for publication items to be present on the page.
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li.PublicationsList_publicationCard__NURkI"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            publications = []
            
            publication_items = soup.find_all('li', class_='PublicationsList_publicationCard__NURkI')
            
            for item in publication_items:
                pub_data = self.extract_publication_metadata(item)
                if pub_data:
                    publications.append(pub_data)
            
            return publications
            
        except TimeoutException:
            self.logger.warning(f"Timeout waiting for publications on URL: {self.driver.current_url}")
            return []
        except Exception as e:
            self.logger.error(f"Failed to get listings from {self.driver.current_url}: {e}")
            return []
            
    def extract_publication_metadata(self, item_soup) -> Optional[Dict]:
        """
        Extract metadata from a publication listing item
        
        Args:
            item_soup: BeautifulSoup object of the publication item
            
        Returns:
            Dictionary with publication metadata
        """
        try:
            # Extract title from cardTitle span
            title_elem = item_soup.find('span', class_='cardTitle') or \
                        item_soup.find('h2') or item_soup.find('h3')
            if title_elem:
                # Remove SVG icons from title
                for svg in title_elem.find_all('svg'):
                    svg.decompose()
                title = title_elem.get_text(strip=True)
            else:
                title = "Unknown Title"
            
            # Extract URL - get the MAIN publication link, not the portfolio link
            main_link = None
            
            # Look for the main publication link (usually the one with target="_blank" or the title link)
            all_links = item_soup.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href', '')
                
                # Skip portfolio/entity links - we want the actual publication link
                if '/portfolio-entities-companies/' in href:
                    continue
                
                # Priority 1: External links (ANAO, direct publication links)
                if href.startswith('http') and ('anao.gov.au' in href or 'previewapi.transparency.gov.au' in href):
                    main_link = href
                    break
                
                # Priority 2: Links containing the title or main content
                if title_elem and title_elem.find_parent('a'):
                    parent_link = title_elem.find_parent('a')
                    if parent_link.get('href'):
                        main_link = parent_link.get('href')
                        break
            
            # Fallback: if no main link found, get the first non-portfolio link
            if not main_link:
                for link in all_links:
                    href = link.get('href', '')
                    if href and not href.startswith('#') and '/portfolio-entities-companies/' not in href:
                        main_link = href
                        break
            
            if not main_link:
                self.logger.warning(f"No valid publication link found for: {title}")
                return None
                
            # Ensure URL is absolute
            if not main_link.startswith('http'):
                url = urljoin(self.base_url, main_link)
            else:
                url = main_link
            
            # Extract portfolio/entity info from the portfolioInfo section
            portfolio_elem = item_soup.find('span', class_='portfolioInfo')
            portfolio = None
            if portfolio_elem:
                portfolio_link = portfolio_elem.find('a')
                if portfolio_link:
                    portfolio = portfolio_link.get_text(strip=True)
            
            # Extract publication type and year from tags
            publication_type = "Annual Report"  # Default
            year = None
            
            tags_container = item_soup.find('span', class_='tags')
            if tags_container:
                tags = tags_container.find_all('span', class_='Tag_tag__MJvdO')
                for tag in tags:
                    tag_text = tag.get_text(strip=True)
                    if tag_text == "CP":
                        publication_type = "Corporate Plan"
                    elif tag_text == "PBS":
                        publication_type = "Portfolio Budget Statement"
                    elif tag_text == "AR":
                        publication_type = "Annual Report"
                    elif re.match(r'\d{4}-\d{2}', tag_text):
                        year = tag_text
            
            # Try to extract year from title if not found in tags
            if not year:
                year_match = re.search(r'(\d{4}-\d{2})', title)
                if year_match:
                    year = year_match.group(1)
                else:
                    year = datetime.now().strftime('%Y-%m')
            
            # Create published date from year
            if year and '-' in year:
                try:
                    year_part = year.split('-')[0]
                    published_date = f"{year_part}-07-01"  # Use July 1st as default
                except:
                    published_date = datetime.now().strftime('%Y-%m-%d')
            else:
                published_date = datetime.now().strftime('%Y-%m-%d')
            
            pub_data = {
                'title': title,
                'url': url,
                'published_date': published_date,
                'publication_type': publication_type,
                'theme': portfolio,
                'year': year,
                'scraped_date': datetime.now().isoformat()
            }
            
            # FIXED: Create unique ID immediately when extracting metadata
            pub_data['unique_id'] = self.create_unique_id(title, published_date, url)
            
            return pub_data
            
        except Exception as e:
            self.logger.error(f"Failed to extract metadata: {e}")
            return None
            
    def determine_publication_type(self, title: str) -> str:
        """Determine publication type from title"""
        title_lower = title.lower()
        
        if 'annual report' in title_lower:
            return 'Annual Report'
        elif 'corporate plan' in title_lower:
            return 'Corporate Plan'
        elif 'portfolio budget statement' in title_lower or 'pbs' in title_lower:
            return 'Portfolio Budget Statement'
        else:
            # Default based on common patterns
            if 'report' in title_lower:
                return 'Annual Report'
            elif 'plan' in title_lower:
                return 'Corporate Plan'
            else:
                return 'Annual Report'  # Default fallback
                
    def extract_date(self, date_elem) -> str:
        """Extract and standardize date"""
        if not date_elem:
            return datetime.now().strftime('%Y-%m-%d')
            
        if hasattr(date_elem, 'get_text'):
            date_text = date_elem.get_text(strip=True)
        else:
            date_text = str(date_elem).strip()
            
        # Try to parse various date formats
        date_patterns = [
            r'(\d{1,2})/(\d{1,2})/(\d{4})',  # DD/MM/YYYY
            r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-MM-DD
            r'(\d{1,2})-(\d{1,2})-(\d{4})'   # DD-MM-YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_text)
            if match:
                try:
                    if len(match.group(3)) == 4:  # YYYY format
                        return f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"
                    else:  # Assume DD/MM/YYYY or DD-MM-YYYY
                        return f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"
                except:
                    continue
                    
        return datetime.now().strftime('%Y-%m-%d')
        
    def scrape_publication_content(self, pub_data: Dict) -> Dict:
        """
        Scrape full content from a publication page. This function now expects
        to be called with a specific URL and does not handle pagination.
        
        Args:
            pub_data: Publication metadata dictionary
            
        Returns:
            Updated dictionary with full content
        """
        try:
            self.logger.info(f"Scraping content for: {pub_data['title']}")
            
            # Handle external URLs (like ANAO links) or direct PDFs
            if (pub_data['url'].startswith('https://www.anao.gov.au') or 
                pub_data['url'].startswith('https://previewapi.transparency.gov.au') or
                'anao.gov.au' in pub_data['url'] or
                pub_data['url'].endswith('.pdf')):
                
                return self.scrape_external_content(pub_data)
            
            # Navigate to the internal page
            self.driver.get(pub_data['url'])
            time.sleep(3)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract main content based on publication type
            if pub_data['publication_type'] == 'Annual Report':
                content_text = self.scrape_annual_report_content(soup, pub_data['url'])
                pdf_content = "" # PDFs handled within AR scraper
            elif pub_data['publication_type'] == 'Corporate Plan':
                content_text = self.scrape_corporate_plan_content(soup)
                pdf_links = self.find_pdf_links(soup, pub_data['url'])
                pdf_content = ""
                for pdf_url in pdf_links[:2]:
                    pdf_text = self.extract_pdf_content(pdf_url)
                    if pdf_text:
                        pdf_content += f"\n\n--- PDF Content from {pdf_url} ---\n{pdf_text}"
            else: # Portfolio Budget Statements and others
                content_text = self.extract_text_content(soup)
                pdf_links = self.find_pdf_links(soup, pub_data['url'])
                pdf_content = ""
                for pdf_url in pdf_links[:self.MAX_PDFS_PER_PAGE]:
                    pdf_text = self.extract_pdf_content(pdf_url)
                    if pdf_text:
                        pdf_content += f"\n\n--- PDF Content from {pdf_url} ---\n{pdf_text}"
            
            related_links = self.extract_related_links(soup, pub_data['url'])
            
            pub_data.update({
                'content_text': self.clean_text(content_text),
                'content_pdf_text': self.clean_text(pdf_content) if pdf_content else None,
                'related_links': related_links,
                'associated_image': None
            })
            
            self.stats['total_processed'] += 1
            time.sleep(1) # Small delay between content scrapes
            return pub_data
            
        except Exception as e:
            self.logger.error(f"Failed to scrape content for {pub_data['url']}: {e}")
            self.stats['errors'] += 1
            # Return original data so it's not lost, but without content
            return pub_data
    
    def scrape_external_content(self, pub_data: Dict) -> Dict:
        """Handle external URLs (ANAO, PDF links, etc.)"""
        try:
            if pub_data['url'].endswith('.pdf'):
                pdf_text = self.extract_pdf_content(pub_data['url'])
                content_text = f"PDF Document: {pub_data['title']}"
                pdf_content = pdf_text if pdf_text else ""
                related_links = []
            else:
                # For external web pages, we need the driver
                self.driver.get(pub_data['url'])
                time.sleep(3)
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                if 'anao.gov.au' in pub_data['url']:
                    content_text = self.scrape_anao_content(soup)
                else:
                    content_text = self.extract_text_content(soup)
                
                pdf_links = self.find_pdf_links(soup, pub_data['url'])
                pdf_content = ""
                for pdf_url in pdf_links[:3]:
                    pdf_text = self.extract_pdf_content(pdf_url)
                    if pdf_text:
                        pdf_content += pdf_text
                
                related_links = self.extract_content_links(soup, pub_data['url'])
            
            pub_data.update({
                'content_text': self.clean_text(content_text),
                'content_pdf_text': self.clean_text(pdf_content) if pdf_content else None,
                'related_links': related_links
            })
            self.stats['total_processed'] += 1
            return pub_data
            
        except Exception as e:
            self.logger.error(f"Failed to scrape external content for {pub_data['url']}: {e}")
            self.stats['errors'] += 1
            return pub_data
    
    def scrape_anao_content(self, soup: BeautifulSoup) -> str:
        """Extract content from ANAO website pages based on provided HTML structure"""
        content_parts = []
        
        # Extract title
        title_elem = soup.find('h2') or soup.find('div', class_='field--name-node-title')
        if title_elem:
            content_parts.append(f"Title: {title_elem.get_text(strip=True)}")
        
        # Extract audit objective summary
        summary_elem = soup.find('div', class_='audit-objective-summary')
        if summary_elem:
            content_parts.append(f"Summary: {summary_elem.get_text(strip=True)}")
        
        # Extract all field items (main content)
        field_items = soup.find_all('div', class_='field__item')
        for item in field_items:
            # Skip if it's just images or empty
            text = item.get_text(strip=True)
            if text and len(text) > 50:
                content_parts.append(text)
        
        # Extract chapter content specifically
        chapter_bodies = soup.find_all('div', class_='field--name-field-chapter-body')
        for body in chapter_bodies:
            text = body.get_text(separator='\n', strip=True)
            if text and len(text) > 50:
                content_parts.append(text)
        
        # Extract any additional text-formatted content
        formatted_content = soup.find_all('div', class_='clearfix text-formatted')
        for content in formatted_content:
            text = content.get_text(separator='\n', strip=True)
            if text and len(text) > 50:
                content_parts.append(text)
        
        return '\n\n'.join(content_parts)
    
    def extract_content_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract relevant links only from content areas, not navigation/footer"""
        related_links = []
        
        # Define content areas to look for links
        content_areas = [
            soup.find('div', class_='field__item'),
            soup.find('div', class_='clearfix text-formatted'),
            soup.find('div', class_='audit-objective-summary'),
            soup.find('div', class_='related-documents-container'),
            soup.find('main'),
            soup.find('article')
        ]
        
        # URLs to exclude (navigation, footer, common pages)
        exclude_patterns = [
            '/about', '/contact', '/privacy', '/accessibility', '/copyright',
            '/feedback', '/portfolio-entities-companies', 'javascript:', 'mailto:',
            '#', 'facebook', 'twitter', 'linkedin', 'instagram', 'youtube'
        ]
        
        for content_area in content_areas:
            if content_area:
                links = content_area.find_all('a', href=True)
                
                for link in links:
                    href = link['href']
                    full_url = urljoin(base_url, href)
                    
                    # Skip if it matches exclude patterns
                    if any(pattern in full_url.lower() for pattern in exclude_patterns):
                        continue
                    
                    # Only include if it looks like a document or relevant resource
                    if any(ext in full_url.lower() for ext in ['.pdf', '.doc', '.xls', 'report', 'plan', 'statement', 'audit']):
                        related_links.append(full_url)
        
        return list(set(related_links))[:5]  # Limit and deduplicate
    
    def scrape_annual_report_content(self, soup: BeautifulSoup, base_url: str) -> str:
        """Scrape Annual Report with navigation-based pagination - handles various AR structures"""
        content_text = ""
        discovered_sections = []
        
        # Get main content from current page first
        main_content = self.extract_annual_report_page_content(soup)
        if main_content:
            content_text += main_content
        
        # Try multiple navigation patterns for different Annual Report structures
        nav_links = self.find_annual_report_navigation_links(soup)
        
        visited_urls = set()
        visited_urls.add(base_url)  # Don't revisit the main page
        
        for nav_link in nav_links[:self.MAX_ANNUAL_REPORT_SECTIONS]:  # Configurable section limit
            if 'href' in nav_link.attrs:
                section_url = urljoin(base_url, nav_link['href'])
                section_name = nav_link.get_text(strip=True)
                
                # Skip if already visited or if it's not a valid section link
                if section_url in visited_urls or not self.is_valid_section_url(section_url, base_url):
                    continue
                    
                visited_urls.add(section_url)
                discovered_sections.append(section_name)
                
                try:
                    self.logger.info(f"Scraping AR section: '{section_name}' -> {section_url}")
                    self.driver.get(section_url)
                    time.sleep(2)
                    
                    section_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    section_content = self.extract_annual_report_page_content(section_soup)
                    
                    if section_content:
                        content_text += f"\n\n=== {section_name} ===\n{section_content}"
                    else:
                        self.logger.warning(f"No content extracted from section: {section_name}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to scrape AR section '{section_name}': {e}")
                    continue
        
        # Log the dynamic section discovery summary
        if discovered_sections:
            self.logger.info(f"Annual Report sections discovered and processed:")
            for i, section in enumerate(discovered_sections, 1):
                self.logger.info(f"  {i}. {section}")
            
            # Add section summary to content
            section_list = '\n'.join([f"{i}. {section}" for i, section in enumerate(discovered_sections, 1)])
            content_text = f"=== ANNUAL REPORT SECTIONS PROCESSED ===\n{section_list}\n\n{content_text}"
        else:
            self.logger.warning("No additional sections discovered for this Annual Report")
        
        return content_text
    
    def find_annual_report_navigation_links(self, soup: BeautifulSoup) -> list:
        """Find navigation links for Annual Reports - dynamically discovers all sections"""
        nav_links = []
        
        # Navigation pattern 1: Standard side navigation (like AAF Company)
        side_nav_links = soup.find_all('a', class_='SideNavigation_item__AGFGH')
        nav_links.extend(side_nav_links)
        
        # Navigation pattern 2: Alternative side navigation patterns
        alt_nav_selectors = [
            'nav a[href*="annual-report"]',
            '.side-navigation a',
            '.navigation a',
            '.menu a[href*="annual-report"]',
            '.sidebar a[href*="annual-report"]',
            '.nav-menu a',
            '.contents a',
            '.toc a',  # Table of contents
            '.index a'
        ]
        
        for selector in alt_nav_selectors:
            try:
                links = soup.select(selector)
                nav_links.extend(links)
            except:
                continue
        
        # Navigation pattern 3: Accordion/collapsible navigation
        accordion_links = soup.select('.Accordion_accordion__k7ycz a[href]')
        nav_links.extend(accordion_links)
        
        # Navigation pattern 4: Dynamic section discovery from main content
        # Look for any links that point to subsections of the current Annual Report
        main_content_areas = [
            soup.find('main'),
            soup.find('article'),
            soup.find(class_='content'),
            soup.find(id='main-content'),
            soup.find(class_='annual-report'),
            soup.find(class_='report-content')
        ]
        
        for content_area in main_content_areas:
            if content_area:
                # Find ALL links within content that could be sections
                links = content_area.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    link_text = link.get_text(strip=True)
                    
                    # Include if it looks like a section link (has meaningful text and relative URL)
                    if (href.startswith('/') or href.startswith('./') or not href.startswith('http')) and \
                       len(link_text) > 3 and len(link_text) < 100:
                        nav_links.append(link)
        
        # Navigation pattern 5: Page navigation (Next/Previous)
        page_nav_links = soup.select('#PageNavigation_pageNavigation__3wEBs a[href]')
        nav_links.extend(page_nav_links)
        
        # Remove duplicates while preserving order and log discovered sections
        seen_urls = set()
        unique_links = []
        discovered_sections = []
        
        for link in nav_links:
            href = link.get('href', '')
            link_text = link.get_text(strip=True)
            
            if href and href not in seen_urls and link_text:
                seen_urls.add(href)
                unique_links.append(link)
                discovered_sections.append(link_text)
        
        if discovered_sections:
            self.logger.info(f"Dynamically discovered {len(unique_links)} AR sections:")
            for i, section in enumerate(discovered_sections[:10], 1):  # Log first 10 sections
                self.logger.info(f"  {i}. {section}")
            if len(discovered_sections) > 10:
                self.logger.info(f"  ... and {len(discovered_sections) - 10} more sections")
        
        return unique_links
    
    def is_valid_section_url(self, url: str, base_url: str) -> bool:
        """Check if URL is a valid Annual Report section"""
        try:
            # Must be from the same annual report
            if not url.startswith(base_url.rstrip('/')):
                return False
            
            # Skip certain URL patterns that aren't content sections
            skip_patterns = [
                '#', 'javascript:', 'mailto:', 'tel:',
                '/search', '/contact', '/feedback', '/about',
                '.pdf', '.doc', '.xlsx', '.zip'
            ]
            
            for pattern in skip_patterns:
                if pattern in url.lower():
                    return False
            
            # URL should have additional path segments (indicating a section)
            base_path = base_url.rstrip('/').split('/')
            url_path = url.rstrip('/').split('/')
            
            return len(url_path) > len(base_path)
            
        except:
            return False
    
    def extract_annual_report_page_content(self, soup: BeautifulSoup) -> str:
        """Extract content from a single Annual Report page - handles multiple layouts"""
        content_parts = []
        
        # Try multiple content extraction strategies for different AR layouts
        
        # Strategy 1: Standard Annual Report structure (like AAF Company)
        section_title = soup.find('h1', class_='AnnualReportArticle_sectionTitle__QYYn2')
        if section_title:
            content_parts.append(f"Section: {section_title.get_text(strip=True)}")
        
        article_content = soup.find('div', class_='AnnualReportArticle_articleContent__eheNu')
        if article_content:
            text_content = article_content.get_text(separator='\n', strip=True)
            if text_content and len(text_content) > 50:
                content_parts.append(text_content)
        
        # Strategy 2: Check for embedded PDF viewer content
        pdf_viewer = soup.find('div', class_='PdfViewer_pdfViewer__aPXMm')
        if pdf_viewer:
            self.logger.info("Found embedded PDF viewer, extracting text content")
            pdf_text_content = self.extract_pdf_viewer_content(pdf_viewer)
            if pdf_text_content:
                content_parts.append(f"--- PDF Viewer Content ---\n{pdf_text_content}")
        
        # Strategy 3: Alternative Annual Report layouts
        if not content_parts:
            # Try various content selectors for different layouts
            content_selectors = [
                # Main content areas
                'main article',
                'article.content',
                '.main-content',
                '.report-content',
                '.annual-report-content',
                
                # Section content
                'section.content',
                '.section-content',
                '.page-content',
                
                # Generic content areas
                '.content',
                '#content',
                'main',
                'article'
            ]
            
            for selector in content_selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        text = elem.get_text(separator='\n', strip=True)
                        if text and len(text) > 50:
                            content_parts.append(text)
                            break
                    if content_parts:
                        break
                except:
                    continue
        
        # Strategy 4: Look for specific Annual Report elements
        if not content_parts:
            ar_specific_selectors = [
                'h1, h2, h3',  # Headers
                'p',           # Paragraphs
                'table',       # Tables
                'div.text',    # Text divisions
                'section'      # Sections
            ]
            
            page_content = []
            
            # Extract headers first
            headers = soup.find_all(['h1', 'h2', 'h3'])
            for header in headers[:5]:  # Limit headers
                header_text = header.get_text(strip=True)
                if header_text and len(header_text) > 3:
                    page_content.append(f"=== {header_text} ===")
            
            # Extract substantial paragraphs
            paragraphs = soup.find_all('p')
            for para in paragraphs:
                para_text = para.get_text(strip=True)
                if para_text and len(para_text) > 100:  # Only substantial paragraphs
                    page_content.append(para_text)
            
            # Extract table content
            tables = soup.find_all('table')
            for i, table in enumerate(tables[:3]):  # Limit tables
                table_text = table.get_text(separator=' | ', strip=True)
                if table_text and len(table_text) > 50:
                    page_content.append(f"--- Table {i+1} ---\n{table_text}")
            
            if page_content:
                content_parts.extend(page_content)
        
        # Strategy 5: Fallback - extract all meaningful text
        if not content_parts:
            # Remove navigation, footer, and other non-content elements
            for unwanted in soup(['nav', 'footer', 'header', 'aside', 'script', 'style']):
                unwanted.decompose()
            
            body_text = soup.get_text(separator='\n', strip=True)
            if body_text and len(body_text) > 200:
                content_parts.append("--- Fallback Content Extraction ---")
                content_parts.append(body_text[:5000])  # Limit fallback content
        
        result = '\n\n'.join(content_parts)
        
        # Log the extraction strategy used
        if content_parts:
            if 'PDF Viewer Content' in result:
                self.logger.info("Used PDF viewer extraction strategy")
            elif 'AnnualReportArticle' in str(soup):
                self.logger.info("Used standard Annual Report extraction strategy")
            elif 'Fallback Content' in result:
                self.logger.info("Used fallback extraction strategy")
            else:
                self.logger.info("Used alternative layout extraction strategy")
        
        return result
    
    def extract_pdf_viewer_content(self, pdf_viewer_div) -> str:
        """Extract text content from embedded PDF viewer"""
        content_parts = []
        
        try:
            # Look for text content layers in the PDF viewer
            text_layers = pdf_viewer_div.find_all('div', class_='react-pdf__Page__textContent')
            
            for page_num, text_layer in enumerate(text_layers, 1):
                page_content = []
                
                # Extract all text spans from the page
                text_spans = text_layer.find_all('span', role='presentation')
                
                current_line = ""
                for span in text_spans:
                    text = span.get_text(strip=True)
                    if text:
                        # Check if this is a line break
                        next_sibling = span.find_next_sibling()
                        if next_sibling and next_sibling.name == 'br':
                            # End of line
                            current_line += text
                            if current_line.strip():
                                page_content.append(current_line.strip())
                            current_line = ""
                        else:
                            # Continue on same line
                            current_line += text + " "
                
                # Add any remaining line content
                if current_line.strip():
                    page_content.append(current_line.strip())
                
                if page_content:
                    content_parts.append(f"--- PDF Page {page_num} ---")
                    content_parts.extend(page_content)
            
            self.logger.info(f"Extracted content from {len(text_layers)} PDF pages in viewer")
            
        except Exception as e:
            self.logger.warning(f"Error extracting PDF viewer content: {e}")
            
            # Fallback: extract all text from the PDF viewer div
            try:
                fallback_text = pdf_viewer_div.get_text(separator='\n', strip=True)
                if fallback_text and len(fallback_text) > 100:
                    content_parts.append("--- PDF Viewer Content (Fallback) ---")
                    content_parts.append(fallback_text)
            except:
                pass
        
        return '\n'.join(content_parts)
    
    def scrape_corporate_plan_content(self, soup: BeautifulSoup) -> str:
        """Scrape Corporate Plan content"""
        content_parts = []
        
        # Extract main content areas
        content_selectors = [
            '.field__item',
            '.clearfix.text-formatted',
            '.audit-objective-summary',
            '.content'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            for elem in elements:
                text = elem.get_text(separator='\n', strip=True)
                if text and len(text) > 50:  # Only include substantial content
                    content_parts.append(text)
        
        return '\n\n'.join(content_parts)
            
    def extract_text_content(self, soup: BeautifulSoup) -> str:
        """Extract main text content from page"""
        # Remove script and style elements
        for element in soup(["script", "style", "nav", "header", "footer"]):
            element.decompose()
            
        # Look for main content areas (adjust selectors based on actual HTML)
        content_selectors = [
            '.main-content',
            '.content',
            '.publication-content',
            'main',
            '.body-content',
            'article'
        ]
        
        content_text = ""
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                content_text = content_elem.get_text(separator='\n', strip=True)
                break
                
        if not content_text:
            # Fallback to body content
            content_text = soup.get_text(separator='\n', strip=True)
            
        return content_text
        
    def scrape_paginated_content(self, soup: BeautifulSoup, base_url: str) -> str:
        """Scrape additional content from paginated pages"""
        additional_content = ""
        
        try:
            # Look for "Next" links or pagination
            next_links = soup.find_all('a', string=re.compile(r'next', re.I)) or \
                        soup.find_all('a', class_=re.compile(r'next', re.I))
            
            for next_link in next_links[:10]:  # Limit pagination depth
                if 'href' in next_link.attrs:
                    next_url = urljoin(base_url, next_link['href'])
                    
                    try:
                        self.driver.get(next_url)
                        time.sleep(2)
                        
                        next_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                        page_content = self.extract_text_content(next_soup)
                        
                        additional_content += f"\n\n--- Page Content ---\n{page_content}"
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to scrape paginated content from {next_url}: {e}")
                        break
                        
        except Exception as e:
            self.logger.warning(f"Error in pagination handling: {e}")
            
        return additional_content
        
    def find_pdf_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find all PDF links on the page"""
        pdf_links = []
        
        # Find all links that point to PDFs
        links = soup.find_all('a', href=re.compile(r'\.pdf', re.I))
        
        for link in links:
            pdf_url = urljoin(base_url, link['href'])
            pdf_links.append(pdf_url)
            
        return list(set(pdf_links))  # Remove duplicates
        
    def extract_pdf_content(self, pdf_url: str) -> Optional[str]:
        """
        Extract text content from PDF including tables with improved handling for large documents
        
        Args:
            pdf_url: URL of the PDF file
            
        Returns:
            Extracted text content
        """
        try:
            self.logger.info(f"Extracting PDF content from: {pdf_url}")
            
            # Download PDF with timeout  
            response = self.session.get(pdf_url, timeout=self.PDF_TIMEOUT)
            response.raise_for_status()
            
            # Check PDF size
            content_length = int(response.headers.get('content-length', 0))
            size_mb = content_length / (1024 * 1024)
            
            if size_mb > self.MAX_PDF_SIZE_MB:
                self.logger.warning(f"PDF too large ({size_mb:.1f}MB), skipping: {pdf_url}")
                self.stats['large_pdfs_skipped'] += 1
                return f"PDF Document ({size_mb:.1f}MB) - Too large for extraction"
            
            self.stats['pdf_size_mb_total'] += size_mb
            
            self.logger.info(f"Processing PDF ({size_mb:.1f}MB)")
            
            # Try pdfplumber first (better for tables)
            try:
                import io
                pdf_content = ""
                pages_processed = 0
                
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                    total_pages = len(pdf.pages)
                    max_pages = min(total_pages, self.MAX_PDF_PAGES)
                    
                    self.logger.info(f"PDF has {total_pages} pages, processing up to {max_pages} pages")
                    
                    # Process in larger batches for better performance
                    for batch_start in range(0, max_pages, self.BATCH_SIZE_PAGES):
                        batch_end = min(batch_start + self.BATCH_SIZE_PAGES, max_pages)
                        batch_content = []
                        
                        self.logger.info(f"Processing pages {batch_start + 1}-{batch_end}")
                        
                        for page_num in range(batch_start, batch_end):
                            try:
                                page = pdf.pages[page_num]
                                
                                # Extract text
                                text = page.extract_text()
                                if text and len(text.strip()) > 10:
                                    batch_content.append(f"\n--- Page {page_num + 1} ---\n{text}")
                                    
                                # Extract tables
                                tables = page.extract_tables()
                                for table_num, table in enumerate(tables[:5]):
                                    if table and len(table) > 1:
                                        batch_content.append(f"\n--- Table {table_num + 1} on Page {page_num + 1} ---")
                                        for row in table[:50]:
                                            if row:
                                                clean_row = [str(cell).strip() if cell else "" for cell in row]
                                                if any(clean_row):
                                                    batch_content.append(" | ".join(clean_row))
                                
                                pages_processed += 1
                                
                            except Exception as page_error:
                                self.logger.warning(f"Error processing page {page_num + 1}: {page_error}")
                                continue
                        
                        pdf_content += "\n".join(batch_content)
                        
                        import gc
                        gc.collect()
                        
                    if total_pages > max_pages:
                        pdf_content += f"\n\n--- PDF TRUNCATED: Showing {max_pages} of {total_pages} pages ---\n"
                        
                self.stats['pdf_extractions'] += 1
                self.stats['pdf_pages_processed'] += pages_processed
                self.logger.info(f"Successfully extracted {pages_processed} pages from PDF")
                return pdf_content
                
            except ImportError:
                # Fallback to PyPDF2
                self.logger.warning("pdfplumber not available, using PyPDF2 for PDF extraction.")
                return self.extract_pdf_with_pypdf2(response.content)
                    
        except Exception as e:
            self.logger.error(f"Failed to extract PDF content from {pdf_url}: {e}")
            self.stats['errors'] += 1
            return f"PDF Document - Error during extraction: {str(e)}"
    
    def extract_pdf_with_pypdf2(self, pdf_bytes):
        """Fallback PDF extraction using PyPDF2."""
        import io
        pdf_content = ""
        pages_processed = 0
        try:
            with io.BytesIO(pdf_bytes) as pdf_file:
                reader = PyPDF2.PdfReader(pdf_file)
                total_pages = len(reader.pages)
                max_pages = min(total_pages, self.MAX_PDF_PAGES)

                for page_num in range(max_pages):
                    try:
                        page = reader.pages[page_num]
                        text = page.extract_text()
                        if text and len(text.strip()) > 10:
                            pdf_content += f"\n--- Page {page_num + 1} ---\n{text}\n"
                            pages_processed += 1
                    except Exception as page_error:
                        self.logger.warning(f"PyPDF2 error on page {page_num + 1}: {page_error}")

                if total_pages > max_pages:
                    pdf_content += f"\n\n--- PDF TRUNCATED (PyPDF2): {max_pages}/{total_pages} pages ---\n"

            self.stats['pdf_extractions'] += 1
            self.stats['pdf_pages_processed'] += pages_processed
            return pdf_content
        except Exception as e:
            self.logger.error(f"PyPDF2 fallback failed: {e}")
            return None

            
    def extract_related_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract related links from the content area only"""
        related_links = []
        
        # Define content-specific areas to look for links
        content_selectors = [
            '.content',
            '#main-content',
            '.field__item',
            '.clearfix.text-formatted',
            '.audit-objective-summary',
            '.related-documents-container',
            'main article',
            '.publication-content'
        ]
        
        # URLs to exclude (navigation, footer, common pages)
        exclude_patterns = [
            '/about', '/contact', '/privacy', '/accessibility', '/copyright',
            '/feedback', '/portfolio-entities-companies', '/publications',
            'javascript:', 'mailto:', '#', 'facebook.com', 'twitter.com', 
            'linkedin.com', 'instagram.com', 'youtube.com', '/search',
            '/sitemap', '/terms', '/disclaimer'
        ]
        
        # Find content areas
        content_found = False
        for selector in content_selectors:
            content_area = soup.select_one(selector)
            if content_area:
                content_found = True
                links = content_area.find_all('a', href=True)
                
                for link in links:
                    href = link['href']
                    full_url = urljoin(base_url, href)
                    
                    # Skip if it matches exclude patterns
                    if any(pattern in full_url.lower() for pattern in exclude_patterns):
                        continue
                    
                    # Only include document-like links or relevant resources
                    link_text = link.get_text(strip=True).lower()
                    if (any(ext in full_url.lower() for ext in ['.pdf', '.doc', '.xls', '.xlsx']) or
                        any(keyword in link_text for keyword in ['report', 'plan', 'statement', 'strategy', 'document', 'download']) or
                        any(keyword in full_url.lower() for keyword in ['report', 'plan', 'statement', 'audit', 'corporate'])):
                        related_links.append(full_url)
                
                break  # Use first matching content area
        
        # If no content area found, don't extract any links to avoid navigation links
        if not content_found:
            return []
            
        return list(set(related_links))[:5]  # Limit and deduplicate
        
    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
            
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\.,;:!?\-()[\]{}"]', '', text)
        
        # Remove HTML artifacts
        text = re.sub(r'&[a-zA-Z0-9#]+;', ' ', text)
        
        return text.strip()
            
    def save_data(self, all_publications: List[Dict]):
        """FIXED: Save scraped data to JSON file with proper unique ID deduplication"""
        try:
            # Combine with existing data, removing duplicates by unique ID
            combined_data = self.existing_data.copy()
            
            # Create a set of existing unique IDs for faster lookups
            existing_ids = {item.get('unique_id') for item in combined_data if 'unique_id' in item}
            
            for pub in all_publications:
                # FIXED: Ensure the pub has content before saving and use unique ID for deduplication
                if (pub.get('unique_id') and 
                    pub.get('unique_id') not in existing_ids and 
                    pub.get('content_text')):
                    combined_data.append(pub)
                    existing_ids.add(pub.get('unique_id'))  # Track newly added IDs
                    self.stats['new_records'] += 1
                elif pub.get('unique_id') in existing_ids:
                    self.logger.info(f"Skipping duplicate record: {pub.get('title')} (ID: {pub.get('unique_id')[:8]}...)")
                    self.stats['duplicates_skipped'] += 1
                elif not pub.get('content_text'):
                    self.logger.warning(f"Skipping record with no content: {pub.get('title')}")
                    
            # Save to file
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(combined_data, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Saved {len(combined_data)} total records to {self.output_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
            
    def run(self):
        """Main scraping execution with FIXED deduplication logic"""
        start_time = datetime.now()
        self.logger.info(f"Starting scraper run at {start_time}")
        
        try:
            # --- PHASE 1: COLLECT PUBLICATION METADATA ---
            self.logger.info("--- Starting Phase 1: Collecting all publication links ---")
            
            if not self.simulate_human_navigation():
                self.logger.error("Failed to initialize navigation, aborting.")
                return

            all_publications_metadata = []
            page_num = 1
            
            while True:
                if self.max_pages and page_num > self.max_pages:
                    self.logger.info(f"Reached max pages limit: {self.max_pages}")
                    break
                
                self.logger.info(f"Collecting links from page {page_num}...")
                publications_on_page = self.get_publication_listings_from_page()
                
                if not publications_on_page:
                    self.logger.info(f"No publications found on page {page_num}. Ending link collection.")
                    break
                
                # FIXED: Filter out already scraped publications using unique IDs instead of URLs
                new_pubs = [pub for pub in publications_on_page 
                           if pub.get('unique_id') and pub.get('unique_id') not in self.scraped_unique_ids]
                skipped_count = len(publications_on_page) - len(new_pubs)
                if skipped_count > 0:
                    self.logger.info(f"Skipped {skipped_count} already processed publications on this page.")
                    self.stats['duplicates_skipped'] += skipped_count

                all_publications_metadata.extend(new_pubs)
                
                # Add unique IDs to tracking set to avoid processing later pages
                for pub in new_pubs:
                    if pub.get('unique_id'):
                        self.scraped_unique_ids.add(pub.get('unique_id'))
                
                # Find and click the 'Next' button
                try:
                    # Use a more reliable selector and wait condition
                    wait = WebDriverWait(self.driver, 10)
                    next_button_selector = (By.CSS_SELECTOR, 'a[aria-label="Next"]')
                    
                    # Wait for the element to be present in the DOM
                    next_button = wait.until(EC.presence_of_element_located(next_button_selector))
                    
                    # Scroll the element into view and click
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                    time.sleep(0.5) # Brief pause after scroll
                    self.driver.execute_script("arguments[0].click();", next_button)
                    
                    self.logger.info("Successfully clicked the 'Next' button.")
                    
                    page_num += 1
                    time.sleep(3) # Wait for the new page's content to load
                except TimeoutException:
                    self.logger.info("No more 'Next' buttons available. Link collection complete.")
                    break
                except Exception as e:
                    self.logger.error(f"An error occurred while trying to navigate to the next page: {e}")
                    break # Exit loop if we can't click next
            
            self.logger.info(f"--- Phase 1 Complete: Collected metadata for {len(all_publications_metadata)} new publications ---")

            # --- PHASE 2: SCRAPE CONTENT FOR EACH PUBLICATION ---
            self.logger.info("--- Starting Phase 2: Scraping content for each publication ---")
            
            processed_publications = []
            total_to_scrape = len(all_publications_metadata)
            if total_to_scrape == 0:
                self.logger.info("No new publications to scrape.")

            for i, pub_meta in enumerate(all_publications_metadata):
                self.logger.info(f"Processing item {i+1} of {total_to_scrape}...")
                full_pub_data = self.scrape_publication_content(pub_meta)
                processed_publications.append(full_pub_data)

            # Save all collected data
            self.save_data(processed_publications)
            
        except Exception as e:
            self.logger.error(f"Critical error during scraping: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        finally:
            if self.driver:
                self.driver.quit()
            
            # Final statistics
            end_time = datetime.now()
            duration = end_time - start_time
            self.logger.info("=== Scraping Complete ===")
            self.logger.info(f"Duration: {duration}")
            self.logger.info(f"Total new records with content: {self.stats['new_records']}")
            self.logger.info(f"Total publications processed: {self.stats['total_processed']}")
            self.logger.info(f"Duplicates skipped from previous runs: {len(self.scraped_unique_ids)}")
            self.logger.info(f"PDF extractions: {self.stats['pdf_extractions']}")
            self.logger.info(f"PDF pages processed: {self.stats['pdf_pages_processed']}")
            self.logger.info(f"Errors: {self.stats['errors']}")
                
    def cleanup(self):
        """Cleanup resources with better error handling"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Chrome driver closed successfully")
            except Exception as e:
                if "Connection refused" not in str(e):
                    self.logger.warning(f"Error during driver cleanup: {e}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Australian Government Transparency Portal Scraper')
    parser.add_argument('--max-pages', type=int, default=2, 
                       help='Maximum number of pages to scrape (default: all pages)')
    parser.add_argument('--data-dir', default='data', 
                       help='Directory to save data files (default: data)')
    parser.add_argument('--logs-dir', default='logs', 
                       help='Directory to save log files (default: logs)')
    parser.add_argument('--max-pdf-pages', type=int, default=2000,
                       help='Maximum pages to extract from each PDF (default: 2000)')
    parser.add_argument('--max-pdf-size', type=int, default=500,
                       help='Maximum PDF size in MB to process (default: 500)')
    parser.add_argument('--max-ar-sections', type=int, default=50,
                       help='Maximum sections to process per Annual Report (default: 50)')
    parser.add_argument('--no-headless', action='store_true',
                       help='Disable headless mode to watch the browser work.')
    
    args = parser.parse_args()
    
    # Create scraper instance
    scraper = TransparencyPortalScraper(
        max_pages=args.max_pages,
        data_dir=args.data_dir,
        logs_dir=args.logs_dir,
        headless=not args.no_headless
    )
    
    # Override PDF limits if specified
    scraper.MAX_PDF_PAGES = args.max_pdf_pages
    scraper.MAX_PDF_SIZE_MB = args.max_pdf_size
    scraper.MAX_ANNUAL_REPORT_SECTIONS = args.max_ar_sections
    
    try:
        scraper.run()
    except KeyboardInterrupt:
        scraper.logger.info("Scraping interrupted by user")
    except Exception as e:
        scraper.logger.error(f"Scraping failed: {e}")
        raise
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    main()