#!/usr/bin/env python3
"""
FMA NZ Speeches and Presentations Scraper
Scrapes speeches from https://www.fma.govt.nz/library/speeches-and-presentations/
with support for pagination, comprehensive PDF extraction, and deduplication.
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import re
import time
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from pathlib import Path
import hashlib
import PyPDF2
import io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from fake_useragent import UserAgent

# Configuration
BASE_URL = "https://www.fma.govt.nz"
SPEECHES_URL = f"{BASE_URL}/library/speeches-and-presentations/"
DATA_DIR = "data"
MAX_PAGES = 3  # Set to 3 for daily runs, 10+ for initial full scrape
DELAY_RANGE = (2, 5)  # Random delay between requests (seconds)
DAYS_LOOKBACK = 30  # For daily runs, only scrape speeches from last 30 days
MAX_PDF_SIZE = 50 * 1024 * 1024  # 50MB max PDF size
PDF_TIMEOUT = 60  # Timeout for PDF downloads in seconds

# File paths
SPEECHES_JSON = os.path.join(DATA_DIR, "fma_speeches.json")
SPEECHES_CSV = os.path.join(DATA_DIR, "fma_speeches.csv")
LOG_FILE = os.path.join(DATA_DIR, "speeches_scraper.log")

# Create data directory
os.makedirs(DATA_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FMASpeechesScraper:
    def __init__(self, is_daily_run=False):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.is_daily_run = is_daily_run
        self.setup_session()
        self.existing_speeches = self.load_existing_speeches()
        
        # For daily runs, calculate cutoff date
        if is_daily_run:
            self.cutoff_date = datetime.now() - timedelta(days=DAYS_LOOKBACK)
            logger.info(f"Daily run mode: Only scraping speeches from {self.cutoff_date.strftime('%Y-%m-%d')} onwards")
        
    def setup_session(self):
        """Configure session with retry strategy and realistic headers"""
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set realistic headers
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': BASE_URL,
        })
    
    def random_delay(self):
        """Random delay to avoid being detected as bot"""
        delay = random.uniform(*DELAY_RANGE)
        time.sleep(delay)
    
    def establish_session(self):
        """Visit main page to establish session and collect cookies"""
        try:
            logger.info("Establishing session with FMA website...")
            response = self.session.get(BASE_URL)
            response.raise_for_status()
            
            # Visit speeches page to collect more cookies
            self.random_delay()
            response = self.session.get(SPEECHES_URL)
            response.raise_for_status()
            
            logger.info("Session established successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to establish session: {str(e)}")
            return False
    
    def load_existing_speeches(self):
        """Load existing speeches for deduplication"""
        existing = {}
        if os.path.exists(SPEECHES_JSON):
            try:
                with open(SPEECHES_JSON, 'r', encoding='utf-8') as f:
                    speeches = json.load(f)
                    for speech in speeches:
                        if 'url' in speech:
                            existing[speech['url']] = speech
                logger.info(f"Loaded {len(existing)} existing speeches")
            except Exception as e:
                logger.error(f"Error loading existing speeches: {str(e)}")
        return existing
    
    def get_speech_links(self, page_url):
        """Extract speech links from a page using enhanced selectors from provided script"""
        try:
            self.random_delay()
            response = self.session.get(page_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find speech links using strategies from the provided script
            speech_links = []
            
            # Method 1: More robust speech selection from provided script
            speeches_on_page = (
                soup.select('ol#results > li article') or
                soup.select('.results article') or
                soup.select('.speeches article') or
                soup.select('article') or
                soup.select('.speech-item') or
                soup.select('.content-item')
            )
            
            for speech in speeches_on_page:
                # More robust link selection for speeches from provided script
                link_tag = (
                    speech.select_one('h3.results-list__result-title > a') or
                    speech.select_one('h3 > a') or
                    speech.select_one('h2 > a') or
                    speech.select_one('.speech-title a') or
                    speech.select_one('a[href*="/speeches-and-presentations/"]') or
                    speech.select_one('a[href*="/library/"]') or
                    speech.select_one('a')
                )
                
                if link_tag and link_tag.has_attr('href'):
                    href = link_tag['href']
                    if href and '/library/speeches-and-presentations/' in href and href != '/library/speeches-and-presentations/':
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in [sl['url'] for sl in speech_links]:
                            speech_links.append({
                                'url': full_url,
                                'title': link_tag.get_text(strip=True) or 'No title'
                            })
            
            # Method 2: General fallback search for speech links
            if not speech_links:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href')
                    if href and '/library/speeches-and-presentations/' in href and href != '/library/speeches-and-presentations/':
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in [sl['url'] for sl in speech_links]:
                            speech_links.append({
                                'url': full_url,
                                'title': link.get_text(strip=True) or 'No title'
                            })
            
            logger.info(f"Found {len(speech_links)} speech links on page: {page_url}")
            return speech_links
            
        except Exception as e:
            logger.error(f"Error getting speech links from {page_url}: {str(e)}")
            return []
    
    def extract_pdf_text_advanced(self, pdf_url):
        """Extract text from PDF with advanced cleaning for LLM consumption"""
        try:
            self.random_delay()
            
            # Set longer timeout for PDFs
            response = self.session.get(pdf_url, timeout=PDF_TIMEOUT)
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and 'application/octet-stream' not in content_type:
                logger.warning(f"Unexpected content type for PDF: {content_type}")
            
            # Check content length
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > MAX_PDF_SIZE:
                raise Exception(f"PDF too large: {content_length} bytes")
            
            pdf_bytes = response.content
            
            # Verify we got some content
            if len(pdf_bytes) < 100:  # PDFs should be at least 100 bytes
                raise Exception("PDF appears to be empty or corrupted")
            
            logger.info(f"Downloaded PDF: {len(pdf_bytes)} bytes")
            
            # Extract text using PyPDF2
            pdf_file = io.BytesIO(pdf_bytes)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            page_count = len(pdf_reader.pages)
            
            if page_count == 0:
                return "Error: PDF contains no pages."
            
            logger.info(f"Processing PDF with {page_count} pages")
            
            for page_num in range(page_count):
                try:
                    page = pdf_reader.pages[page_num]
                    page_text = page.extract_text()
                    if page_text and page_text.strip():
                        text += f"\n\n--- Page {page_num + 1} ---\n\n"
                        text += page_text
                except Exception as e:
                    logger.warning(f"Error extracting text from page {page_num}: {e}")
                    continue
            
            if not text:
                return "Error: No text could be extracted from PDF."
            
            # Advanced text cleaning for LLM friendliness
            text = self.clean_text_for_llm(text)
            
            # Limit text length to prevent memory issues
            max_text_length = 1000000  # 1MB of text
            if len(text) > max_text_length:
                text = text[:max_text_length] + "... [Text truncated due to length]"
                logger.info(f"Text truncated to {max_text_length} characters")
            
            logger.info(f"Extracted {len(text)} characters from PDF: {pdf_url}")
            return text
            
        except Exception as e:
            logger.error(f"Error extracting PDF text from {pdf_url}: {str(e)}")
            return f"Error: Could not extract text from PDF - {str(e)}"
    
    def clean_text_for_llm(self, text):
        """Clean and format text to be LLM-friendly"""
        if not text:
            return ""
        
        # Remove excessive whitespace but preserve structure
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)  # Max 2 consecutive newlines
        text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces/tabs to single space
        
        # Fix common PDF extraction issues
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)  # Add space between words stuck together
        text = re.sub(r'(\w)(\d)', r'\1 \2', text)  # Space between word and number
        text = re.sub(r'(\d)([A-Za-z])', r'\1 \2', text)  # Space between number and word
        
        # Clean up special characters but keep meaningful punctuation
        text = re.sub(r'[^\w\s\.\,\;\:\!\?\-\(\)\[\]\"\'\$\%\&\@\#\/\\]', ' ', text)
        
        # Fix bullet points and lists
        text = re.sub(r'^\s*[•·▪▫‣⁃]\s*', '• ', text, flags=re.MULTILINE)
        text = re.sub(r'^\s*[\-\*]\s*', '• ', text, flags=re.MULTILINE)
        
        # Normalize quotes using Unicode escape sequences
        text = re.sub(r'[\u201c\u201d\u201e]', '"', text)  # Smart double quotes
        text = re.sub(r'[\u2018\u2019\u201a]', "'", text)  # Smart single quotes
        
        # Remove page headers/footers patterns
        text = re.sub(r'\n\s*Page \d+.*?\n', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'\n\s*\d+\s*\n', '\n', text)  # Standalone page numbers
        
        # Remove FMA standard footers/headers
        text = re.sub(r'\n\s*Financial Markets Authority.*?\n', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'\n\s*www\.fma\.govt\.nz.*?\n', '\n', text, flags=re.IGNORECASE)
        
        # Final cleanup
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Remove excessive line breaks
        text = text.strip()
        
        return text
    
    def extract_all_pdfs_from_page(self, soup):
        """Extract content from ALL PDF files found on the page (ignore Excel/CSV)"""
        pdf_content = {}
        pdf_links = []
        
        # Find all PDF links (exclude Excel/CSV files)
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href and href.lower().endswith('.pdf'):
                # Skip if it's an Excel or CSV file
                if not any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv']):
                    pdf_url = urljoin(BASE_URL, href)
                    pdf_links.append(pdf_url)
        
        # Extract content from each PDF
        for i, pdf_url in enumerate(pdf_links):
            content = self.extract_pdf_text_advanced(pdf_url)
            if content and not content.startswith('Error:'):
                pdf_content[f"pdf_{i+1}"] = {
                    "url": pdf_url,
                    "content": content,
                    "filename": os.path.basename(urlparse(pdf_url).path)
                }
        
        return pdf_content, pdf_links
    
    def parse_speech_date(self, date_text):
        """Parse various date formats found in speeches"""
        if not date_text:
            return None
        
        # Common date patterns
        date_patterns = [
            r'(\d{1,2})\s+(\w+)\s+(\d{4})',  # "15 March 2024"
            r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})',  # "15/03/2024" or "15-03-24"
            r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',  # "March 15, 2024"
            r'(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})',  # "2024/03/15"
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_text)
            if match:
                try:
                    if pattern == date_patterns[0]:  # "15 March 2024"
                        day, month_name, year = match.groups()
                        parsed_date = datetime.strptime(f"{day} {month_name} {year}", "%d %B %Y")
                    elif pattern == date_patterns[2]:  # "March 15, 2024"
                        month_name, day, year = match.groups()
                        parsed_date = datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y")
                    else:
                        # For numeric patterns
                        groups = match.groups()
                        if len(groups[0]) == 4:  # Year first
                            year, month, day = groups
                        else:  # Day first
                            day, month, year = groups
                        if len(year) == 2:
                            year = "20" + year
                        parsed_date = datetime.strptime(f"{day}/{month}/{year}", "%d/%m/%Y")
                    
                    return parsed_date
                except ValueError:
                    continue
        
        return None
    
    def extract_speech_metadata(self, soup):
        """Extract speech-specific metadata using enhanced selectors from provided script"""
        metadata = {}
        
        # Enhanced date selectors for speeches from provided script
        date_element = (
            soup.select_one('span.published__text') or
            soup.select_one('.published') or
            soup.select_one('.speech-date') or
            soup.select_one('[class*="date"]') or
            soup.select_one('time')
        )
        
        publication_date = None
        if date_element:
            date_text = date_element.get_text(strip=True)
            publication_date = self.parse_speech_date(date_text)
            if publication_date:
                metadata['publication_date'] = publication_date.strftime('%Y-%m-%d')
        
        # Extract speaker information from provided script
        speaker_element = (
            soup.select_one('.speaker') or
            soup.select_one('.author') or
            soup.select_one('.presenter') or
            soup.select_one('[class*="speaker"]')
        )
        
        if speaker_element:
            metadata['speaker'] = speaker_element.get_text(strip=True)
        else:
            # Try to extract speaker from title or content
            title_text = soup.find('h1')
            if title_text:
                title_content = title_text.get_text()
                # Look for common speaker patterns in title
                speaker_patterns = [
                    r'by\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                    r'([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+FMA',
                ]
                for pattern in speaker_patterns:
                    match = re.search(pattern, title_content)
                    if match:
                        metadata['speaker'] = match.group(1)
                        break
        
        # Extract event or venue information from provided script
        event_element = (
            soup.select_one('.event') or
            soup.select_one('.venue') or
            soup.select_one('.location') or
            soup.select_one('[class*="event"]')
        )
        
        if event_element:
            metadata['event'] = event_element.get_text(strip=True)
        
        # Extract tags or categories from provided script
        tags_elements = soup.select('.tags a, .categories a, [class*="tag"] a')
        if tags_elements:
            metadata['tags'] = [tag.get_text(strip=True) for tag in tags_elements]
        else:
            metadata['tags'] = []
        
        return metadata, publication_date
    
    def extract_links_from_content(self, soup):
        """Extract all relevant links from speech content"""
        links = []
        main_content = soup.find('main') or soup
        
        for link in main_content.find_all('a', href=True):
            href = link.get('href')
            if href:
                full_url = urljoin(BASE_URL, href)
                link_text = link.get_text(strip=True)
                # Exclude file downloads and empty links
                if (link_text and 
                    not href.lower().endswith(('.pdf', '.xlsx', '.csv', '.mp3', '.mp4', '.wav')) and
                    len(link_text) > 2):
                    links.append({
                        'url': full_url,
                        'text': link_text
                    })
        
        return links
    
    def should_skip_for_daily_run(self, publication_date):
        """Check if speech should be skipped for daily run based on date"""
        if not self.is_daily_run or not publication_date:
            return False
        
        return publication_date < self.cutoff_date
    
    def scrape_speech(self, speech_url):
        """Scrape individual speech using enhanced selectors from provided script"""
        try:
            # Check if already scraped
            if speech_url in self.existing_speeches:
                logger.info(f"Speech already exists: {speech_url}")
                return self.existing_speeches[speech_url]
            
            logger.info(f"Scraping speech: {speech_url}")
            self.random_delay()
            
            response = self.session.get(speech_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Enhanced selectors with fallbacks for speeches from provided script
            title_element = (
                soup.select_one('h1.registry-item-page__heading-wrap--title-item') or
                soup.select_one('h1.speech-title') or
                soup.select_one('h1') or
                soup.select_one('.title')
            )
            title = title_element.get_text(strip=True) if title_element else "Title not found"
            
            # Extract meta description
            description = ""
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                description = meta_desc.get('content', '')
            
            # Extract speech metadata
            metadata, publication_date = self.extract_speech_metadata(soup)
            
            # Skip if this is a daily run and speech is too old
            if self.should_skip_for_daily_run(publication_date):
                logger.info(f"Skipping old speech for daily run: {title}")
                return None
            
            # Enhanced content selectors for speeches from provided script
            content_element = (
                soup.select_one('div.registry-item-page__body-wrap-main--elemental') or
                soup.select_one('.speech-content') or
                soup.select_one('.content') or
                soup.select_one('main') or
                soup.select_one('.body')
            )
            content_text = content_element.get_text(strip=True, separator='\n') if content_element else "Content not found"
            
            # Clean content for LLM
            content_text = self.clean_text_for_llm(content_text)
            
            # Extract category/breadcrumb information
            category = ""
            breadcrumbs = soup.find('nav', class_='breadcrumbs') or soup.find('ol', class_='breadcrumb')
            if breadcrumbs:
                category = breadcrumbs.get_text(separator=' > ', strip=True)
            
            # Extract image
            image_url = ""
            img = soup.find('img')
            if img and img.get('src'):
                image_url = urljoin(BASE_URL, img.get('src'))
            
            # Extract related links (excluding files)
            related_links = self.extract_links_from_content(soup)
            
            # Extract ALL PDF content (ignore Excel/CSV)
            pdf_content, pdf_links = self.extract_all_pdfs_from_page(soup)
            
            # Combine all content for LLM analysis
            full_content = content_text
            
            if pdf_content:
                full_content += "\n\n=== SPEECH DOCUMENTS ===\n\n"
                for pdf_key, pdf_data in pdf_content.items():
                    full_content += f"\n--- {pdf_data['filename']} ---\n\n"
                    full_content += pdf_data['content']
            
            # Final LLM-friendly cleaning
            full_content = self.clean_text_for_llm(full_content)
            
            # Generate unique ID
            speech_id = hashlib.md5(speech_url.encode()).hexdigest()
            
            speech_data = {
                'id': speech_id,
                'url': speech_url,
                'title': title,
                'description': description,
                'category': category,
                'publication_date': metadata.get('publication_date', ''),
                'speaker': metadata.get('speaker', ''),
                'event': metadata.get('event', ''),
                'tags': metadata.get('tags', []),
                'scraped_date': datetime.now().isoformat(),
                'content': full_content,
                'content_text': content_text,
                'pdf_content': pdf_content,
                'image_url': image_url,
                'related_links': related_links,
                'pdf_links': pdf_links,
                'content_length': len(full_content),
                'pdf_count': len(pdf_content)
            }
            
            logger.info(f"Successfully scraped speech: {title[:100]}... (PDFs: {len(pdf_content)})")
            return speech_data
            
        except Exception as e:
            logger.error(f"Error scraping speech {speech_url}: {str(e)}")
            return None
    
    def save_data(self, speeches):
        """Save speeches to JSON and CSV files"""
        try:
            # Save JSON
            with open(SPEECHES_JSON, 'w', encoding='utf-8') as f:
                json.dump(speeches, f, indent=2, ensure_ascii=False)
            
            # Save CSV
            if speeches:
                fieldnames = [
                    'id', 'url', 'title', 'description', 'category', 'publication_date',
                    'speaker', 'event', 'tags', 'scraped_date', 'content',
                    'content_text', 'image_url', 'related_links', 'pdf_links',
                    'content_length', 'pdf_count'
                ]
                
                with open(SPEECHES_CSV, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for speech in speeches:
                        # Convert complex data to strings for CSV
                        csv_speech = speech.copy()
                        csv_speech['tags'] = json.dumps(speech.get('tags', []))
                        csv_speech['related_links'] = json.dumps(speech.get('related_links', []))
                        csv_speech['pdf_links'] = json.dumps(speech.get('pdf_links', []))
                        # Remove complex nested data from CSV
                        csv_speech.pop('pdf_content', None)
                        writer.writerow(csv_speech)
            
            logger.info(f"Saved {len(speeches)} speeches to {SPEECHES_JSON} and {SPEECHES_CSV}")
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
    
    def run(self):
        """Main scraping function"""
        run_type = "daily" if self.is_daily_run else "full"
        max_pages = 3 if self.is_daily_run else MAX_PAGES
        
        logger.info(f"Starting FMA speeches and presentations scraper ({run_type} run)...")
        
        if not self.establish_session():
            logger.error("Failed to establish session. Exiting.")
            return
        
        all_speeches = list(self.existing_speeches.values())
        new_speeches_count = 0
        
        try:
            # Start with first page
            current_page = 1
            page_url = SPEECHES_URL
            consecutive_empty_pages = 0
            
            while current_page <= max_pages:
                logger.info(f"Scraping page {current_page}: {page_url}")
                
                # Get speech links from current page
                speech_links = self.get_speech_links(page_url)
                
                if not speech_links:
                    consecutive_empty_pages += 1
                    logger.warning(f"No speech links found on page {current_page} (consecutive empty: {consecutive_empty_pages})")
                    
                    if consecutive_empty_pages >= 3:
                        logger.info("Found 3 consecutive empty pages. Ending pagination.")
                        break
                else:
                    consecutive_empty_pages = 0
                
                page_has_new_items = False
                speeches_processed_this_page = 0
                
                # Scrape each speech
                for link_info in speech_links:
                    speech_url = link_info['url']
                    
                    # Skip if already exists
                    if speech_url in self.existing_speeches:
                        continue
                    
                    page_has_new_items = True
                    speeches_processed_this_page += 1
                    
                    speech_data = self.scrape_speech(speech_url)
                    if speech_data:
                        all_speeches.append(speech_data)
                        new_speeches_count += 1
                        
                        # Save periodically
                        if new_speeches_count % 3 == 0:
                            self.save_data(all_speeches)
                
                logger.info(f"Processed {speeches_processed_this_page} new speeches on page {current_page}")
                
                # Check if we should continue pagination
                if not page_has_new_items and len(self.existing_speeches) > 0:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 2:
                        logger.info("Found multiple pages with no new speeches. Stopping pagination.")
                        break
                
                # Look for next page
                self.random_delay()
                response = self.session.get(page_url)
                soup = BeautifulSoup(response.content, 'html.parser')
                
                next_link = soup.find('a', class_='next')
                if next_link and next_link.get('href') and not next_link.has_attr('disabled'):
                    page_url = urljoin(BASE_URL, next_link.get('href'))
                    current_page += 1
                else:
                    logger.info("No next page found or next button is disabled.")
                    break
            
            # Final save
            self.save_data(all_speeches)
            
            # Log speaker and event statistics
            speakers_found_count = len([item for item in all_speeches if item.get('speaker')])
            events_found_count = len([item for item in all_speeches if item.get('event')])
            pdf_found_count = len([item for item in all_speeches if item.get('pdf_links')])
            pdf_extracted_count = len([item for item in all_speeches if item.get('pdf_count', 0) > 0])
            
            logger.info(f"Scraping completed ({run_type} run). Total speeches: {len(all_speeches)}, New speeches: {new_speeches_count}")
            logger.info(f"Speaker information found for {speakers_found_count} speeches")
            logger.info(f"Event information found for {events_found_count} speeches")
            logger.info(f"PDF Statistics: Found {pdf_found_count} speeches with PDF links")
            logger.info(f"PDF Statistics: Successfully extracted text from {pdf_extracted_count} speeches")
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            # Save whatever we have
            self.save_data(all_speeches)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='FMA Speeches and Presentations Scraper')
    parser.add_argument('--daily', action='store_true', 
                       help='Run in daily mode (only scrape recent speeches)')
    
    args = parser.parse_args()
    
    scraper = FMASpeechesScraper(is_daily_run=args.daily)
    scraper.run()

if __name__ == "__main__":
    main()