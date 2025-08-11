#!/usr/bin/env python3
"""
FMA NZ Opinions Scraper
Scrapes opinion pieces from https://www.fma.govt.nz/library/opinion/
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
OPINIONS_URL = f"{BASE_URL}/library/opinion/"
DATA_DIR = "data"
MAX_PAGES = 3  # Set to 3 for daily runs, 10+ for initial full scrape
DELAY_RANGE = (2, 5)  # Random delay between requests (seconds)
DAYS_LOOKBACK = 30  # For daily runs, only scrape opinions from last 30 days (opinions are less frequent)

# File paths
OPINIONS_JSON = os.path.join(DATA_DIR, "fma_opinions.json")
OPINIONS_CSV = os.path.join(DATA_DIR, "fma_opinions.csv")
LOG_FILE = os.path.join(DATA_DIR, "opinions_scraper.log")

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

class FMAOpinionsScraper:
    def __init__(self, is_daily_run=False):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.is_daily_run = is_daily_run
        self.setup_session()
        self.existing_opinions = self.load_existing_opinions()
        
        # For daily runs, calculate cutoff date
        if is_daily_run:
            self.cutoff_date = datetime.now() - timedelta(days=DAYS_LOOKBACK)
            logger.info(f"Daily run mode: Only scraping opinions from {self.cutoff_date.strftime('%Y-%m-%d')} onwards")
        
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
            
            # Visit opinions page to collect more cookies
            self.random_delay()
            response = self.session.get(OPINIONS_URL)
            response.raise_for_status()
            
            logger.info("Session established successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to establish session: {str(e)}")
            return False
    
    def load_existing_opinions(self):
        """Load existing opinions for deduplication"""
        existing = {}
        if os.path.exists(OPINIONS_JSON):
            try:
                with open(OPINIONS_JSON, 'r', encoding='utf-8') as f:
                    opinions = json.load(f)
                    for opinion in opinions:
                        if 'url' in opinion:
                            existing[opinion['url']] = opinion
                logger.info(f"Loaded {len(existing)} existing opinions")
            except Exception as e:
                logger.error(f"Error loading existing opinions: {str(e)}")
        return existing
    
    def get_opinion_links(self, page_url):
        """Extract opinion links from a page"""
        try:
            self.random_delay()
            response = self.session.get(page_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find opinion links using multiple selectors
            opinion_links = []
            
            # Method 1: Look for links in H3 tags (primary method)
            h3_links = soup.find_all('h3')
            for h3 in h3_links:
                link = h3.find('a', href=True)
                if link:
                    href = link.get('href')
                    if href and '/library/opinion/' in href and href != '/library/opinion/':
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in [ol['url'] for ol in opinion_links]:
                            opinion_links.append({
                                'url': full_url,
                                'title': link.get_text(strip=True) or 'No title'
                            })
            
            # Method 2: Look for article links as backup
            article_sections = soup.find_all('article')
            for article in article_sections:
                links = article.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    if href and '/library/opinion/' in href and href != '/library/opinion/':
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in [ol['url'] for ol in opinion_links]:
                            opinion_links.append({
                                'url': full_url,
                                'title': link.get_text(strip=True) or 'No title'
                            })
            
            # Method 3: General search for opinion links
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href and '/library/opinion/' in href and href != '/library/opinion/':
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in [ol['url'] for ol in opinion_links]:
                        opinion_links.append({
                            'url': full_url,
                            'title': link.get_text(strip=True) or 'No title'
                        })
            
            logger.info(f"Found {len(opinion_links)} opinion links on page: {page_url}")
            return opinion_links
            
        except Exception as e:
            logger.error(f"Error getting opinion links from {page_url}: {str(e)}")
            return []
    
    def extract_pdf_text_advanced(self, pdf_url):
        """Extract text from PDF with advanced cleaning for LLM consumption"""
        try:
            self.random_delay()
            response = self.session.get(pdf_url)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page_num, page in enumerate(pdf_reader.pages):
                page_text = page.extract_text()
                if page_text:
                    # Add page separator for better structure
                    text += f"\n\n--- Page {page_num + 1} ---\n\n"
                    text += page_text
            
            # Advanced text cleaning for LLM friendliness
            text = self.clean_text_for_llm(text)
            
            logger.info(f"Extracted {len(text)} characters from PDF: {pdf_url}")
            return text
            
        except Exception as e:
            logger.error(f"Error extracting PDF text from {pdf_url}: {str(e)}")
            return ""
    
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
                # Skip if it's an Excel or CSV file (sometimes have .pdf in query params)
                if not any(ext in href.lower() for ext in ['.xlsx', '.xls', '.csv']):
                    pdf_url = urljoin(BASE_URL, href)
                    pdf_links.append(pdf_url)
        
        # Extract content from each PDF
        for i, pdf_url in enumerate(pdf_links):
            content = self.extract_pdf_text_advanced(pdf_url)
            if content:
                pdf_content[f"pdf_{i+1}"] = {
                    "url": pdf_url,
                    "content": content,
                    "filename": os.path.basename(urlparse(pdf_url).path)
                }
        
        return pdf_content, pdf_links
    
    def parse_opinion_date(self, date_text):
        """Parse various date formats found in opinions"""
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
                    # Try to parse the date
                    if pattern == date_patterns[0]:  # "15 March 2024"
                        day, month_name, year = match.groups()
                        parsed_date = datetime.strptime(f"{day} {month_name} {year}", "%d %B %Y")
                    elif pattern == date_patterns[2]:  # "March 15, 2024"
                        month_name, day, year = match.groups()
                        parsed_date = datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y")
                    else:
                        # For numeric patterns, assume first group is day/year
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
    
    def extract_opinion_metadata(self, soup):
        """Extract opinion-specific metadata"""
        metadata = {}
        
        # Look for publication date with multiple strategies
        date_elements = [
            soup.find('time'),
            soup.find('span', class_=re.compile(r'date|published', re.I)),
            soup.find('div', class_=re.compile(r'date|published', re.I)),
            soup.find('p', class_=re.compile(r'date|published', re.I)),
        ]
        
        # Also look for date in text content
        page_text = soup.get_text()
        date_in_text = re.search(r'\b\d{1,2}\s+\w+\s+\d{4}\b', page_text)
        
        publication_date = None
        for date_elem in date_elements:
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                publication_date = self.parse_opinion_date(date_text)
                if publication_date:
                    metadata['publication_date'] = publication_date.strftime('%Y-%m-%d')
                    break
        
        # If no date found in elements, try text content
        if not publication_date and date_in_text:
            publication_date = self.parse_opinion_date(date_in_text.group())
            if publication_date:
                metadata['publication_date'] = publication_date.strftime('%Y-%m-%d')
        
        # Extract author information
        author_patterns = [
            soup.find('span', class_=re.compile(r'author', re.I)),
            soup.find('div', class_=re.compile(r'author|byline', re.I)),
            soup.find('p', class_=re.compile(r'author|byline', re.I)),
        ]
        
        for author_elem in author_patterns:
            if author_elem:
                author_text = author_elem.get_text(strip=True)
                if author_text and len(author_text) < 100:  # Reasonable author length
                    metadata['author'] = author_text
                    break
        
        # Look for opinion category/topic
        topic_keywords = ['fintech', 'innovation', 'regulatory', 'compliance', 'market', 'licensing']
        page_text_lower = page_text.lower()
        
        for topic in topic_keywords:
            if topic in page_text_lower:
                metadata['topic'] = topic.title()
                break
        
        # Check if this is a policy opinion or industry update
        policy_indicators = ['policy', 'regulation', 'legislation', 'framework', 'guidance']
        for indicator in policy_indicators:
            if indicator in page_text_lower:
                metadata['opinion_type'] = 'Policy Opinion'
                break
        else:
            metadata['opinion_type'] = 'Industry Update'
        
        return metadata, publication_date
    
    def extract_links_from_content(self, soup):
        """Extract all relevant links from opinion content"""
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
        """Check if opinion should be skipped for daily run based on date"""
        if not self.is_daily_run or not publication_date:
            return False
        
        return publication_date < self.cutoff_date
    
    def scrape_opinion(self, opinion_url):
        """Scrape individual opinion piece"""
        try:
            # Check if already scraped
            if opinion_url in self.existing_opinions:
                logger.info(f"Opinion already exists: {opinion_url}")
                return self.existing_opinions[opinion_url]
            
            logger.info(f"Scraping opinion: {opinion_url}")
            self.random_delay()
            
            response = self.session.get(opinion_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract basic information
            title = ""
            if soup.find('h1'):
                title = soup.find('h1').get_text(strip=True)
            
            # Extract meta description
            description = ""
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                description = meta_desc.get('content', '')
            
            # Extract opinion metadata
            metadata, publication_date = self.extract_opinion_metadata(soup)
            
            # Skip if this is a daily run and opinion is too old
            if self.should_skip_for_daily_run(publication_date):
                logger.info(f"Skipping old opinion for daily run: {title}")
                return None
            
            # Extract main content
            content = ""
            main_content = soup.find('main')
            if main_content:
                # Remove script, style, and navigation elements
                for element in main_content(['script', 'style', 'nav', 'header', 'footer']):
                    element.decompose()
                content = main_content.get_text(separator=' ', strip=True)
            
            # Clean content for LLM
            content = self.clean_text_for_llm(content)
            
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
            full_content = content
            
            if pdf_content:
                full_content += "\n\n=== SUPPORTING DOCUMENTS ===\n\n"
                for pdf_key, pdf_data in pdf_content.items():
                    full_content += f"\n--- {pdf_data['filename']} ---\n\n"
                    full_content += pdf_data['content']
            
            # Final LLM-friendly cleaning
            full_content = self.clean_text_for_llm(full_content)
            
            # Generate unique ID
            opinion_id = hashlib.md5(opinion_url.encode()).hexdigest()
            
            opinion_data = {
                'id': opinion_id,
                'url': opinion_url,
                'title': title,
                'description': description,
                'category': category,
                'publication_date': metadata.get('publication_date', ''),
                'author': metadata.get('author', ''),
                'topic': metadata.get('topic', ''),
                'opinion_type': metadata.get('opinion_type', 'Opinion'),
                'scraped_date': datetime.now().isoformat(),
                'content': full_content,
                'html_content': content,
                'pdf_content': pdf_content,
                'image_url': image_url,
                'related_links': related_links,
                'pdf_links': pdf_links,
                'content_length': len(full_content),
                'pdf_count': len(pdf_content)
            }
            
            logger.info(f"Successfully scraped opinion: {title} (PDFs: {len(pdf_content)})")
            return opinion_data
            
        except Exception as e:
            logger.error(f"Error scraping opinion {opinion_url}: {str(e)}")
            return None
    
    def save_data(self, opinions):
        """Save opinions to JSON and CSV files"""
        try:
            # Save JSON
            with open(OPINIONS_JSON, 'w', encoding='utf-8') as f:
                json.dump(opinions, f, indent=2, ensure_ascii=False)
            
            # Save CSV
            if opinions:
                fieldnames = [
                    'id', 'url', 'title', 'description', 'category', 'publication_date',
                    'author', 'topic', 'opinion_type', 'scraped_date', 'content',
                    'html_content', 'image_url', 'related_links', 'pdf_links',
                    'content_length', 'pdf_count'
                ]
                
                with open(OPINIONS_CSV, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for opinion in opinions:
                        # Convert complex data to strings for CSV
                        csv_opinion = opinion.copy()
                        csv_opinion['related_links'] = json.dumps(opinion.get('related_links', []))
                        csv_opinion['pdf_links'] = json.dumps(opinion.get('pdf_links', []))
                        # Remove complex nested data from CSV
                        csv_opinion.pop('pdf_content', None)
                        writer.writerow(csv_opinion)
            
            logger.info(f"Saved {len(opinions)} opinions to {OPINIONS_JSON} and {OPINIONS_CSV}")
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
    
    def run(self):
        """Main scraping function"""
        run_type = "daily" if self.is_daily_run else "full"
        max_pages = 3 if self.is_daily_run else MAX_PAGES
        
        logger.info(f"Starting FMA opinions scraper ({run_type} run)...")
        
        if not self.establish_session():
            logger.error("Failed to establish session. Exiting.")
            return
        
        all_opinions = list(self.existing_opinions.values())
        new_opinions_count = 0
        
        try:
            # Start with first page
            current_page = 1
            page_url = OPINIONS_URL
            
            while current_page <= max_pages:
                logger.info(f"Scraping page {current_page}: {page_url}")
                
                # Get opinion links from current page
                opinion_links = self.get_opinion_links(page_url)
                
                if not opinion_links:
                    logger.info("No more opinions found.")
                    break
                
                # Scrape each opinion
                for link_info in opinion_links:
                    opinion_url = link_info['url']
                    
                    # Skip if already exists
                    if opinion_url in self.existing_opinions:
                        continue
                    
                    opinion_data = self.scrape_opinion(opinion_url)
                    if opinion_data:
                        all_opinions.append(opinion_data)
                        new_opinions_count += 1
                        
                        # Save periodically
                        if new_opinions_count % 2 == 0:  # Save frequently for opinions
                            self.save_data(all_opinions)
                
                # Get next page URL
                if current_page < max_pages:
                    self.random_delay()
                    response = self.session.get(page_url)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for next page
                    next_link = soup.find('a', class_='next')
                    if next_link and next_link.get('href'):
                        page_url = urljoin(BASE_URL, next_link.get('href'))
                        current_page += 1
                    else:
                        logger.info("No next page found.")
                        break
                else:
                    break
            
            # Final save
            self.save_data(all_opinions)
            
            logger.info(f"Scraping completed ({run_type} run). Total opinions: {len(all_opinions)}, New opinions: {new_opinions_count}")
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            # Save whatever we have
            self.save_data(all_opinions)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='FMA Opinions Scraper')
    parser.add_argument('--daily', action='store_true', 
                       help='Run in daily mode (only scrape recent opinions)')
    
    args = parser.parse_args()
    
    scraper = FMAOpinionsScraper(is_daily_run=args.daily)
    scraper.run()

if __name__ == "__main__":
    main()