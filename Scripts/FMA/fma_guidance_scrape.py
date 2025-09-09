#!/usr/bin/env python3
"""
FMA NZ Guidance Library Scraper
Scrapes guidance documents from https://www.fma.govt.nz/library/guidance-library/
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
from datetime import datetime
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
GUIDANCE_URL = f"{BASE_URL}/library/guidance-library/"
DATA_DIR = "data"
MAX_PAGES = 1  # Set to 3 for daily runs, 10+ for initial full scrape
DELAY_RANGE = (2, 5)  # Random delay between requests (seconds)

# File paths
GUIDANCE_JSON = os.path.join(DATA_DIR, "fma_guidance.json")
GUIDANCE_CSV = os.path.join(DATA_DIR, "fma_guidance.csv")
LOG_FILE = os.path.join(DATA_DIR, "guidance_scraper.log")

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

class FMAGuidanceScraper:
    def __init__(self):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.setup_session()
        self.existing_guidance = self.load_existing_guidance()
        
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
            
            # Visit guidance page to collect more cookies
            self.random_delay()
            response = self.session.get(GUIDANCE_URL)
            response.raise_for_status()
            
            logger.info("Session established successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to establish session: {str(e)}")
            return False
    
    def load_existing_guidance(self):
        """Load existing guidance documents for deduplication"""
        existing = {}
        if os.path.exists(GUIDANCE_JSON):
            try:
                with open(GUIDANCE_JSON, 'r', encoding='utf-8') as f:
                    guidance_docs = json.load(f)
                    for doc in guidance_docs:
                        if 'url' in doc:
                            existing[doc['url']] = doc
                logger.info(f"Loaded {len(existing)} existing guidance documents")
            except Exception as e:
                logger.error(f"Error loading existing guidance: {str(e)}")
        return existing
    
    def get_guidance_links(self, page_url):
        """Extract guidance document links from a page"""
        try:
            self.random_delay()
            response = self.session.get(page_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find guidance links using multiple selectors
            guidance_links = []
            
            # Method 1: Look for links in H3 tags (recommended from analysis)
            h3_links = soup.find_all('h3')
            for h3 in h3_links:
                link = h3.find('a', href=True)
                if link:
                    href = link.get('href')
                    if href and '/library/guidance-library/' in href and href != '/library/guidance-library/':
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in [gl['url'] for gl in guidance_links]:
                            guidance_links.append({
                                'url': full_url,
                                'title': link.get_text(strip=True) or 'No title'
                            })
            
            # Method 2: Look for article links as backup
            article_sections = soup.find_all('article')
            for article in article_sections:
                links = article.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    if href and '/library/guidance-library/' in href and href != '/library/guidance-library/':
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in [gl['url'] for gl in guidance_links]:
                            guidance_links.append({
                                'url': full_url,
                                'title': link.get_text(strip=True) or 'No title'
                            })
            
            # Method 3: General search for guidance links
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href and '/library/guidance-library/' in href and href != '/library/guidance-library/':
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in [gl['url'] for gl in guidance_links]:
                        guidance_links.append({
                            'url': full_url,
                            'title': link.get_text(strip=True) or 'No title'
                        })
            
            logger.info(f"Found {len(guidance_links)} guidance document links on page: {page_url}")
            return guidance_links
            
        except Exception as e:
            logger.error(f"Error getting guidance links from {page_url}: {str(e)}")
            return []
    
    def get_pagination_urls(self, soup):
        """Extract pagination URLs"""
        pagination_urls = []
        
        # Look for pagination container
        pagination = soup.find('div', class_='pagination-container')
        if pagination:
            links = pagination.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href and 'start=' in href:
                    full_url = urljoin(BASE_URL, href)
                    pagination_urls.append(full_url)
        
        return pagination_urls
    
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
        
        # Final cleanup
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Remove excessive line breaks
        text = text.strip()
        
        return text
    
    def extract_all_pdfs_from_page(self, soup):
        """Extract content from ALL PDF files found on the page"""
        pdf_content = {}
        pdf_links = []
        
        # Find all PDF links
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href and href.lower().endswith('.pdf'):
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
    
    def extract_guidance_category(self, soup, url):
        """Extract guidance category/type from breadcrumbs or URL"""
        category = ""
        
        # Try breadcrumbs first
        breadcrumbs = soup.find('nav', class_='breadcrumbs') or soup.find('ol', class_='breadcrumb')
        if breadcrumbs:
            category = breadcrumbs.get_text(separator=' > ', strip=True)
        
        # Try to extract from URL structure
        if not category:
            url_parts = url.split('/')
            if 'guidance-library' in url_parts:
                idx = url_parts.index('guidance-library')
                if idx + 1 < len(url_parts):
                    category = url_parts[idx + 1].replace('-', ' ').title()
        
        # Look for category indicators in the page
        if not category:
            category_indicators = soup.find_all(['span', 'div'], class_=re.compile(r'category|type|tag', re.I))
            for indicator in category_indicators:
                text = indicator.get_text(strip=True)
                if text and len(text) < 50:  # Reasonable category length
                    category = text
                    break
        
        return category
    
    def extract_guidance_metadata(self, soup):
        """Extract guidance-specific metadata"""
        metadata = {}
        
        # Look for effective date
        date_patterns = [
            soup.find('time'),
            soup.find('span', class_=re.compile(r'date|effective|published', re.I)),
            soup.find('div', class_=re.compile(r'date|effective|published', re.I)),
            soup.find(text=re.compile(r'effective|published|updated', re.I))
        ]
        
        for pattern in date_patterns:
            if pattern:
                if hasattr(pattern, 'get_text'):
                    date_text = pattern.get_text(strip=True)
                else:
                    date_text = str(pattern).strip()
                
                # Extract date from text
                date_match = re.search(r'\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4}|\d{1,2}\s+\w+\s+\d{2,4}', date_text)
                if date_match:
                    metadata['effective_date'] = date_match.group()
                    break
        
        # Look for guidance type/classification
        guidance_types = ['guidance note', 'information sheet', 'guide', 'regulatory guide', 'fact sheet']
        page_text = soup.get_text().lower()
        
        for gtype in guidance_types:
            if gtype in page_text:
                metadata['guidance_type'] = gtype.title()
                break
        
        # Look for target audience
        audience_keywords = ['financial institutions', 'issuers', 'investors', 'practitioners', 'aml reporting entities']
        for audience in audience_keywords:
            if audience in page_text:
                metadata['target_audience'] = audience.title()
                break
        
        return metadata
    
    def extract_links_from_content(self, soup):
        """Extract all links from guidance content"""
        links = []
        main_content = soup.find('main') or soup
        
        for link in main_content.find_all('a', href=True):
            href = link.get('href')
            if href:
                full_url = urljoin(BASE_URL, href)
                link_text = link.get_text(strip=True)
                if link_text and not href.lower().endswith(('.pdf', '.xlsx', '.csv', '.mp3', '.mp4', '.wav')):
                    links.append({
                        'url': full_url,
                        'text': link_text
                    })
        
        return links
    
    def scrape_guidance_document(self, guidance_url):
        """Scrape individual guidance document"""
        try:
            # Check if already scraped
            if guidance_url in self.existing_guidance:
                logger.info(f"Guidance document already exists: {guidance_url}")
                return self.existing_guidance[guidance_url]
            
            logger.info(f"Scraping guidance document: {guidance_url}")
            self.random_delay()
            
            response = self.session.get(guidance_url)
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
            
            # Extract category/theme
            category = self.extract_guidance_category(soup, guidance_url)
            
            # Extract guidance-specific metadata
            metadata = self.extract_guidance_metadata(soup)
            
            # Extract image
            image_url = ""
            img = soup.find('img')
            if img and img.get('src'):
                image_url = urljoin(BASE_URL, img.get('src'))
            
            # Extract related links (excluding files)
            related_links = self.extract_links_from_content(soup)
            
            # Extract ALL PDF content
            pdf_content, pdf_links = self.extract_all_pdfs_from_page(soup)
            
            # Combine all content for LLM analysis
            full_content = content
            
            if pdf_content:
                full_content += "\n\n=== PDF CONTENT ===\n\n"
                for pdf_key, pdf_data in pdf_content.items():
                    full_content += f"\n--- {pdf_data['filename']} ---\n\n"
                    full_content += pdf_data['content']
            
            # Final LLM-friendly cleaning
            full_content = self.clean_text_for_llm(full_content)
            
            # Generate unique ID
            doc_id = hashlib.md5(guidance_url.encode()).hexdigest()
            
            guidance_data = {
                'id': doc_id,
                'url': guidance_url,
                'title': title,
                'description': description,
                'category': category,
                'guidance_type': metadata.get('guidance_type', ''),
                'target_audience': metadata.get('target_audience', ''),
                'effective_date': metadata.get('effective_date', ''),
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
            
            logger.info(f"Successfully scraped guidance: {title} (PDFs: {len(pdf_content)})")
            return guidance_data
            
        except Exception as e:
            logger.error(f"Error scraping guidance document {guidance_url}: {str(e)}")
            return None
    
    def save_data(self, guidance_docs):
        """Save guidance documents to JSON and CSV files"""
        try:
            # Save JSON
            with open(GUIDANCE_JSON, 'w', encoding='utf-8') as f:
                json.dump(guidance_docs, f, indent=2, ensure_ascii=False)
            
            # Save CSV
            if guidance_docs:
                fieldnames = [
                    'id', 'url', 'title', 'description', 'category', 'guidance_type',
                    'target_audience', 'effective_date', 'scraped_date', 'content',
                    'html_content', 'image_url', 'related_links', 'pdf_links',
                    'content_length', 'pdf_count'
                ]
                
                with open(GUIDANCE_CSV, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for doc in guidance_docs:
                        # Convert complex data to strings for CSV
                        csv_doc = doc.copy()
                        csv_doc['related_links'] = json.dumps(doc.get('related_links', []))
                        csv_doc['pdf_links'] = json.dumps(doc.get('pdf_links', []))
                        # Remove complex nested data from CSV
                        csv_doc.pop('pdf_content', None)
                        writer.writerow(csv_doc)
            
            logger.info(f"Saved {len(guidance_docs)} guidance documents to {GUIDANCE_JSON} and {GUIDANCE_CSV}")
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
    
    def run(self):
        """Main scraping function"""
        logger.info("Starting FMA guidance library scraper...")
        
        if not self.establish_session():
            logger.error("Failed to establish session. Exiting.")
            return
        
        all_guidance = list(self.existing_guidance.values())
        new_guidance_count = 0
        
        try:
            # Start with first page
            current_page = 1
            page_url = GUIDANCE_URL
            
            while current_page <= MAX_PAGES:
                logger.info(f"Scraping page {current_page}: {page_url}")
                
                # Get guidance document links from current page
                guidance_links = self.get_guidance_links(page_url)
                
                if not guidance_links:
                    logger.info("No more guidance documents found.")
                    break
                
                # Scrape each guidance document
                for link_info in guidance_links:
                    guidance_url = link_info['url']
                    
                    # Skip if already exists
                    if guidance_url in self.existing_guidance:
                        continue
                    
                    guidance_data = self.scrape_guidance_document(guidance_url)
                    if guidance_data:
                        all_guidance.append(guidance_data)
                        new_guidance_count += 1
                        
                        # Save periodically
                        if new_guidance_count % 3 == 0:  # Save more frequently due to PDF processing
                            self.save_data(all_guidance)
                
                # Get next page URL
                if current_page < MAX_PAGES:
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
            self.save_data(all_guidance)
            
            logger.info(f"Scraping completed. Total guidance documents: {len(all_guidance)}, New documents: {new_guidance_count}")
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            # Save whatever we have
            self.save_data(all_guidance)

def main():
    """Main function"""
    scraper = FMAGuidanceScraper()
    scraper.run()

if __name__ == "__main__":
    main()