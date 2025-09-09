#!/usr/bin/env python3
"""
RBA News Scraper - Complete Enhanced Version
Comprehensive scraper for Reserve Bank of Australia news and announcements
with all requirements implemented
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import time
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
import re
import hashlib
from fake_useragent import UserAgent
import PyPDF2
import io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import pandas as pd

# Configuration
BASE_URL = "https://www.rba.gov.au"
NEWS_URL = f"{BASE_URL}/news/"
DATA_FOLDER = "data"
MAX_PAGES = 2  # Set to high number for full scrape, set to 3 for daily runs
LOG_FILE = f"{DATA_FOLDER}/rba_scraper.log"
JSON_FILE = f"{DATA_FOLDER}/rba_all_news.json"
CSV_FILE = f"{DATA_FOLDER}/rba_all_news.csv"

# Setup logging
os.makedirs(DATA_FOLDER, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RBAScraper:
    def __init__(self):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.setup_session()
        self.existing_articles = self.load_existing_data()
        
    def setup_session(self):
        """Setup session with headers and retry strategy"""
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        })
        
    def get_page_with_stealth(self, url, delay=True):
        """Get page with stealth techniques"""
        if delay:
            time.sleep(random.uniform(1, 3))
            
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def establish_session(self):
        """Visit main page to establish session and collect cookies"""
        logger.info("Establishing session...")
        try:
            main_response = self.get_page_with_stealth(BASE_URL)
            if main_response:
                logger.info("Successfully visited main page")
                
            news_response = self.get_page_with_stealth(NEWS_URL)
            if news_response:
                logger.info("Successfully visited news page")
                return news_response
            else:
                logger.error("Failed to establish session")
                return None
        except Exception as e:
            logger.error(f"Error establishing session: {e}")
            return None
    
    def create_article_hash(self, headline, published_date):
        """
        Create unique hash for article deduplication using headline and published date
        This handles duplicate article names with different dates (like RBA Balance Sheet)
        """
        normalized_headline = re.sub(r'\s+', ' ', headline.strip().lower())
        normalized_date = self.normalize_date_to_day(published_date)
        combined = f"{normalized_headline}|{normalized_date}"
        return hashlib.md5(combined.encode('utf-8')).hexdigest()
    
    def normalize_date_to_day(self, date_str):
        """Normalize date string to YYYY-MM-DD format for consistent comparison"""
        if not date_str:
            return ""
        
        try:
            if 'T' in date_str:
                date_part = date_str.split('T')[0]
                return date_part
            elif ' ' in date_str and ':' in date_str:
                date_part = date_str.split(' ')[0]
                return date_part
            elif re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                return date_str[:10]
            else:
                from dateutil import parser
                dt = parser.parse(date_str)
                return dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Could not normalize date '{date_str}': {e}")
            return date_str.strip()
    
    def load_existing_data(self):
        """Load existing articles for deduplication"""
        existing = {}
        if os.path.exists(JSON_FILE):
            try:
                with open(JSON_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for article in data:
                        unique_key = self.create_article_hash(
                            article.get('headline', ''),
                            article.get('published_date', '')
                        )
                        existing[unique_key] = article
                        
                logger.info(f"Loaded {len(existing)} existing articles")
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
        return existing
    
    def extract_excel_csv_data(self, file_url, file_type='xlsx'):
        """Extract ALL data from Excel or CSV files - no row limits"""
        try:
            logger.info(f"Extracting {file_type.upper()} data from: {file_url}")
            response = self.get_page_with_stealth(file_url)
            if not response:
                return ""
            
            content = ""
            
            if file_type.lower() in ['xlsx', 'xls']:
                try:
                    excel_data = pd.read_excel(io.BytesIO(response.content), sheet_name=None)
                    
                    for sheet_name, df in excel_data.items():
                        content += f"\n--- Sheet: {sheet_name} ---\n"
                        
                        if not df.empty:
                            content += f"Data dimensions: {df.shape[0]} rows x {df.shape[1]} columns\n"
                            content += "Columns: " + " | ".join(str(col) for col in df.columns) + "\n"
                            content += "Complete data:\n"
                            
                            # Extract ALL rows - no limits
                            for i, row in df.iterrows():
                                row_data = " | ".join(str(val) if pd.notna(val) else "--" for val in row)
                                content += f"Row {i+1}: {row_data}\n"
                        else:
                            content += "Sheet is empty\n"
                            
                except Exception as e:
                    logger.error(f"Error processing Excel file: {e}")
                    content += f"Error reading Excel file: {str(e)}\n"
                    
            elif file_type.lower() == 'csv':
                try:
                    csv_data = pd.read_csv(io.BytesIO(response.content))
                    
                    content += f"CSV Data - {csv_data.shape[0]} rows x {csv_data.shape[1]} columns\n"
                    content += "Columns: " + " | ".join(str(col) for col in csv_data.columns) + "\n"
                    content += "Complete data:\n"
                    
                    # Extract ALL rows - no limits
                    for i, row in csv_data.iterrows():
                        row_data = " | ".join(str(val) if pd.notna(val) else "--" for val in row)
                        content += f"Row {i+1}: {row_data}\n"
                        
                except Exception as e:
                    logger.error(f"Error processing CSV file: {e}")
                    content += f"Error reading CSV file: {str(e)}\n"
            
            logger.info(f"Extracted {len(content)} characters from {file_type.upper()} file")
            return content
            
        except Exception as e:
            logger.error(f"Error extracting {file_type.upper()} file {file_url}: {e}")
            return ""
    
    def extract_pdf_text(self, pdf_url):
        """Extract text from PDF"""
        try:
            logger.info(f"Extracting PDF: {pdf_url}")
            response = self.get_page_with_stealth(pdf_url)
            if not response:
                return ""
                
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text()
            
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'[^\w\s\.,;:!?\-\(\)"\']', '', text)
            text = text.strip()
            
            logger.info(f"Extracted {len(text)} characters from PDF")
            return text
        except Exception as e:
            logger.error(f"Error extracting PDF {pdf_url}: {e}")
            return ""
    
    def clean_cell_text(self, cell):
        """Clean cell text while preserving numbers and data"""
        text = cell.get_text(strip=True)
        
        for sup in cell.find_all('sup'):
            sup.decompose()
        
        text = cell.get_text(strip=True)
        text = text.replace('&nbsp;', '').replace('\xa0', ' ')
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def extract_table_data(self, soup):
        """Extract structured data from tables with enhanced handling"""
        table_content = ""
        tables = soup.find_all('table')
        
        for table_idx, table in enumerate(tables):
            table_content += f"\n--- Table {table_idx + 1} ---\n"
            
            caption = table.find('caption')
            if caption:
                caption_text = self.clean_cell_text(caption)
                table_content += f"Caption: {caption_text}\n"
            
            thead = table.find('thead')
            tbody = table.find('tbody') 
            tfoot = table.find('tfoot')
            
            if thead:
                header_rows = thead.find_all('tr')
                for i, row in enumerate(header_rows):
                    cells = row.find_all(['th', 'td'])
                    if cells:
                        cell_texts = []
                        for cell in cells:
                            text = self.clean_cell_text(cell)
                            if text:
                                colspan = cell.get('colspan', '1')
                                if colspan != '1':
                                    text += f" (spans {colspan} cols)"
                                cell_texts.append(text)
                        
                        if cell_texts:
                            table_content += f"Header Row {i+1}: {' | '.join(cell_texts)}\n"
            
            if tbody:
                data_rows = tbody.find_all('tr')
                for i, row in enumerate(data_rows):
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        cell_texts = []
                        for cell in cells:
                            text = self.clean_cell_text(cell)
                            if text:
                                cell_texts.append(text)
                            else:
                                cell_texts.append('--')
                        
                        if any(text != '--' for text in cell_texts):
                            table_content += f"Data Row {i+1}: {' | '.join(cell_texts)}\n"
            
            elif not thead and not tbody:
                rows = table.find_all('tr')
                for i, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        cell_texts = []
                        for cell in cells:
                            text = self.clean_cell_text(cell)
                            if text:
                                cell_texts.append(text)
                            else:
                                cell_texts.append('--')
                        
                        if any(text != '--' for text in cell_texts):
                            row_type = "Header" if row.find('th') else "Data"
                            table_content += f"{row_type} Row {i+1}: {' | '.join(cell_texts)}\n"
            
            if tfoot:
                footer_rows = tfoot.find_all('tr')
                for i, row in enumerate(footer_rows):
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        cell_texts = []
                        for cell in cells:
                            text = self.clean_cell_text(cell)
                            if text:
                                cell_texts.append(text)
                        
                        if cell_texts:
                            table_content += f"Footer {i+1}: {' | '.join(cell_texts)}\n"
        
        return table_content.strip()

    def extract_relevant_links(self, soup, article_url):
        """
        Extract only relevant links from content and Related Information section
        Excludes marketing, navigation, and other irrelevant links
        """
        relevant_links = []
        
        # Extract links from main content paragraphs
        main_content = soup.find('div', {'id': 'content'}) or soup.find('section')
        if main_content:
            content_paragraphs = main_content.find_all('p')
            for p in content_paragraphs:
                parent_classes = p.parent.get('class', []) if p.parent else []
                if any(cls in str(parent_classes) for cls in ['nav', 'sharing', 'source', 'meta']):
                    continue
                
                links = p.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    link_text = link.get_text(strip=True)
                    if href and self.is_relevant_link(href, link_text):
                        full_url = urljoin(BASE_URL, href)
                        if full_url != article_url and full_url not in relevant_links:
                            relevant_links.append(full_url)
        
        # Extract links from "Related Information" section
        related_section = soup.find('aside', role='complementary') or soup.find('div', class_='complementary')
        if related_section:
            related_heading = related_section.find('h2', string=re.compile(r'related.*information', re.I))
            if related_heading:
                related_list = related_heading.find_next_sibling('ul')
                if related_list:
                    links = related_list.find_all('a', href=True)
                    for link in links:
                        href = link.get('href')
                        link_text = link.get_text(strip=True)
                        if href and self.is_relevant_link(href, link_text):
                            full_url = urljoin(BASE_URL, href)
                            if full_url != article_url and full_url not in relevant_links:
                                relevant_links.append(full_url)
        
        return relevant_links
    
    def is_relevant_link(self, href, link_text):
        """Determine if a link is relevant (not marketing, social media, etc.)"""
        href_lower = href.lower()
        text_lower = link_text.lower()
        
        # Exclude social media and sharing links
        if any(social in href_lower for social in ['facebook', 'twitter', 'linkedin', 'instagram', 'youtube']):
            return False
        
        # Exclude sharing and marketing related links
        if any(word in text_lower for word in ['share', 'follow', 'subscribe', 'newsletter']):
            return False
        
        # Exclude navigation links
        if any(nav in href_lower for nav in ['#', 'javascript:', 'mailto:']):
            return False
        
        # Exclude obvious marketing/corporate links
        if any(marketing in href_lower for marketing in ['/about/', '/contact/', '/careers/', '/media/']):
            return False
        
        # Include RBA internal links and external relevant links
        if href.startswith('/') or 'rba.gov.au' in href_lower:
            return True
        
        # Include external links that seem relevant
        relevant_domains = ['.gov.', '.edu.', '.org.', 'bis.org', 'imf.org', 'worldbank.org', 'oecd.org']
        if any(domain in href_lower for domain in relevant_domains):
            return True
        
        return False

    def extract_complete_content(self, soup):
        """
        Extract ALL content from the page including Notes sections and everything else
        Simple, comprehensive approach that captures everything
        """
        main_content = soup.find('div', {'id': 'content'})
        if not main_content:
            return ""
        
        content_parts = []
        
        # Remove only sharing tools and navigation elements we definitely don't want
        for unwanted in main_content.find_all(class_=['sharing-tools']):
            unwanted.decompose()
        
        # Process all meaningful elements in document order
        for element in main_content.find_all():
            # Skip tables (handled separately) and unwanted elements
            if (element.name in ['table', 'script', 'style', 'noscript'] or 
                element.find_parent('table') or
                'sharing-tools' in str(element.get('class', []))):
                continue
            
            # Get text content
            text = element.get_text(strip=True)
            
            if text and len(text) > 2:
                # Check if this element has meaningful child elements
                # If so, skip to avoid duplication (children will be processed)
                children = element.find(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'div'])
                
                if not children:  # This is a leaf element
                    if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        content_parts.append(f"\n=== {text} ===\n")
                    elif element.name == 'li':
                        content_parts.append(f"• {text}\n")
                    elif element.name in ['p', 'div', 'span']:
                        content_parts.append(f"{text} ")
                elif element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    # Always include headers even if they have children
                    direct_text = ''.join(element.find_all(text=True, recursive=False)).strip()
                    if direct_text:
                        content_parts.append(f"\n=== {direct_text} ===\n")
        
        # Fallback: if we didn't get much content, be more aggressive
        full_content = ''.join(content_parts).strip()
        if len(full_content) < 200:
            content_parts = []
            
            # Get all text-containing elements more aggressively
            for element in main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li']):
                if element.find_parent('table') or 'sharing-tools' in str(element.get('class', [])):
                    continue
                
                text = element.get_text(strip=True)
                if text:
                    if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        content_parts.append(f"\n=== {text} ===\n")
                    elif element.name == 'li':
                        content_parts.append(f"• {text}\n")
                    else:
                        content_parts.append(f"{text} ")
        
        # Clean up and return
        content = ''.join(content_parts)
        content = re.sub(r'\s+', ' ', content)
        content = re.sub(r'\n\s*\n+', '\n', content)
        return content.strip()

    def parse_article_page(self, article_url, headline):
        """Parse individual article page with complete content extraction"""
        logger.info(f"Parsing article: {article_url}")
        
        response = self.get_page_with_stealth(article_url)
        if not response:
            return None
            
        soup = BeautifulSoup(response.content, 'html.parser')
        
        article_data = {
            'url': article_url,
            'headline': headline,
            'content': '',
            'pdf_content': '',
            'excel_csv_content': '',
            'related_links': [],
            'images': [],
            'category': '',
            'article_type': '',
            'table_data': ''
        }
        
        # Extract complete content using comprehensive method
        article_data['content'] = self.extract_complete_content(soup)
        
        # Extract table data separately
        article_data['table_data'] = self.extract_table_data(soup)
        
        # Process Excel/CSV files - extract ALL data
        excel_csv_content = ""
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href')
            if href:
                href_lower = href.lower()
                if any(ext in href_lower for ext in ['.xlsx', '.xls']):
                    excel_url = urljoin(BASE_URL, href)
                    excel_content = self.extract_excel_csv_data(excel_url, 'xlsx')
                    if excel_content:
                        excel_csv_content += f"\n--- Excel File: {href} ---\n{excel_content}\n"
                elif '.csv' in href_lower:
                    csv_url = urljoin(BASE_URL, href)
                    csv_content = self.extract_excel_csv_data(csv_url, 'csv')
                    if csv_content:
                        excel_csv_content += f"\n--- CSV File: {href} ---\n{csv_content}\n"
        
        article_data['excel_csv_content'] = excel_csv_content.strip()
        
        # Process PDF files (first one found)
        pdf_links = [link for link in all_links if link.get('href', '').lower().endswith('.pdf')]
        if pdf_links:
            first_pdf = pdf_links[0]
            pdf_url = urljoin(BASE_URL, first_pdf.get('href'))
            article_data['pdf_content'] = self.extract_pdf_text(pdf_url)
        
        # Extract relevant links only
        article_data['related_links'] = self.extract_relevant_links(soup, article_url)
        
        # Extract images (excluding icons, logos, buttons)
        images = soup.find_all('img', src=True)
        for img in images:
            src = img.get('src')
            if src and not any(skip in src.lower() for skip in ['icon', 'logo', 'button']):
                img_url = urljoin(BASE_URL, src)
                article_data['images'].append(img_url)
        
        # Extract category/type information
        writeoff = soup.find('p', class_='writeoff')
        if writeoff:
            article_data['article_type'] = writeoff.get_text(strip=True)
        
        pub_name = soup.find('span', class_='publication-name')
        if pub_name:
            article_data['category'] = pub_name.get_text(strip=True)
        
        return article_data
    
    def parse_datetime(self, datetime_str):
        """Parse datetime from the HTML datetime attribute"""
        try:
            clean_datetime = re.sub(r'[+-]\d{2}:\d{2}$', '', datetime_str)
            dt = datetime.fromisoformat(clean_datetime)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.error(f"Error parsing datetime {datetime_str}: {e}")
            return datetime_str
    
    def scrape_news_page(self):
        """Main scraping function - processes single page with all articles"""
        logger.info("Starting RBA news scraping...")
        
        response = self.establish_session()
        if not response:
            logger.error("Failed to establish session")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = []
        new_articles_count = 0
        articles_processed = 0
        
        # Find all article items on the single page
        article_items = soup.find_all('article', class_='item')
        logger.info(f"Found {len(article_items)} total articles on the page")
        
        for item in article_items:
            if articles_processed >= MAX_PAGES:
                logger.info(f"Reached maximum articles limit: {MAX_PAGES}")
                break
                
            try:
                link_elem = item.find('a', class_=lambda x: x and 'link' in x.split()) or item.find('a')
                if not link_elem:
                    logger.warning("No link element found in article item")
                    continue
                    
                headline = link_elem.get_text(strip=True)
                article_url = urljoin(BASE_URL, link_elem.get('href'))
                
                logger.debug(f"Processing article: {headline} - {article_url}")
                
                # Check if this is an external link
                link_classes = link_elem.get('class', [])
                is_external = 'anchor-external' in link_classes
                
                # Extract datetime
                datetime_elem = item.find('time', class_='datetime')
                published_date = ''
                if datetime_elem:
                    datetime_attr = datetime_elem.get('datetime')
                    if datetime_attr:
                        published_date = self.parse_datetime(datetime_attr)
                
                # Create unique hash for proper deduplication
                article_hash = self.create_article_hash(headline, published_date)
                
                # Check for duplicates
                if article_hash in self.existing_articles:
                    logger.info(f"Skipping existing article: {headline} (published: {published_date})")
                    continue
                
                # Extract article type
                article_type = ''
                writeoff = item.find('p', class_='writeoff')
                if writeoff:
                    article_type = writeoff.get_text(strip=True)
                
                # Base article data
                article_data = {
                    'headline': headline,
                    'url': article_url,
                    'published_date': published_date,
                    'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'article_type': article_type,
                    'is_external': is_external,
                    'content': '',
                    'pdf_content': '',
                    'excel_csv_content': '',
                    'table_data': '',
                    'related_links': [],
                    'images': [],
                    'category': '',
                    'hash': article_hash
                }
                
                # Parse full article if not external
                if not is_external:
                    detailed_data = self.parse_article_page(article_url, headline)
                    if detailed_data:
                        article_data.update(detailed_data)
                        article_data['hash'] = article_hash  # Preserve hash
                
                articles.append(article_data)
                new_articles_count += 1
                articles_processed += 1
                
                logger.info(f"Scraped: {headline} (Published: {published_date})")
                
            except Exception as e:
                logger.error(f"Error processing article: {e}")
                articles_processed += 1
                continue
        
        logger.info(f"Processed {articles_processed} articles, scraped {new_articles_count} new articles")
        return articles
    
    def save_data(self, new_articles):
        """Save scraped data to JSON and CSV with enhanced fields"""
        if not new_articles:
            logger.info("No new articles to save")
            return
        
        # Combine with existing articles
        all_articles = list(self.existing_articles.values()) + new_articles
        
        # Remove duplicates based on hash (final cleanup)
        unique_articles = {}
        for article in all_articles:
            article_hash = article.get('hash')
            if not article_hash:
                article_hash = self.create_article_hash(
                    article.get('headline', ''),
                    article.get('published_date', '')
                )
                article['hash'] = article_hash
            
            unique_articles[article_hash] = article
        
        final_articles = list(unique_articles.values())
        
        # Sort by published date (newest first)
        final_articles.sort(key=lambda x: x.get('published_date', ''), reverse=True)
        
        # Save JSON
        try:
            with open(JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(final_articles, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(final_articles)} articles to {JSON_FILE}")
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")
        
        # Save CSV with all enhanced fields
        try:
            if final_articles:
                fieldnames = [
                    'headline', 'url', 'published_date', 'scraped_date', 'article_type',
                    'category', 'is_external', 'content', 'pdf_content', 'excel_csv_content',
                    'table_data', 'related_links', 'images', 'hash'
                ]
                
                with open(CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for article in final_articles:
                        csv_article = article.copy()
                        csv_article['related_links'] = '|'.join(article.get('related_links', []))
                        csv_article['images'] = '|'.join(article.get('images', []))
                        writer.writerow(csv_article)
                
                logger.info(f"Saved {len(final_articles)} articles to {CSV_FILE}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
        
        logger.info(f"Total unique articles: {len(final_articles)}")
        logger.info(f"New articles added: {len(new_articles)}")
        
        # Print detailed summary of new articles
        if new_articles:
            logger.info("\n--- NEW ARTICLES SUMMARY ---")
            for article in new_articles:
                logger.info(f"Title: {article['headline']}")
                logger.info(f"Date: {article['published_date']}")
                logger.info(f"Type: {article['article_type']}")
                
                # Content summary
                content_summary = []
                if article.get('content'):
                    content_summary.append(f"Text content: {len(article['content'])} chars")
                if article.get('pdf_content'):
                    content_summary.append(f"PDF content: {len(article['pdf_content'])} chars")
                if article.get('excel_csv_content'):
                    content_summary.append(f"Excel/CSV content: {len(article['excel_csv_content'])} chars")
                if article.get('table_data'):
                    content_summary.append(f"Table data: {len(article['table_data'])} chars")
                if article.get('related_links'):
                    content_summary.append(f"{len(article['related_links'])} related links")
                
                logger.info(f"Content: {', '.join(content_summary) if content_summary else 'No content extracted'}")
                logger.info(f"URL: {article['url']}")
                logger.info("---")
    
    def run(self):
        """Main execution function"""
        start_time = datetime.now()
        logger.info(f"RBA Scraper started at {start_time}")
        logger.info(f"Existing articles loaded: {len(self.existing_articles)}")
        
        try:
            new_articles = self.scrape_news_page()
            self.save_data(new_articles)
            
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"Scraping completed in {duration}")
            
        except Exception as e:
            logger.error(f"Scraper failed: {e}")
            raise

def main():
    """Entry point"""
    scraper = RBAScraper()
    scraper.run()

if __name__ == "__main__":
    main()