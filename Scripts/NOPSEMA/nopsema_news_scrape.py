#!/usr/bin/env python3
"""
NOPSEMA News Scraper - Fully Reviewed Working Version
Based on successful test approach with all bugs fixed.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import hashlib
import logging
import time
import random
from datetime import datetime
from typing import Dict, List, Set, Optional
from urllib.parse import urljoin
import re

import requests
from bs4 import BeautifulSoup
import PyPDF2
import pandas as pd


class NOPSEMAScraper:
    def __init__(self, max_pages: int = 1, data_dir: str = "./data"):
        """Initialize the NOPSEMA scraper."""
        self.base_url = "https://www.nopsema.gov.au"
        self.blog_url = "https://www.nopsema.gov.au/blogs"
        self.max_pages = max_pages
        self.data_dir = data_dir
        self.output_file = os.path.join(data_dir, "nopsema_news.json")
        self.log_file = os.path.join(data_dir, "nopsema_scrape.log")
        
        # Create data directory
        os.makedirs(data_dir, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize session - exact same as working test
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        
        # Load existing data for deduplication
        self.existing_articles = self._load_existing_data()
        self.scraped_urls: Set[str] = set()
        self.consecutive_duplicates = 0
        
        self.logger.info(f"Initialized scraper with max_pages={max_pages}")
        self.logger.info(f"Found {len(self.existing_articles)} existing articles")
    
    def _setup_logging(self):
        """Setup logging configuration."""
        # Clear any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_existing_data(self) -> List[Dict]:
        """Load existing scraped articles for deduplication."""
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Could not load existing data: {e}")
        return []
    
    def _get_article_hash(self, url: str, headline: str) -> str:
        """Generate a hash for article deduplication."""
        return hashlib.md5(f"{url}_{headline}".encode()).hexdigest()
    
    def _is_duplicate(self, url: str, headline: str) -> bool:
        """Check if article is already scraped."""
        article_hash = self._get_article_hash(url, headline)
        for article in self.existing_articles:
            if article.get('url') == url:
                return True
            if article.get('hash') == article_hash:
                return True
       
        return False
    
    def _human_delay(self, min_seconds: float = 1.0, max_seconds: float = 3.0):
        """Add human-like delay between requests."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def _is_valid_article_url(self, url: str) -> bool:
        """Check if URL is a valid article (not PDF, external site, etc.)."""
        
        # Skip PDFs - they should be handled within articles, not as articles
        if url.lower().endswith('.pdf'):
            return False
        
        # Skip external sites
        external_domains = [
            'consultation.nopsema.gov.au',
            'info.nopsema.gov.au', 
            'online.nopsema.gov.au',
            'oir.gov.au'
        ]
        
        for domain in external_domains:
            if domain in url:
                return False
        
        # Skip specific non-article pages on main site
        skip_paths = [
            '/published-directions-and-notices',
            '/offshore-industry/directions-notices-alerts',
            '/offshore-industry/submissions',
            '/contact',
            '/search',
            '/sitemap',
            '/about',
            '/careers'
        ]
        
        for path in skip_paths:
            if path in url:
                return False
        
        # Accept URLs that are on main domain and contain 'blogs'
        if 'nopsema.gov.au' in url and 'blogs' in url:
            return True
        
        return False
    
    def _extract_article_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract article links from page."""
        links = []
        
        # Find article cards
        cards = soup.find_all('div', class_='au-card--clickable')
        self.logger.info(f"Found {len(cards)} article cards")
        
        if not cards:
            self.logger.warning("No article cards found")
            return links
        
        for i, card in enumerate(cards):
            try:
                link = card.find('a', class_='au-card--clickable__link')
                if link and link.get('href'):
                    href = link.get('href')
                    title = link.get_text().strip()
                    full_url = urljoin(self.base_url, href)
                    
                    # Log all URLs found for debugging
                    self.logger.info(f"Found URL {i+1}: {full_url}")
                    
                    # Filter out non-article URLs
                    if self._is_valid_article_url(full_url):
                        links.append(full_url)
                        self.logger.info(f"✅ Accepted: '{title}'")
                    else:
                        self.logger.info(f"❌ Filtered out: {full_url}")
                    
            except Exception as e:
                self.logger.warning(f"Error processing card {i+1}: {e}")
        
        self.logger.info(f"Final result: {len(links)} valid article links out of {len(cards)} cards")
        return links
    
    def _clean_text(self, text: str) -> str:
        """Clean text for LLM processing."""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common HTML artifacts
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&quot;', '"', text)
        text = re.sub(r'&#39;', "'", text)
        
        return text
    
    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF files with improved error handling."""
        try:
            self.logger.info(f"Extracting PDF: {pdf_url}")
            
            # Use specific headers for PDF requests
            headers = self.session.headers.copy()
            headers['Accept'] = 'application/pdf,*/*'
            
            response = self.session.get(pdf_url, headers=headers, timeout=60)
            response.raise_for_status()
            
            # Check if we actually got a PDF
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and not pdf_url.lower().endswith('.pdf'):
                self.logger.warning(f"URL doesn't appear to be a PDF: {pdf_url}")
                return ""
            
            from io import BytesIO
            pdf_file = BytesIO(response.content)
            text_content = []
            
            try:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                self.logger.info(f"PDF has {len(pdf_reader.pages)} pages")
                
                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        text = page.extract_text()
                        if text and text.strip():
                            text_content.append(f"--- Page {page_num + 1} ---\n{text}")
                        else:
                            self.logger.warning(f"No text extracted from page {page_num + 1}")
                    except Exception as e:
                        self.logger.warning(f"Failed to extract page {page_num + 1}: {e}")
                
            except Exception as e:
                self.logger.warning(f"Failed to read PDF structure: {e}")
                return ""
            
            if text_content:
                full_text = '\n\n'.join(text_content)
                self.logger.info(f"Successfully extracted {len(text_content)} pages from PDF")
                return self._clean_text(full_text)
            else:
                self.logger.warning(f"No text content extracted from PDF: {pdf_url}")
                return ""
            
        except Exception as e:
            self.logger.warning(f"Failed to extract PDF {pdf_url}: {e}")
            return ""
    
    def _extract_csv_excel_data(self, file_url: str, file_type: str) -> Dict:
        """Extract data from CSV or Excel files."""
        try:
            self.logger.info(f"Extracting {file_type}: {file_url}")
            
            response = self.session.get(file_url, timeout=60)
            response.raise_for_status()
            
            if file_type.lower() == 'csv':
                from io import StringIO
                df = pd.read_csv(StringIO(response.text))
            else:  # Excel
                from io import BytesIO
                df = pd.read_excel(BytesIO(response.content))
            
            return {
                'headers': df.columns.tolist(),
                'data': df.to_dict('records'),
                'summary': {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_types': df.dtypes.astype(str).to_dict()
                }
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to extract {file_type} {file_url}: {e}")
            return {}
    
    def _extract_article_content(self, article_url: str) -> Dict:
        """Extract content from a single article page."""
        try:
            self.logger.info(f"Scraping article: {article_url}")
            
            response = self.session.get(article_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract article data
            article = soup.find('article')
            if not article:
                self.logger.warning(f"No article content found for {article_url}")
                return {}
            
            # Extract headline
            headline_elem = article.find('h1', class_='page-title')
            headline = self._clean_text(headline_elem.get_text()) if headline_elem else ""
            
            # Check for duplicates
            if self._is_duplicate(article_url, headline):
                self.logger.info(f"Duplicate article found: {headline}")
                self.consecutive_duplicates += 1
                return {'duplicate': True}
            
            self.consecutive_duplicates = 0
            
            # Extract published date
            date_elem = article.find('div', class_='field--name-field-blog-date')
            published_date = ""
            if date_elem:
                date_text = date_elem.find('div', class_='field__item')
                if date_text:
                    published_date = self._clean_text(date_text.get_text())
            
            # Extract theme/category
            theme = ""
            category_elem = article.find('div', class_='field--name-field-blog-categories')
            if category_elem:
                category_link = category_elem.find('a')
                if category_link:
                    theme = self._clean_text(category_link.get_text())
            
            # Extract main content
            content_elem = article.find('div', class_='field--name-body')
            content_text = ""
            related_links = []
            pdf_links = []
            csv_excel_links = []
            
            if content_elem:
                content_text = self._clean_text(content_elem.get_text())
                
                # Extract links from content
                for link in content_elem.find_all('a', href=True):
                    href = link.get('href')
                    if href:
                        full_link = urljoin(self.base_url, href)
                        link_text = self._clean_text(link.get_text())
                        
                        # Categorize links
                        if href.lower().endswith('.pdf'):
                            pdf_links.append({'url': full_link, 'text': link_text})
                        elif href.lower().endswith(('.csv', '.xlsx', '.xls')):
                            file_type = href.lower().split('.')[-1]
                            csv_excel_links.append({'url': full_link, 'text': link_text, 'type': file_type})
                        elif not href.startswith('#') and 'nopsema.gov.au' in full_link:
                            related_links.append({'url': full_link, 'text': link_text})
            
            # Extract associated image
            associated_image_url = ""
            img_elem = article.find('img')
            if img_elem and img_elem.get('src'):
                associated_image_url = urljoin(self.base_url, img_elem.get('src'))
            
            # Process PDFs
            pdf_text = ""
            unique_pdfs = {link['url'] for link in pdf_links}
            for pdf_url in unique_pdfs:
                extracted_text = self._extract_pdf_text(pdf_url)
                if extracted_text:
                    pdf_text += f"\n--- PDF Content from {pdf_url} ---\n{extracted_text}\n"
            
            # Process CSV/Excel files
            csv_data = {}
            excel_data = {}
            for file_info in csv_excel_links:
                file_url = file_info['url']
                file_type = file_info['type']
                
                extracted_data = self._extract_csv_excel_data(file_url, file_type)
                if extracted_data:
                    if file_type == 'csv':
                        csv_data[file_url] = extracted_data
                    else:
                        excel_data[file_url] = extracted_data
            
            # Create article record
            article_data = {
                'url': article_url,
                'headline': headline,
                'published_date': published_date,
                'scraped_date': datetime.now().isoformat(),
                'theme': theme,
                'content_text': content_text,
                'related_links': [{'url': link['url'], 'text': link['text']} for link in related_links],
                'associated_image_url': associated_image_url,
                'pdf_text': self._clean_text(pdf_text),
                'csv_data': csv_data,
                'excel_data': excel_data,
                'hash': self._get_article_hash(article_url, headline)
            }
            
            self.logger.info(f"Successfully scraped: {headline}")
            return article_data
            
        except Exception as e:
            self.logger.error(f"Error scraping article {article_url}: {e}")
            return {}
    
    def scrape_all_articles(self) -> List[Dict]:
        """Main scraping function with manual page URL construction."""
        self.logger.info("Starting NOPSEMA news scraping...")
        
        new_articles = []
        
        try:
            for page_num in range(self.max_pages):
                # Construct page URL manually to avoid pagination bugs
                current_page_url = f"{self.blog_url}?page={page_num}"
                
                self.logger.info(f"Scraping page {page_num + 1}: {current_page_url}")
                
                # Get page
                response = self.session.get(current_page_url, timeout=30)
                response.raise_for_status()
                
                self.logger.info(f"Response status: {response.status_code}")
                self.logger.info(f"Content length: {len(response.content)} bytes")
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Verify page title
                title = soup.find('title')
                page_title = title.get_text() if title else "No title"
                self.logger.info(f"Page title: '{page_title}'")
                
                # Verify we're on the blogs page
                if 'blogs' not in page_title.lower():
                    self.logger.warning(f"Page title doesn't contain 'blogs' - might be wrong page")
                    # Save debug HTML
                    try:
                        debug_file = os.path.join(self.data_dir, f"debug_page_{page_num}.html")
                        with open(debug_file, 'w', encoding='utf-8') as f:
                            f.write(response.text)
                        self.logger.info(f"Saved debug HTML to {debug_file}")
                    except Exception as e:
                        self.logger.warning(f"Could not save debug HTML: {e}")
                
                # Extract article links
                article_links = self._extract_article_links(soup)
                
                if not article_links:
                    self.logger.warning(f"No valid articles found on page {page_num + 1}")
                    continue
                
                # Scrape each article
                for article_url in article_links:
                    if article_url in self.scraped_urls:
                        self.logger.info(f"Skipping already scraped: {article_url}")
                        continue
                    
                    self.scraped_urls.add(article_url)
                    self._human_delay(1, 3)
                    
                    article_data = self._extract_article_content(article_url)
                    
                    if article_data.get('duplicate'):
                        continue
                    elif article_data:
                        new_articles.append(article_data)
                
                # Check stopping condition for daily runs
                if self.consecutive_duplicates >= 5:
                    self.logger.info("Found 5 consecutive duplicates, stopping")
                    break
                
                # Add delay between pages
                self._human_delay(2, 5)
            
            self.logger.info(f"Scraping completed. New articles found: {len(new_articles)}")
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            raise
        
        return new_articles
    
    def save_results(self, new_articles: List[Dict]):
        """Save scraping results to JSON file."""
        try:
            all_articles = self.existing_articles + new_articles
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(all_articles)} total articles to {self.output_file}")
            self.logger.info(f"Added {len(new_articles)} new articles")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
    
    def run(self):
        """Run the complete scraping process."""
        start_time = datetime.now()
        self.logger.info(f"Starting scraper at {start_time}")
        
        try:
            new_articles = self.scrape_all_articles()
            self.save_results(new_articles)
            
            end_time = datetime.now()
            duration = end_time - start_time
            self.logger.info(f"Scraping completed in {duration}")
            
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='NOPSEMA News Scraper')
    parser.add_argument('--max-pages', type=int, default=1, 
                       help='Maximum number of pages to scrape')
    parser.add_argument('--data-dir', type=str, default='./data',
                       help='Directory to save output files')
    
    args = parser.parse_args()
    
    scraper = NOPSEMAScraper(max_pages=args.max_pages, data_dir=args.data_dir)
    scraper.run()


if __name__ == "__main__":
    main()