#!/usr/bin/env python3
"""
ATSB News Portal Comprehensive Scraper
Extracts news articles and associated investigation reports for LLM analysis
"""

import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import hashlib

import requests
from bs4 import BeautifulSoup
import pandas as pd
import PyPDF2
import fitz  # PyMuPDF for better PDF extraction
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import undetected_chromedriver as uc

# Configuration
BASE_URL = "https://www.atsb.gov.au"
NEWS_URL = f"{BASE_URL}/news"
MAX_PAGES = 3  # Total pages available - adjust for daily runs (set to 3)
DAILY_RUN_PAGES = 3  # For scheduled daily runs
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "atsb_news.json"
LOG_FILE = "atsb_scraper.log"
VERBOSE_PDF_LOGGING = False  # Set to True for detailed PDF extraction logging

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ATSBScraper:
    def __init__(self, is_daily_run: bool = False):
        self.session = requests.Session()
        self.driver = None
        self.is_daily_run = is_daily_run
        self.max_pages = DAILY_RUN_PAGES if is_daily_run else MAX_PAGES
        self.existing_articles = self._load_existing_articles()
        self.scraped_urls: Set[str] = set()
        
        # Setup session headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def _load_existing_articles(self) -> Dict[str, Dict]:
        """Load existing articles to avoid duplication"""
        if not OUTPUT_FILE.exists():
            return {}
        
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                articles = json.load(f)
                return {self._generate_article_hash(article): article for article in articles}
        except Exception as e:
            logger.error(f"Error loading existing articles: {e}")
            return {}
    
    def _generate_article_hash(self, article: Dict) -> str:
        """Generate unique hash for article based on URL and headline"""
        key = f"{article.get('url', '')}-{article.get('headline', '')}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def _init_driver(self):
        """Initialize undetected Chrome driver with stealth options"""
        try:
            options = uc.ChromeOptions()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = uc.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("Chrome driver initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            raise
    
    def _make_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retries and exponential backoff"""
        for attempt in range(retries):
            try:
                # Add referer header for subsequent requests
                if self.scraped_urls:
                    self.session.headers['Referer'] = list(self.scraped_urls)[-1]
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                self.scraped_urls.add(url)
                
                # Random delay to appear more human-like
                time.sleep(1 + attempt * 0.5)
                return response
                
            except requests.exceptions.RequestException as e:
                wait_time = 2 ** attempt
                logger.warning(f"Request failed for {url} (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
                    return None
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text for LLM consumption"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Remove HTML entities
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        
        return text.strip()
    
    def _extract_links_from_text(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract all relevant links from content"""
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('/'):
                href = urljoin(base_url, href)
            elif not href.startswith('http'):
                continue
            
            # Filter relevant links (reports, PDFs, etc.)
            if any(keyword in href.lower() for keyword in ['report', 'investigation', 'pdf', 'publication']):
                links.append(href)
        
        return list(set(links))  # Remove duplicates
    
    def _extract_pdf_content(self, pdf_url: str) -> str:
        """Extract text content from PDF"""
        try:
            response = self._make_request(pdf_url)
            if not response:
                return ""
            
            # Use PyMuPDF for better text extraction
            pdf_document = fitz.open(stream=response.content, filetype="pdf")
            text_content = []
            
            for page_num in range(pdf_document.page_count):
                page = pdf_document[page_num]
                text = page.get_text()
                
                # Extract tables if present
                tables = page.find_tables()
                for table in tables:
                    try:
                        table_data = table.extract()
                        if table_data:
                            # Convert table to readable format, handling None values
                            clean_rows = []
                            for row in table_data:
                                if row and any(cell for cell in row):  # Skip empty rows
                                    # Convert None values to empty strings
                                    clean_row = [str(cell) if cell is not None else "" for cell in row]
                                    clean_rows.append("\t".join(clean_row))
                            
                            if clean_rows:
                                table_text = "\n".join(clean_rows)
                                text += f"\n\nTable:\n{table_text}\n"
                    except Exception as e:
                        # Only log actual errors if verbose logging is enabled
                        if VERBOSE_PDF_LOGGING and "expected str instance, NoneType found" not in str(e):
                            logger.warning(f"Error extracting table from PDF: {e}")
                
                text_content.append(text)
            
            pdf_document.close()
            return self._clean_text("\n\n".join(text_content))
            
        except Exception as e:
            logger.error(f"Error extracting PDF content from {pdf_url}: {e}")
            return ""
    
    def _extract_excel_content(self, excel_url: str) -> str:
        """Extract data from Excel files"""
        try:
            response = self._make_request(excel_url)
            if not response:
                return ""
            
            # Save temporarily to read with pandas
            temp_file = f"temp_excel_{int(time.time())}.xlsx"
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            # Read all sheets
            excel_data = pd.read_excel(temp_file, sheet_name=None)
            content_parts = []
            
            for sheet_name, df in excel_data.items():
                content_parts.append(f"Sheet: {sheet_name}")
                content_parts.append(df.to_string(index=False))
                content_parts.append("")
            
            # Clean up temp file
            os.remove(temp_file)
            
            return self._clean_text("\n".join(content_parts))
            
        except Exception as e:
            logger.error(f"Error extracting Excel content from {excel_url}: {e}")
            return ""
    
    def _extract_news_articles_from_page(self, page_num: int) -> List[Dict]:
        """Extract news articles from a specific page"""
        page_url = f"{NEWS_URL}?page={page_num}"
        logger.info(f"Scraping page {page_num + 1}: {page_url}")
        
        response = self._make_request(page_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = []
        
        # Find all news article cards
        news_cards = soup.find_all('a', class_='news-item-card')
        
        for card in news_cards:
            try:
                article_url = urljoin(BASE_URL, card['href'])
                
                # Extract basic info from card
                title_elem = card.find('span', class_='title')
                headline = title_elem.text.strip() if title_elem else ""
                
                img_elem = card.find('img')
                image_url = urljoin(BASE_URL, img_elem['src']) if img_elem and img_elem.get('src') else None
                
                # Check if already scraped
                temp_article = {'url': article_url, 'headline': headline}
                if self._generate_article_hash(temp_article) in self.existing_articles:
                    logger.info(f"Skipping existing article: {headline}")
                    continue
                
                # Extract full article content
                article_data = self._extract_full_article(article_url, headline, image_url)
                if article_data:
                    articles.append(article_data)
                    
            except Exception as e:
                logger.error(f"Error processing news card: {e}")
                continue
        
        return articles
    
    def _extract_full_article(self, article_url: str, headline: str, image_url: Optional[str]) -> Optional[Dict]:
        """Extract full article content and associated reports"""
        logger.info(f"Extracting article: {headline}")
        
        response = self._make_request(article_url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract publication date
        published_date = ""
        date_elem = soup.find('time', class_='datetime')
        if date_elem:
            published_date = date_elem.get('datetime', date_elem.text.strip())
        
        # Extract main article content
        content_elem = soup.find('div', class_='field--name-field-content')
        article_text = ""
        if content_elem:
            # Remove script and style elements
            for script in content_elem(["script", "style"]):
                script.decompose()
            article_text = self._clean_text(content_elem.get_text())
        
        # Extract related links
        related_links = self._extract_links_from_text(soup, BASE_URL)
        
        # Look for investigation report link
        report_content = ""
        report_link = soup.find('a', href=re.compile(r'/publications/investigation_reports/'))
        if report_link:
            report_url = urljoin(BASE_URL, report_link['href'])
            logger.info(f"Found investigation report: {report_url}")
            report_content = self._extract_investigation_report(report_url)
            if report_content:
                related_links.append(report_url)
        
        # Combine article and report content
        full_content = article_text
        if report_content:
            full_content += f"\n\n--- Investigation Report ---\n\n{report_content}"
        
        return {
            'url': article_url,
            'headline': headline,
            'published_date': published_date,
            'scraped_date': datetime.now().isoformat(),
            'news_theme': self._extract_theme_from_content(full_content),
            'article_text': full_content,
            'associated_image_url': image_url,
            'related_links': related_links
        }
    
    def _extract_theme_from_content(self, content: str) -> str:
        """Extract theme/category from content based on keywords"""
        content_lower = content.lower()
        
        themes = {
            'helicopter': ['helicopter', 'rotor', 'chopper'],
            'aircraft': ['aircraft', 'airplane', 'plane', 'aviation'],
            'maritime': ['ship', 'vessel', 'maritime', 'boat'],
            'rail': ['train', 'railway', 'rail'],
            'drone': ['drone', 'uas', 'unmanned'],
            'safety': ['safety', 'accident', 'incident'],
        }
        
        for theme, keywords in themes.items():
            if any(keyword in content_lower for keyword in keywords):
                return theme
        
        return 'general'
    
    def _extract_investigation_report(self, report_url: str) -> str:
        """Extract content from investigation report page and associated files"""
        response = self._make_request(report_url)
        if not response:
            return ""
        
        soup = BeautifulSoup(response.content, 'html.parser')
        content_parts = []
        
        # Extract main report content
        main_content = soup.find('div', class_='node__content')
        if main_content:
            # Remove navigation and non-content elements
            for elem in main_content.find_all(['nav', 'script', 'style']):
                elem.decompose()
            
            content_parts.append(self._clean_text(main_content.get_text()))
        
        # Look for PDF download link
        pdf_link = soup.find('a', href=re.compile(r'\.pdf$', re.I))
        if pdf_link:
            pdf_url = urljoin(BASE_URL, pdf_link['href'])
            logger.info(f"Extracting PDF content: {pdf_url}")
            pdf_content = self._extract_pdf_content(pdf_url)
            if pdf_content:
                content_parts.append(f"\n--- PDF Content ---\n{pdf_content}")
        
        # Look for Excel files
        excel_link = soup.find('a', href=re.compile(r'\.(xlsx?|csv)$', re.I))
        if excel_link:
            excel_url = urljoin(BASE_URL, excel_link['href'])
            logger.info(f"Extracting Excel content: {excel_url}")
            excel_content = self._extract_excel_content(excel_url)
            if excel_content:
                content_parts.append(f"\n--- Excel Data ---\n{excel_content}")
        
        return "\n\n".join(content_parts)
    
    def scrape_all_news(self) -> List[Dict]:
        """Main scraping function"""
        logger.info(f"Starting ATSB news scraping (max pages: {self.max_pages})")
        
        all_articles = []
        
        for page_num in range(self.max_pages):
            try:
                articles = self._extract_news_articles_from_page(page_num)
                all_articles.extend(articles)
                
                # If no new articles found on daily run, stop early
                if self.is_daily_run and not articles:
                    logger.info("No new articles found, stopping early")
                    break
                    
                logger.info(f"Extracted {len(articles)} articles from page {page_num + 1}")
                
            except Exception as e:
                logger.error(f"Error scraping page {page_num}: {e}")
                continue
        
        return all_articles
    
    def save_articles(self, new_articles: List[Dict]):
        """Save articles to JSON file"""
        if not new_articles:
            logger.info("No new articles to save")
            return
        
        # Load existing articles if file exists
        existing_data = []
        if OUTPUT_FILE.exists():
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
        
        # Append new articles
        all_articles = existing_data + new_articles
        
        # Save to file
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(new_articles)} new articles to {OUTPUT_FILE}")
            logger.info(f"Total articles in database: {len(all_articles)}")
            
        except Exception as e:
            logger.error(f"Error saving articles: {e}")
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")
        
        self.session.close()
    
    def run(self):
        """Main execution function"""
        try:
            # Initialize driver if needed for complex pages
            # self._init_driver()
            
            # Scrape articles
            new_articles = self.scrape_all_news()
            
            # Save results
            self.save_articles(new_articles)
            
            logger.info("Scraping completed successfully")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise
        finally:
            self.cleanup()

def main():
    """Main function with command line argument support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ATSB News Scraper')
    parser.add_argument('--daily', action='store_true', 
                       help='Run in daily mode (limited pages)')
    
    args = parser.parse_args()
    
    scraper = ATSBScraper(is_daily_run=args.daily)
    scraper.run()

if __name__ == "__main__":
    main()