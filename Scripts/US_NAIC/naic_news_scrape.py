"""
NAIC News Scraper
Production-grade scraper for National Association of Insurance Commissioners newsroom.
Extracts articles, PDFs, and Excel files for LLM analysis.
"""

import json
import os
import time
import random
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium_stealth import stealth
import PyPDF2
import pdfplumber
from PIL import Image
import pytesseract
import pandas as pd
import io
import re


# ==================== CONFIGURATION ====================
BASE_URL = "https://content.naic.org"
NEWSROOM_URL = f"{BASE_URL}/newsroom"
DATA_DIR = Path("./data")
OUTPUT_FILE = DATA_DIR / "naic_news.json"
MAX_PAGES = 1  # Configure number of pages to scrape
DELAY_MIN = 2  # Minimum delay between requests (seconds)
DELAY_MAX = 5  # Maximum delay between requests (seconds)

# Social media and marketing domains to exclude
EXCLUDED_DOMAINS = {
    "facebook.com", "linkedin.com", "twitter.com", "x.com",
    "youtube.com", "instagram.com", "tiktok.com", "pinterest.com"
}


# ==================== UTILITY FUNCTIONS ====================
def create_session() -> requests.Session:
    """Create a requests session with browser-like headers."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': BASE_URL,
        'DNT': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
    })
    return session


def random_delay():
    """Sleep for a random duration to mimic human behavior."""
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def get_url_hash(url: str) -> str:
    """Generate a unique hash for a URL."""
    return hashlib.md5(url.encode()).hexdigest()


def is_excluded_link(url: str) -> bool:
    """Check if URL is from excluded social/marketing domains."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower().replace('www.', '')
    return any(excluded in domain for excluded in EXCLUDED_DOMAINS)


def clean_text(text: str) -> str:
    """Clean extracted text by removing redundant whitespace and non-printable characters."""
    if not text:
        return ""
    
    # Remove non-printable characters except common whitespace
    text = ''.join(char for char in text if char.isprintable() or char in '\n\r\t')
    
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces/tabs to single space
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Multiple newlines to double newline
    text = text.strip()
    
    return text


# ==================== WEB DRIVER SETUP ====================
def create_driver() -> webdriver.Chrome:
    """Create a Selenium WebDriver with stealth settings."""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=chrome_options)
    
    # Apply stealth settings
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True)
    
    return driver


# ==================== PDF EXTRACTION ====================
def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF, including OCR for images if needed."""
    text_parts = []
    
    try:
        # Try pdfplumber first (better for tables and layout)
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                
                # Extract tables separately
                tables = page.extract_tables()
                for table in tables:
                    if table:
                        # Convert table to text format
                        table_text = '\n'.join(['\t'.join([str(cell) if cell else '' for cell in row]) for row in table])
                        text_parts.append(f"\n[TABLE]\n{table_text}\n[/TABLE]\n")
    
    except Exception as e:
        # Fallback to PyPDF2
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
        except Exception as e2:
            print(f"Error extracting PDF text: {e2}")
    
    return clean_text('\n\n'.join(text_parts))


# ==================== EXCEL/CSV EXTRACTION ====================
def extract_text_from_excel(file_content: bytes, filename: str) -> str:
    """Extract text from Excel or CSV files."""
    text_parts = []
    
    try:
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_content))
            text_parts.append(df.to_string(index=False))
        else:
            # Read all sheets from Excel
            excel_file = pd.ExcelFile(io.BytesIO(file_content))
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                text_parts.append(f"[SHEET: {sheet_name}]\n{df.to_string(index=False)}\n")
    
    except Exception as e:
        print(f"Error extracting Excel/CSV text from {filename}: {e}")
    
    return clean_text('\n\n'.join(text_parts))


# ==================== ARTICLE SCRAPING ====================
def scrape_article_links(driver: webdriver.Chrome, page_num: int) -> List[Dict[str, str]]:
    """Scrape article links and metadata from a single page."""
    article_metadata = []
    
    try:
        # Build URL with page parameter
        # Page 1 has no param or page=0, Page 2 = page=1, Page 3 = page=2, etc.
        page_param = page_num - 1
        
        # Construct the complex search_api_fulltext parameter from the pagination links
        search_params = '%22109%22%20%22110%22%20%22111%22%20%22112%22%20%22185%22%20%22114%22%20%22113%22%20%221004%22%20%22363%22%20%22115%22%20%221963%22%20%22557%22%20%221961%22%20%22541%22%20%221959%22%20%22542%22%20%22544%22%20%22545%22%20%22546%22%20%221962%22%20%22548%22%20%22556%22%20%221960%22%20%221958%22'
        
        if page_num == 1:
            url = NEWSROOM_URL
        else:
            url = f"{NEWSROOM_URL}?search_api_fulltext={search_params}&page={page_param}"
        
        print(f"Loading URL: {url}")
        driver.get(url)
        
        # Wait for articles to load with increased timeout
        wait = WebDriverWait(driver, 15)
        
        # Wait for the grid container
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.grid-3, div[class*='grid']")))
            time.sleep(2)  # Additional wait for AJAX content
        except Exception as e:
            print(f"Timeout waiting for articles grid: {e}")
            # Continue anyway, might still have content
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find all article cards - they are in divs with class "relative min-height-25 shadow-normal"
        articles = soup.find_all('div', class_='relative')
        
        if not articles:
            # Try alternative selectors
            articles = soup.find_all('div', class_=lambda x: x and 'shadow-normal' in x)
        
        for article in articles:
            link_tag = article.find('a', href=True)
            if link_tag and '/article/' in link_tag['href']:
                article_url = urljoin(BASE_URL, link_tag['href'])
                
                # Extract article type from the card (the blue text)
                article_type = "Unknown"
                type_tag = link_tag.find('p', class_=lambda x: x and 'text-base' in x and 'color-blue' in x)
                if type_tag:
                    article_type = clean_text(type_tag.get_text())
                
                # Extract headline from the card
                card_headline = "Unknown"
                headline_tag = link_tag.find('h3', class_=lambda x: x and 'text-xl' in x)
                if headline_tag:
                    card_headline = clean_text(headline_tag.get_text())
                
                # Extract date from the card
                card_date = "Unknown"
                date_tag = link_tag.find('p', class_=lambda x: x and 'text-sm' in x and 'color-grey' in x)
                if date_tag:
                    card_date = clean_text(date_tag.get_text())
                
                metadata = {
                    'url': article_url,
                    'type': article_type,
                    'headline': card_headline,
                    'date': card_date
                }
                
                article_metadata.append(metadata)
        
        print(f"Found {len(article_metadata)} articles on page {page_num}")
    
    except Exception as e:
        print(f"Error scraping page {page_num}: {e}")
        import traceback
        traceback.print_exc()
    
    return article_metadata


def scrape_article_content(session: requests.Session, url: str, metadata: Dict[str, str]) -> Optional[Dict]:
    """Scrape content from a single article page."""
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Use metadata from index page for headline, type, and date (more reliable)
        headline = metadata.get('headline', 'Unknown')
        article_type = metadata.get('type', 'Unknown')
        published_date = metadata.get('date', 'Unknown')
        
        # Also try to extract from article page as backup
        if headline == 'Unknown':
            headline_tag = soup.find('h1', class_=lambda x: x and 'text-3xl' in x)
            if headline_tag:
                headline = clean_text(headline_tag.get_text())
        
        if published_date == 'Unknown':
            article_section = soup.find('section', class_='container-sm')
            if article_section:
                date_tag = article_section.find('p', class_=lambda x: x and 'text-lg' in x and 'font-bold' in x and 'color-grey-dark' in x)
                if date_tag:
                    published_date = clean_text(date_tag.get_text())
        
        # Extract main article text
        article_text_parts = []
        
        # The main content is in a div with class containing 'text-xl' within the article section
        article_section = soup.find('section', class_='container-sm')
        if article_section:
            content_div = article_section.find('div', class_=lambda x: x and 'text-xl' in x)
            
            if content_div:
                # Extract all paragraphs and lists
                for element in content_div.find_all(['p', 'ul', 'ol', 'li', 'h2', 'h3', 'h4', 'h5', 'h6']):
                    text = clean_text(element.get_text())
                    if text:
                        article_text_parts.append(text)
        
        article_text = '\n\n'.join(article_text_parts)
        
        # Extract related links (excluding social media)
        related_links = []
        if article_section:
            content_div = article_section.find('div', class_=lambda x: x and 'text-xl' in x)
            if content_div:
                for link in content_div.find_all('a', href=True):
                    link_url = urljoin(BASE_URL, link['href'])
                    if not is_excluded_link(link_url) and link_url != url:
                        related_links.append(link_url)
        
        # Remove duplicates
        related_links = list(set(related_links))
        
        # Extract attachments
        attachments_text = extract_attachments(session, soup, url)
        
        article_data = {
            'headline': headline,
            'article_type': article_type,
            'published_date': published_date,
            'scraped_date': datetime.now().isoformat(),
            'article_url': url,
            'article_text': article_text,
            'attachments_text': attachments_text,
            'related_links': related_links
        }
        
        print(f"✓ Scraped: {headline[:60]}...")
        return article_data
    
    except Exception as e:
        print(f"✗ Error scraping {url}: {e}")
        import traceback
        traceback.print_exc()
        return None


def extract_attachments(session: requests.Session, soup: BeautifulSoup, base_url: str) -> str:
    """Extract text from PDF and Excel attachments."""
    attachments_text_parts = []
    processed_urls = set()
    
    # Find all links to PDFs and Excel files
    for link in soup.find_all('a', href=True):
        href = link['href']
        file_url = urljoin(base_url, href)
        
        # Skip if already processed
        if file_url in processed_urls:
            continue
        
        # Check if it's a PDF or Excel file
        if any(file_url.lower().endswith(ext) for ext in ['.pdf', '.xlsx', '.xls', '.csv']):
            processed_urls.add(file_url)
            
            try:
                print(f"  Downloading attachment: {file_url}")
                response = session.get(file_url, timeout=60)
                response.raise_for_status()
                
                if file_url.lower().endswith('.pdf'):
                    text = extract_text_from_pdf(response.content)
                    if text:
                        attachments_text_parts.append(f"[ATTACHMENT: {file_url}]\n{text}\n")
                
                elif any(file_url.lower().endswith(ext) for ext in ['.xlsx', '.xls', '.csv']):
                    text = extract_text_from_excel(response.content, file_url)
                    if text:
                        attachments_text_parts.append(f"[ATTACHMENT: {file_url}]\n{text}\n")
                
                random_delay()
            
            except Exception as e:
                print(f"  Error downloading {file_url}: {e}")
    
    return clean_text('\n\n'.join(attachments_text_parts))


# ==================== DEDUPLICATION & PERSISTENCE ====================
def load_existing_data() -> Dict[str, Dict]:
    """Load existing scraped articles from JSON file."""
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                articles_list = json.load(f)
                # Convert to dict with URL as key for fast lookup
                return {article['article_url']: article for article in articles_list}
        except Exception as e:
            print(f"Error loading existing data: {e}")
    
    return {}


def save_data(articles: Dict[str, Dict]):
    """Save articles to JSON file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Convert dict back to list for JSON output
    articles_list = list(articles.values())
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(articles_list, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Saved {len(articles_list)} articles to {OUTPUT_FILE}")


# ==================== MAIN SCRAPER ====================
def main():
    """Main scraper function."""
    print("=" * 70)
    print("NAIC News Scraper")
    print("=" * 70)
    print(f"Target: {NEWSROOM_URL}")
    print(f"Max pages: {MAX_PAGES}")
    print(f"Output: {OUTPUT_FILE}")
    print("=" * 70)
    
    # Load existing data
    existing_articles = load_existing_data()
    print(f"\nLoaded {len(existing_articles)} existing articles")
    
    # Initialize driver and session
    driver = None
    try:
        driver = create_driver()
        session = create_session()
        
        # Scrape article links from all pages
        all_article_metadata = []
        for page_num in range(1, MAX_PAGES + 1):
            print(f"\n--- Scraping page {page_num} ---")
            article_metadata = scrape_article_links(driver, page_num)
            
            if not article_metadata:
                print(f"No articles found on page {page_num}. Stopping.")
                break
            
            all_article_metadata.extend(article_metadata)
            random_delay()
        
        # Remove duplicates by URL
        seen_urls = set()
        unique_metadata = []
        for meta in all_article_metadata:
            if meta['url'] not in seen_urls:
                seen_urls.add(meta['url'])
                unique_metadata.append(meta)
        
        print(f"\n✓ Total unique articles found: {len(unique_metadata)}")
        
        # Filter out already scraped articles
        new_article_metadata = [meta for meta in unique_metadata if meta['url'] not in existing_articles]
        print(f"✓ New articles to scrape: {len(new_article_metadata)}")
        
        # Scrape new articles
        if new_article_metadata:
            print("\n--- Scraping article content ---")
            for i, metadata in enumerate(new_article_metadata, 1):
                url = metadata['url']
                print(f"\n[{i}/{len(new_article_metadata)}] {url}")
                article_data = scrape_article_content(session, url, metadata)
                
                if article_data:
                    existing_articles[url] = article_data
                
                random_delay()
            
            # Save updated data
            save_data(existing_articles)
        else:
            print("\n✓ No new articles to scrape. Dataset is up to date.")
    
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if driver:
            driver.quit()
    
    print("\n" + "=" * 70)
    print("Scraping complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()