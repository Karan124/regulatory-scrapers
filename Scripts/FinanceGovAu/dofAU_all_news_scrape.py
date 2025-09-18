#!/usr/bin/env python3
"""
Enhanced Department of Finance Australia News Scraper
- Improved content extraction with better error handling
- PDF content extraction and processing with multiple methods
- LLM-friendly content formatting
- Daily vs Initial run differentiation
- Enhanced robustness and logging
- Based on working Playwright implementation
"""

import os
import logging
import json
import re
import hashlib
import argparse
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin
from typing import List, Dict, Set, Optional
from pathlib import Path

import pandas as pd
import httpx
import fitz  # PyMuPDF
import pdfplumber
import PyPDF2
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeoutError

STEALTH_AVAILABLE = False

# --- ENHANCED CONFIGURATION ---

# Base URL of the website
BASE_URL = "https://www.finance.gov.au"
NEWS_URL = "https://www.finance.gov.au/about-us/news"

# File and Folder Paths
DATA_DIR = Path("data")
PDF_CACHE_DIR = DATA_DIR / "pdf_cache"
OUTPUT_FILENAME_BASE = "dof_all_news_enhanced"
OUTPUT_JSON = DATA_DIR / f"{OUTPUT_FILENAME_BASE}.json"
OUTPUT_CSV = DATA_DIR / f"{OUTPUT_FILENAME_BASE}.csv"
LOG_FILE = DATA_DIR / "scraper.log"

# Anti-Scraping Configuration
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
REQUEST_TIMEOUT = 30

# --- ENHANCED UTILITY FUNCTIONS ---

def setup_logging():
    """Configure enhanced logging to both console and file."""
    DATA_DIR.mkdir(exist_ok=True)
    PDF_CACHE_DIR.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("="*80)
    logging.info("🚀 Enhanced Department of Finance Scraper Started")
    logging.info("="*80)

def get_existing_urls(filepath: Path) -> Set[str]:
    """Load previously scraped URLs to prevent duplicates."""
    if not filepath.exists():
        logging.info("No existing data file found. Treating as initial run.")
        return set()
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Handle both old and new format
        if isinstance(data, dict) and 'articles' in data:
            articles = data['articles']
        else:
            articles = data
            
        urls = {item.get('source_url') for item in articles if item.get('source_url')}
        logging.info(f"Loaded {len(urls)} existing URLs for deduplication.")
        return urls
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logging.error(f"Error reading existing data: {e}. Treating as initial run.")
        return set()

def clean_text(text: str) -> str:
    """Enhanced text cleaning for better LLM consumption."""
    if not text:
        return ""
    
    # Remove non-breaking spaces and normalize whitespace
    text = text.replace(u'\xa0', ' ')
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Multiple empty lines
    
    # Remove common artifacts
    text = re.sub(r'(?i)skip to main content', '', text)
    text = re.sub(r'#$', '', text, flags=re.MULTILINE)  # Remove anchor hash symbols
    
    # Clean up spacing around punctuation
    text = re.sub(r'\s+([.!?,:;])', r'\1', text)
    text = re.sub(r'([.!?])\s*([A-Z])', r'\1 \2', text)
    
    return text.strip()

def generate_hash(url: str, title: str, date: str) -> str:
    """Generate unique hash for article deduplication."""
    content = f"{url}_{title}_{date}"
    return hashlib.sha256(content.encode()).hexdigest()

def is_recent_article(article_date: str, run_type: str, days_back: int = 7) -> bool:
    """Check if article is recent based on run type."""
    if run_type == "initial":
        return True
    
    try:
        if 'T' in article_date:
            parsed_date = datetime.fromisoformat(article_date.replace('Z', '+00:00'))
        else:
            parsed_date = datetime.fromisoformat(article_date)
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
        return parsed_date >= cutoff_date
    except:
        return True  # Default to including if unsure

def format_content_for_llm(article_content: str, pdf_content: str = "") -> str:
    """Format content to be LLM-friendly."""
    formatted_content = ""
    
    # Add main article content
    if article_content and article_content.strip():
        formatted_content += "ARTICLE CONTENT:\n"
        formatted_content += "=" * 50 + "\n"
        formatted_content += article_content.strip() + "\n\n"
    
    # Add PDF content if available
    if pdf_content and pdf_content.strip():
        formatted_content += "ATTACHED DOCUMENT CONTENT:\n"
        formatted_content += "=" * 50 + "\n"
        formatted_content += pdf_content.strip() + "\n\n"
    
    # If no content found
    if not formatted_content.strip():
        return "No content could be extracted from this article."
    
    return formatted_content.strip()

def extract_category(headline: str, themes: str) -> str:
    """Extract category from article headline and themes."""
    combined_text = f"{headline} {themes}".lower()
    
    # Enhanced categories based on Department of Finance classifications
    categories = {
        'budget': ['budget', 'fiscal', 'economic outlook', 'financial statement'],
        'procurement': ['procurement', 'tender', 'contract', 'supplier', 'buying'],
        'pgpa': ['pgpa', 'governance', 'accountability', 'flipchart'],
        'risk': ['risk', 'compliance', 'audit', 'assurance'],
        'technology': ['digital', 'technology', 'ict', 'cyber', 'data'],
        'hr': ['employment', 'career', 'graduate', 'training', 'workforce'],
        'property': ['property', 'construction', 'facility', 'building'],
        'finance': ['financial', 'accounting', 'superannuation', 'investment'],
        'reform': ['reform', 'policy', 'framework', 'strategy'],
        'climate': ['climate', 'sustainability', 'environment', 'carbon'],
        'regulation': ['regulation', 'regulatory', 'compliance', 'legal']
    }
    
    for category, keywords in categories.items():
        if any(keyword in combined_text for keyword in keywords):
            return category.title()
    
    return 'General'

# --- ENHANCED PDF PROCESSING ---

def clean_pdf_text(text: str) -> str:
    """Clean and format PDF text for LLM consumption."""
    if not text:
        return ""
    
    # Remove page headers/footers and artifacts
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        line = line.strip()
        
        # Skip common PDF artifacts
        if not line or len(line) < 3:
            continue
        if re.match(r'^Page \d+ of \d+$', line):
            continue
        if re.match(r'^\d+$', line) and len(line) <= 3:  # Standalone page numbers
            continue
        if line.startswith('---'):  # Page markers
            continue
        
        # Remove extra spaces
        line = re.sub(r'\s+', ' ', line)
        cleaned_lines.append(line)
    
    # Join lines and clean up formatting
    cleaned_text = '\n'.join(cleaned_lines)
    
    # Fix common PDF extraction issues
    cleaned_text = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_text)  # Missing spaces
    cleaned_text = re.sub(r'(\w)\n(\w)', r'\1 \2', cleaned_text)  # Join broken words
    
    return cleaned_text.strip()

def get_pdf_text_enhanced(pdf_url: str) -> Optional[str]:
    """Enhanced PDF download and extraction with multiple methods and caching."""
    logging.info(f"📄 Processing PDF: {pdf_url}")
    
    # Create cache filename
    pdf_filename = hashlib.md5(pdf_url.encode()).hexdigest() + ".pdf"
    cached_pdf_path = PDF_CACHE_DIR / pdf_filename
    cached_text_path = PDF_CACHE_DIR / (pdf_filename + ".txt")
    
    # Check if we have cached text
    if cached_text_path.exists():
        logging.info("📄 Using cached PDF content")
        with open(cached_text_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    try:
        # Download PDF if not cached
        if not cached_pdf_path.exists():
            with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
                response = client.get(pdf_url, headers={'User-Agent': USER_AGENT})
                response.raise_for_status()
            
            with open(cached_pdf_path, 'wb') as f:
                f.write(response.content)
            logging.info(f"📄 Downloaded PDF: {pdf_filename}")
        
        # Method 1: Try pdfplumber (best for structured content)
        text_content = ""
        try:
            with pdfplumber.open(cached_pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_content += page_text + "\n"
                    except Exception as e:
                        logging.warning(f"Error extracting page {page_num + 1} with pdfplumber: {e}")
                        continue
            
            if text_content:
                logging.info(f"✅ Extracted text using pdfplumber: {len(text_content)} chars")
        except Exception as e:
            logging.warning(f"pdfplumber failed: {e}")
        
        # Method 2: Fallback to PyMuPDF if pdfplumber failed
        if not text_content:
            try:
                with fitz.open(cached_pdf_path) as doc:
                    for page in doc:
                        text_content += page.get_text() + "\n"
                
                if text_content:
                    logging.info(f"✅ Extracted text using PyMuPDF: {len(text_content)} chars")
            except Exception as e:
                logging.warning(f"PyMuPDF failed: {e}")
        
        # Method 3: Final fallback to PyPDF2
        if not text_content:
            try:
                with open(cached_pdf_path, 'rb') as f:
                    pdf_reader = PyPDF2.PdfReader(f)
                    for page in pdf_reader.pages:
                        try:
                            text_content += page.extract_text() + "\n"
                        except Exception as e:
                            logging.warning(f"Error with PyPDF2 page: {e}")
                            continue
                
                if text_content:
                    logging.info(f"✅ Extracted text using PyPDF2: {len(text_content)} chars")
            except Exception as e:
                logging.warning(f"PyPDF2 failed: {e}")
        
        # Clean and cache the extracted text
        if text_content:
            cleaned_text = clean_pdf_text(text_content)
            
            # Cache the cleaned text
            with open(cached_text_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_text)
            
            logging.info(f"📄 Successfully processed PDF: {len(cleaned_text)} characters")
            return cleaned_text
        else:
            logging.warning("📄 No text content extracted from PDF")
            return None
            
    except Exception as e:
        logging.error(f"Failed to process PDF {pdf_url}: {e}")
        return None

# --- ENHANCED CONTENT PARSING ---

def parse_article_page_enhanced(page: Page, article_url: str) -> Dict:
    """Enhanced article page parsing with better content extraction."""
    result = {'content': '', 'related_links': [], 'pdf_links': [], 'content_type': 'unknown'}
    
    try:
        page.goto(article_url, timeout=REQUEST_TIMEOUT * 1000)
        soup = BeautifulSoup(page.content(), 'html.parser')
        
        # Log page title for debugging
        title = soup.find('title')
        if title:
            logging.info(f"📄 Page title: {title.get_text().strip()}")
        
    except Exception as e:
        logging.error(f"Error loading article {article_url}: {e}")
        return result

    # Enhanced content area detection
    content_selectors = [
        'article .node__content .field--name-body',
        'div.clearfix.text-formatted.field.field--name-body.field--type-text-with-summary.field--label-hidden.field__item',
        '.field--name-body .field__item',
        'article .node__content',
        '.text-formatted'
    ]
    
    content_area = None
    for selector in content_selectors:
        content_area = soup.select_one(selector)
        if content_area:
            logging.info(f"✅ Content found with selector: {selector}")
            break
    
    if not content_area:
        logging.warning(f"No content area found on {article_url}")
        return result

    # Extract PDF links first
    pdf_links = []
    for link in content_area.find_all('a', href=True):
        href = link.get('href', '')
        if href.lower().endswith('.pdf'):
            pdf_full_url = urljoin(BASE_URL, href)
            pdf_links.append(pdf_full_url)
            logging.info(f"📎 Found PDF: {pdf_full_url}")
    
    result['pdf_links'] = pdf_links
    
    # Extract PDF content
    pdf_content = ""
    if pdf_links:
        logging.info(f"📎 Processing {len(pdf_links)} PDF(s)")
        pdf_texts = []
        
        for pdf_url in pdf_links:
            pdf_text = get_pdf_text_enhanced(pdf_url)
            if pdf_text:
                pdf_texts.append(pdf_text)
        
        if pdf_texts:
            pdf_content = "\n\n".join(pdf_texts)
            result['content_type'] = 'pdf_primary'

    # Extract HTML content
    html_content = ""
    if content_area:
        # Remove unwanted elements
        for unwanted in content_area.find_all(['script', 'style', 'nav', 'header', 'footer']):
            unwanted.decompose()
        
        # Remove anchor links
        for anchor in content_area.find_all('a', class_='anchor'):
            anchor.decompose()
        
        # Remove external link icons
        for icon in content_area.find_all('i', class_='fa'):
            icon.decompose()
        
        html_content = clean_text(content_area.get_text(separator=' '))
        
        if html_content and result['content_type'] == 'unknown':
            result['content_type'] = 'html_primary'
    
    # Format content for LLM
    if not html_content and pdf_content:
        html_content = "Content is primarily available in attached PDF documents."
    
    result['content'] = format_content_for_llm(html_content, pdf_content)

    # Extract relevant links (excluding PDFs)
    links = set()
    for link in content_area.select('a[href]'):
        href = link['href']
        if (href and not href.startswith('#') and 
            href.startswith(('http', '/')) and 
            not href.lower().endswith('.pdf')):
            
            full_link = urljoin(BASE_URL, href)
            # Exclude certain file types
            if not any(ext in full_link.lower() for ext in ['.xlsx', '.csv', '.mp3', '.wav', '.xls']):
                links.add(full_link)
    
    result['related_links'] = sorted(list(links))
    
    logging.info(f"✅ Extracted content: {len(result['content'])} chars, {len(result['related_links'])} links, {len(pdf_links)} PDFs")
    
    return result

# --- ENHANCED SCRAPING FUNCTIONS ---

def scrape_site_enhanced(context: BrowserContext, existing_urls: Set[str], run_type: str = "daily") -> List[Dict]:
    """Enhanced main scraping function with run type support."""
    newly_scraped_articles = []
    page = context.new_page()
    
    # Configure run parameters
    if run_type == "initial":
        max_pages_default = 15
        days_back = 365
    else:
        max_pages_default = 3
        days_back = 7
    
    logging.info(f"🚀 Starting {run_type.upper()} run (checking {days_back} days back)")
    
    try:
        apply_stealth(page)
        page.goto(NEWS_URL, timeout=REQUEST_TIMEOUT * 1000)
    except Exception as e:
        logging.critical(f"Failed to load news page: {e}")
        return []

    # Determine pagination
    max_pages = max_pages_default
    try:
        last_page_href = page.locator('li.page-item a[title="Go to last page"]').get_attribute('href')
        if last_page_href:
            detected_max = int(re.search(r'page=(\d+)', last_page_href).group(1)) + 1
            if run_type == "initial":
                max_pages = detected_max
                logging.info(f"Initial run - scraping all {max_pages} pages.")
            else:
                max_pages = min(max_pages_default, detected_max)
                logging.info(f"Daily run - checking {max_pages} pages.")
    except Exception:
        logging.warning(f"Couldn't determine last page. Using default: {max_pages} pages.")

    # Process each listing page
    recent_articles_found = 0
    for page_num in range(max_pages):
        current_url = f"{NEWS_URL}?page={page_num}"
        logging.info(f"📄 Processing page {page_num + 1}/{max_pages}: {current_url}")
        
        if page_num > 0:
            try:
                page.goto(current_url, timeout=REQUEST_TIMEOUT * 1000)
            except Exception:
                logging.error(f"Timeout loading page {page_num}")
                continue

        soup = BeautifulSoup(page.content(), 'html.parser')
        article_blocks = soup.select('.view-content .views-row')

        if not article_blocks:
            logging.info(f"No articles found on page {page_num + 1}. Stopping.")
            break
            
        found_new = False
        page_recent_count = 0
        
        for block in article_blocks:
            link_tag = block.select_one('a')
            if not link_tag or not link_tag.get('href'):
                continue
            
            article_url = urljoin(BASE_URL, link_tag['href'])

            if article_url in existing_urls:
                logging.debug(f"Skipping duplicate: {article_url}")
                continue
            
            # Extract basic metadata
            try:
                headline = clean_text(block.select_one('p > strong').text) if block.select_one('p > strong') else "N/A"
                published_date = block.select_one('time[datetime]')['datetime'] if block.select_one('time[datetime]') else None
                
                # Check if article is recent enough for daily runs
                if not is_recent_article(published_date or "", run_type, days_back):
                    if run_type == "daily":
                        logging.debug(f"Skipping old article: {headline}")
                        continue
                
                recent_articles_found += 1
                page_recent_count += 1
                found_new = True
                
                logging.info(f"📰 Processing new article: {headline}")
                
                image_url = urljoin(BASE_URL, block.select_one('img')['src']) if block.select_one('img') and block.select_one('img').get('src') else None
                
                # Extract themes/topics
                theme_elements = block.select('span.pillDefault.pillTopic')
                themes = [clean_text(theme.text) for theme in theme_elements]
                
                # Extract audience
                audience_elements = block.select('span.pillDefault.pillAudience')
                audiences = [clean_text(aud.text) for aud in audience_elements]
                
                # Process article content
                article_page = context.new_page()
                apply_stealth(article_page)
                article_details = parse_article_page_enhanced(article_page, article_url)
                article_page.close()
                
                # Generate hash for deduplication
                hash_id = generate_hash(article_url, headline, published_date or "")
                
                # Determine category
                category = extract_category(headline, ' '.join(themes))
                
                # Compile enhanced article data
                article_data = {
                    "hash_id": hash_id,
                    "headline": headline,
                    "published_date": published_date,
                    "scraped_date": datetime.now(timezone.utc).isoformat(),
                    "category": category,
                    "themes": themes,
                    "audience": audiences,
                    "image_url": image_url,
                    "source_url": article_url,
                    "run_type": run_type,
                    "content_type": article_details.get('content_type', 'unknown'),
                    "pdf_count": len(article_details.get('pdf_links', [])),
                    **article_details
                }
                
                newly_scraped_articles.append(article_data)
                existing_urls.add(article_url)

            except Exception as e:
                logging.error(f"Error processing article {article_url}: {e}")

        logging.info(f"📊 Page {page_num + 1}: Found {page_recent_count} recent articles")

        # Early termination for daily runs if no recent articles
        if run_type == "daily" and not found_new and page_num > 0:
            logging.info("No new recent articles found. Ending daily run early.")
            break

    page.close()
    logging.info(f"✅ Scraping completed: {len(newly_scraped_articles)} new articles, {recent_articles_found} recent articles total")
    return newly_scraped_articles

def apply_stealth(page):
    """Apply stealth measures if available."""
    pass

def save_enhanced_data(all_articles: List[Dict]):
    """Save enhanced data with metadata to JSON and CSV files."""
    if not all_articles:
        logging.info("No articles to save.")
        return

    logging.info(f"💾 Saving {len(all_articles)} articles...")
    
    # Sort by published date (newest first)
    all_articles.sort(key=lambda x: x.get('published_date', ''), reverse=True)

    # Enhanced metadata
    save_data = {
        'metadata': {
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'total_articles': len(all_articles),
            'scraper_version': '2.0_enhanced'
        },
        'articles': all_articles
    }

    # Save enhanced JSON with metadata
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, indent=2, ensure_ascii=False)

    # Create enhanced DataFrame
    df = pd.DataFrame(all_articles)
    column_order = [
        'hash_id', 'headline', 'published_date', 'category', 'themes', 'audience',
        'content', 'content_type', 'pdf_count', 'source_url', 'image_url', 
        'related_links', 'pdf_links', 'run_type', 'scraped_date'
    ]
    
    # Ensure all columns exist
    for col in column_order:
        if col not in df.columns:
            df[col] = None
    
    # Convert lists to strings for CSV
    for col in ['themes', 'audience', 'related_links', 'pdf_links']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: '; '.join(x) if isinstance(x, list) else str(x))
    
    df = df[column_order]
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    
    logging.info(f"💾 Data saved to {OUTPUT_JSON} and {OUTPUT_CSV}")
    
    # Log summary statistics
    categories = df['category'].value_counts().to_dict() if 'category' in df.columns else {}
    pdf_articles = len(df[df['pdf_count'] > 0]) if 'pdf_count' in df.columns else 0
    
    logging.info("="*80)
    logging.info("📊 SUMMARY STATISTICS:")
    logging.info(f"   Total articles: {len(all_articles)}")
    logging.info(f"   Articles with PDFs: {pdf_articles}")
    logging.info(f"   Categories: {dict(categories)}")
    logging.info("="*80)

# --- MAIN EXECUTION ---

def main():
    """Enhanced main execution function with argument parsing."""
    parser = argparse.ArgumentParser(description='Enhanced Department of Finance News Scraper')
    parser.add_argument('--run-type', choices=['daily', 'initial'], default='daily',
                        help='Type of run: daily (recent articles) or initial (comprehensive)')
    parser.add_argument('--headless', action='store_true', default=True,
                        help='Run browser in headless mode')
    parser.add_argument('--max-pages', type=int, help='Override maximum pages to scrape')
    
    args = parser.parse_args()
    
    setup_logging()
    existing_urls = get_existing_urls(OUTPUT_JSON)
    new_articles = []
    
    # Auto-detect initial run if no existing data
    if not existing_urls and args.run_type == 'daily':
        logging.info("No existing data found, switching to initial run")
        args.run_type = 'initial'

    try:
        with sync_playwright() as p:
            # Enhanced browser launch with stealth options
            browser = p.chromium.launch(
                headless=args.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-infobars',
                    '--no-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
            
            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={'width': 1920, 'height': 1080}
            )
            
            new_articles = scrape_site_enhanced(context, existing_urls, args.run_type)
            
            # Clean up
            context.close()
            browser.close()

    except Exception as e:
        logging.critical(f"Scraping failed: {e}")
        import traceback
        logging.error(traceback.format_exc())

    # Save results
    if new_articles:
        logging.info(f"✅ Scraped {len(new_articles)} new articles.")
        
        # Load existing data and merge
        if OUTPUT_JSON.exists():
            with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            
            # Handle both old and new format
            if isinstance(existing_data, dict) and 'articles' in existing_data:
                all_articles = existing_data['articles'] + new_articles
            else:
                all_articles = existing_data + new_articles
        else:
            all_articles = new_articles
            
        save_enhanced_data(all_articles)
    else:
        logging.info("ℹ️  No new articles found.")
        
    logging.info("="*80)
    logging.info("🏁 Enhanced scraper execution finished")
    logging.info("="*80)

if __name__ == "__main__":
    main()