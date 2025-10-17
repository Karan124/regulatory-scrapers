"""
OSFI Media Releases Scraper - Production Grade with Anti-Detection
Scrapes news, speeches, and other media releases from OSFI Canada
Uses Playwright with stealth mode for JavaScript rendering and bot detection avoidance
Extracts text content from webpages, PDFs, and Excel files for LLM analysis
"""

import json
import os
import time
import random
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
import hashlib

from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import pdfplumber
import pandas as pd
from io import BytesIO
import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_PAGE = 2  # Maximum number of pages to scrape
START_DATE = "2025-01-01"  # Only scrape news on or after this date (optional filter)
BASE_URL = "https://www.osfi-bsif.gc.ca"
NEWS_URL = f"{BASE_URL}/en/news"
OUTPUT_FILE = "data/osfi_news.json"
REQUEST_DELAY = (3, 7)  # Random delay between requests (min, max) in seconds
PAGE_LOAD_TIMEOUT = 60000  # Page load timeout in milliseconds
HEADLESS = True  # Set to False for debugging

# Browser fingerprinting settings
VIEWPORT = {'width': 1920, 'height': 1080}
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

async def random_delay():
    """Random delay between requests to mimic human behavior"""
    delay = random.uniform(*REQUEST_DELAY)
    await asyncio.sleep(delay)


async def random_mouse_movement(page: Page):
    """Simulate random mouse movements to appear more human-like"""
    try:
        for _ in range(random.randint(2, 4)):
            x = random.randint(100, 1800)
            y = random.randint(100, 900)
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
    except Exception:
        pass


async def scroll_page(page: Page):
    """Scroll page naturally to trigger lazy-loaded content"""
    try:
        # Get page height
        page_height = await page.evaluate('document.body.scrollHeight')
        viewport_height = VIEWPORT['height']
        
        # Scroll in chunks
        current_position = 0
        while current_position < page_height:
            scroll_amount = random.randint(300, 600)
            current_position += scroll_amount
            
            await page.evaluate(f'window.scrollTo(0, {current_position})')
            await asyncio.sleep(random.uniform(0.2, 0.5))
        
        # Scroll back to top
        await page.evaluate('window.scrollTo(0, 0)')
        await asyncio.sleep(random.uniform(0.3, 0.7))
    except Exception:
        pass


def clean_text(text: str) -> str:
    """Clean extracted text by removing excessive whitespace"""
    if not text:
        return ""
    
    # Remove multiple spaces and newlines
    text = ' '.join(text.split())
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse date string to YYYY-MM-DD format"""
    try:
        for fmt in ['%B %d, %Y', '%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%dT%H:%M:%SZ']:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        return date_str
    except Exception:
        return None


def should_scrape_article(published_date: str) -> bool:
    """Check if article should be scraped based on START_DATE filter"""
    if not START_DATE or not published_date:
        return True
    
    try:
        article_date = datetime.strptime(published_date, '%Y-%m-%d')
        filter_date = datetime.strptime(START_DATE, '%Y-%m-%d')
        return article_date >= filter_date
    except Exception:
        return True


def load_existing_data() -> List[Dict]:
    """Load existing scraped data to avoid duplicates"""
    if not os.path.exists(OUTPUT_FILE):
        return []
    
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load existing data: {e}")
        return []


def save_data(data: List[Dict]):
    """Save scraped data to JSON file"""
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def generate_content_hash(article: Dict) -> str:
    """Generate unique hash for article content"""
    content = f"{article.get('headline', '')}{article.get('published_date', '')}{article.get('url', '')}"
    return hashlib.md5(content.encode()).hexdigest()


def is_duplicate(article: Dict, existing_data: List[Dict]) -> bool:
    """Check if article already exists in dataset"""
    new_hash = generate_content_hash(article)
    
    for existing in existing_data:
        existing_hash = generate_content_hash(existing)
        if new_hash == existing_hash:
            return True
        
        # Fallback check
        if (article.get('url') == existing.get('url') or 
            (article.get('headline') == existing.get('headline') and 
             article.get('published_date') == existing.get('published_date'))):
            return True
    
    return False


# ============================================================================
# STEALTH BROWSER SETUP
# ============================================================================

async def create_stealth_context(browser: Browser) -> BrowserContext:
    """Create a browser context with stealth settings to avoid detection"""
    
    # Random user agent
    user_agent = random.choice(USER_AGENTS)
    
    # Create context with stealth settings
    context = await browser.new_context(
        viewport=VIEWPORT,
        user_agent=user_agent,
        locale='en-US',
        timezone_id='America/Toronto',
        permissions=['geolocation'],
        geolocation={'longitude': -79.3832, 'latitude': 43.6532},  # Toronto coordinates
        color_scheme='light',
        accept_downloads=True,
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Upgrade-Insecure-Requests': '1',
        }
    )
    
    # Add stealth scripts to avoid detection
    await context.add_init_script("""
        // Overwrite the navigator.webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        // Mock plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                {
                    0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                    description: "Portable Document Format",
                    filename: "internal-pdf-viewer",
                    length: 1,
                    name: "Chrome PDF Plugin"
                }
            ]
        });
        
        // Mock languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });
        
        // Chrome runtime
        window.chrome = {
            runtime: {}
        };
        
        // Permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
    """)
    
    return context


# ============================================================================
# CONTENT EXTRACTION FUNCTIONS
# ============================================================================

def extract_pdf_text(pdf_url: str) -> str:
    """Extract text content from PDF file"""
    try:
        # Use requests for PDF download (simpler than Playwright)
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'application/pdf,*/*'
        }
        
        response = requests.get(pdf_url, headers=headers, timeout=60)
        response.raise_for_status()
        
        pdf_file = BytesIO(response.content)
        text_content = []
        
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_content.append(text)
                
                # Extract tables if present
                tables = page.extract_tables()
                for table in tables:
                    table_text = '\n'.join([' | '.join([str(cell) for cell in row if cell]) for row in table])
                    text_content.append(table_text)
        
        return clean_text(' '.join(text_content))
    
    except Exception as e:
        print(f"  Error extracting PDF from {pdf_url}: {e}")
        return ""


def extract_excel_text(excel_url: str) -> str:
    """Extract text content from Excel/CSV files"""
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,text/csv,*/*'
        }
        
        response = requests.get(excel_url, headers=headers, timeout=60)
        response.raise_for_status()
        
        excel_file = BytesIO(response.content)
        
        # Try reading as Excel first
        try:
            df_dict = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')
            text_parts = []
            
            for sheet_name, sheet_df in df_dict.items():
                text_parts.append(f"Sheet: {sheet_name}")
                text_parts.append(sheet_df.to_string(index=False))
            
            return clean_text(' '.join(text_parts))
        
        except Exception:
            # Try as CSV
            excel_file.seek(0)
            df = pd.read_csv(excel_file)
            return clean_text(df.to_string(index=False))
    
    except Exception as e:
        print(f"  Error extracting Excel/CSV from {excel_url}: {e}")
        return ""


async def extract_article_links(page: Page) -> List[str]:
    """Extract article URLs from the news listing page"""
    try:
        # Wait for articles to load
        await page.wait_for_selector('article.news', timeout=30000)
        
        # Scroll to trigger any lazy loading
        await scroll_page(page)
        
        # Extract links
        article_links = await page.evaluate("""
            () => {
                const articles = document.querySelectorAll('article.news');
                const links = [];
                
                articles.forEach(article => {
                    const link = article.querySelector('a.title--link');
                    if (link && link.href) {
                        links.push(link.href);
                    }
                });
                
                return links;
            }
        """)
        
        return article_links
    
    except Exception as e:
        print(f"  Error extracting article links: {e}")
        return []


async def extract_article_content(url: str, page: Page) -> Optional[Dict]:
    """Extract content from a single article page"""
    try:
        # Navigate to article
        await page.goto(url, wait_until='networkidle', timeout=PAGE_LOAD_TIMEOUT)
        
        # Random mouse movement for stealth
        await random_mouse_movement(page)
        
        # Wait for content to load
        await page.wait_for_selector('article.news', timeout=10000)
        
        # Scroll page to load all content
        await scroll_page(page)
        
        article = {}
        article['url'] = url
        
        # Extract all content using JavaScript
        content_data = await page.evaluate("""
            () => {
                const data = {};
                
                // Headline
                const headline = document.querySelector('h1#wb-cont');
                data.headline = headline ? headline.innerText.trim() : '';
                
                // Date
                const dateElem = document.querySelector('time');
                data.date = dateElem ? dateElem.getAttribute('datetime') || dateElem.innerText : '';
                
                // Article type
                const typeElem = document.querySelector('p.news--date');
                data.type = typeElem ? typeElem.innerText.split('-')[0].trim() : '';
                
                // Main content
                const contentDiv = document.querySelector('div.field--name-body');
                if (contentDiv) {
                    // Remove script and style tags
                    const clone = contentDiv.cloneNode(true);
                    clone.querySelectorAll('script, style').forEach(el => el.remove());
                    data.content = clone.innerText.trim();
                } else {
                    data.content = '';
                }
                
                // Quick facts
                const quickFacts = document.querySelector('div.field--name-field-quick-facts');
                data.quickFacts = quickFacts ? quickFacts.innerText.trim() : '';
                
                // Related links
                const relatedLinks = [];
                const relatedSection = document.querySelector('aside.field--name-field-related-links');
                if (relatedSection) {
                    relatedSection.querySelectorAll('a').forEach(link => {
                        const href = link.href;
                        // Exclude social media
                        if (!href.match(/facebook|twitter|linkedin|instagram|youtube/i)) {
                            relatedLinks.push(href);
                        }
                    });
                }
                data.relatedLinks = relatedLinks;
                
                // Find all document links (PDFs, Excel)
                const docLinks = [];
                document.querySelectorAll('a[href]').forEach(link => {
                    const href = link.href;
                    if (href.match(/\.(pdf|xlsx?|csv)$/i)) {
                        docLinks.push(href);
                    }
                });
                data.docLinks = docLinks;
                
                return data;
            }
        """)
        
        # Process extracted data
        article['headline'] = clean_text(content_data.get('headline', ''))
        article['published_date'] = parse_date(content_data.get('date', ''))
        article['article_type'] = clean_text(content_data.get('type', ''))
        article['content_text'] = clean_text(content_data.get('content', ''))
        
        # Add quick facts to content if present
        if content_data.get('quickFacts'):
            article['content_text'] += ' ' + clean_text(content_data.get('quickFacts', ''))
        
        article['related_links'] = content_data.get('relatedLinks', [])
        
        # Extract attachments
        pdf_texts = []
        excel_texts = []
        
        doc_links = content_data.get('docLinks', [])
        processed_urls = set()
        
        for doc_url in doc_links:
            # Avoid duplicates
            if doc_url in processed_urls:
                continue
            processed_urls.add(doc_url)
            
            parsed = urlparse(doc_url)
            path = parsed.path.lower()
            
            if path.endswith('.pdf'):
                print(f"    Extracting PDF: {doc_url}")
                pdf_text = extract_pdf_text(doc_url)
                if pdf_text:
                    pdf_texts.append(pdf_text)
                await random_delay()
            
            elif path.endswith(('.xlsx', '.xls', '.csv')):
                print(f"    Extracting Excel/CSV: {doc_url}")
                excel_text = extract_excel_text(doc_url)
                if excel_text:
                    excel_texts.append(excel_text)
                await random_delay()
        
        article['pdf_text'] = ' | '.join(pdf_texts) if pdf_texts else ""
        article['excel_text'] = ' | '.join(excel_texts) if excel_texts else ""
        article['scraped_date'] = datetime.now().strftime('%Y-%m-%d')
        
        return article
    
    except Exception as e:
        print(f"  Error extracting article from {url}: {e}")
        return None


# ============================================================================
# MAIN SCRAPING FUNCTION
# ============================================================================

async def scrape_osfi_news():
    """Main function to scrape OSFI news releases"""
    print("=" * 80)
    print("OSFI Media Releases Scraper - Production Grade")
    print("Using Playwright with stealth mode")
    print("=" * 80)
    
    # Load existing data
    existing_data = load_existing_data()
    print(f"\nLoaded {len(existing_data)} existing articles")
    
    new_articles = []
    total_processed = 0
    
    # Launch browser with stealth settings
    async with async_playwright() as p:
        print("\nLaunching browser with anti-detection measures...")
        
        # Launch Chromium with specific args to avoid detection
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu',
                '--window-size=1920,1080',
            ]
        )
        
        # Create stealth context
        context = await create_stealth_context(browser)
        page = await context.new_page()
        
        # Visit base URL first to collect cookies and establish session
        print("Establishing session...")
        try:
            await page.goto(BASE_URL, wait_until='networkidle', timeout=PAGE_LOAD_TIMEOUT)
            await random_delay()
        except Exception as e:
            print(f"Warning: Could not load base URL: {e}")
        
        # Iterate through pages
        for page_num in range(MAX_PAGE):
            print(f"\n--- Scraping Page {page_num + 1}/{MAX_PAGE} ---")
            
            # Construct URL for current page
            page_url = f"{NEWS_URL}?search=&type=All&year=&field_topics=All&field_speakers=&items_per_page=5&page={page_num}"
            
            try:
                # Navigate to listing page
                await page.goto(page_url, wait_until='networkidle', timeout=PAGE_LOAD_TIMEOUT)
                await random_delay()
                
                # Extract article links
                article_links = await extract_article_links(page)
                print(f"Found {len(article_links)} articles on this page")
                
                if not article_links:
                    print("No more articles found. Stopping.")
                    break
                
                # Process each article
                for idx, article_url in enumerate(article_links, 1):
                    print(f"\n  Processing article {idx}/{len(article_links)}:")
                    print(f"  URL: {article_url}")
                    
                    # Extract article content
                    article_data = await extract_article_content(article_url, page)
                    
                    if article_data:
                        # Check date filter
                        if not should_scrape_article(article_data.get('published_date')):
                            print(f"    Skipping: Published before {START_DATE}")
                            continue
                        
                        # Check for duplicates
                        if is_duplicate(article_data, existing_data + new_articles):
                            print("    Skipping: Duplicate article")
                            continue
                        
                        new_articles.append(article_data)
                        print(f"    ✓ Successfully extracted: {article_data['headline'][:60]}...")
                    
                    total_processed += 1
                    await random_delay()
                
                # Delay between pages
                await random_delay()
            
            except Exception as e:
                print(f"Error scraping page {page_num}: {e}")
                continue
        
        # Close browser
        await context.close()
        await browser.close()
    
    # Save results
    if new_articles:
        combined_data = existing_data + new_articles
        save_data(combined_data)
        print(f"\n{'=' * 80}")
        print(f"Scraping completed successfully!")
        print(f"New articles scraped: {len(new_articles)}")
        print(f"Total articles in database: {len(combined_data)}")
        print(f"Data saved to: {OUTPUT_FILE}")
        print(f"{'=' * 80}")
    else:
        print(f"\n{'=' * 80}")
        print("No new articles found.")
        print(f"Total articles in database: {len(existing_data)}")
        print(f"{'=' * 80}")


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Entry point with async execution"""
    try:
        asyncio.run(scrape_osfi_news())
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user.")
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        raise


if __name__ == "__main__":
    main()