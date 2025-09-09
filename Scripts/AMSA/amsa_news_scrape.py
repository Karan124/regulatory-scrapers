import os
import json
import logging
import hashlib
from datetime import datetime
import re
import requests
from bs4 import BeautifulSoup
import time
from io import BytesIO

try:
    import pdfplumber
except ImportError:
    print("pdfplumber is not installed. Please install it using 'pip install pdfplumber'")
    exit()

# --- Configuration ---
BASE_URL = "https://www.amsa.gov.au"
NEWS_URL = f"{BASE_URL}/news-community/news-and-media-releases"
MAX_PAGE = 2
DATA_DIR = "data"
LOGS_DIR = "logs"
DATA_FILE = os.path.join(DATA_DIR, "amsa_news.json")

# --- Setup ---

def setup_directories():
    for directory in [DATA_DIR, LOGS_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)

def setup_logging():
    log_filename = os.path.join(LOGS_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()]
    )

def get_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    })
    try:
        session.get(BASE_URL, timeout=10)
        logging.info("Session initialized and cookies gathered.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to initialize session: {e}")
    return session

def load_existing_data():
    if not os.path.exists(DATA_FILE):
        return [], set()
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        processed_urls = {item.get('url') for item in data}
        logging.info(f"Loaded {len(processed_urls)} existing articles.")
        return data, processed_urls
    except (json.JSONDecodeError, FileNotFoundError):
        logging.warning("Could not read existing data file. Starting fresh.")
        return [], set()

# --- Content Extraction and Cleaning ---

def clean_text(text):
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

def extract_pdf_content(pdf_url, session):
    try:
        logging.info(f"Processing PDF with pdfplumber: {pdf_url}")
        response = session.get(pdf_url, timeout=30)
        response.raise_for_status()
        
        pdf_bytes = response.content
        if not pdf_bytes:
            logging.error(f"PDF download was empty for {pdf_url}")
            return None

        with BytesIO(pdf_bytes) as pdf_file:
            text_content = ""
            with pdfplumber.open(pdf_file) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_content += page_text + "\n"
        
        logging.info(f"Successfully extracted text with pdfplumber from: {pdf_url}")
        return clean_text(text_content)
        
    except Exception as e:
        logging.error(f"Failed to process PDF {pdf_url} with pdfplumber: {e}", exc_info=True)
        return None

def find_published_date(content_area):
    """Try multiple strategies to find the published date."""
    # Strategy 1: Original selector
    date_element = content_area.find('div', class_='mb-0 text-base')
    if date_element:
        return clean_text(date_element.text)
    
    # Strategy 2: Look for other common date classes
    date_selectors = [
        {'class_': 'text-base'},
        {'class_': 'date'},
        {'class_': 'published'},
        {'class_': 'publish-date'},
        {'class_': 'article-date'},
        {'class_': re.compile(r'.*date.*', re.IGNORECASE)},
        {'class_': re.compile(r'.*publish.*', re.IGNORECASE)}
    ]
    
    for selector in date_selectors:
        date_element = content_area.find('div', selector)
        if date_element:
            return clean_text(date_element.text)
    
    # Strategy 3: Look for date patterns in any text
    all_text = content_area.get_text()
    date_patterns = [
        r'\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}',
        r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
        r'\d{1,2}/\d{1,2}/\d{4}',
        r'\d{4}-\d{2}-\d{2}'
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, all_text, re.IGNORECASE)
        if match:
            return match.group()
    
    logging.warning("Could not find published date using any strategy")
    return "Date not found"

# --- Scraping Logic ---

def scrape_article_page(article_url, session):
    """Scrapes a single news article, capturing both web and PDF content if available."""
    try:
        response = session.get(article_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract title with fallback
        title_element = soup.find('h1', class_='page-title')
        if not title_element:
            title_element = soup.find('h1')  # Fallback to any h1
        if not title_element:
            logging.warning(f"Could not find title for {article_url}")
            return None
        title = clean_text(title_element.text)
        
        # Find content area with fallback
        content_area = soup.find('div', class_='node__content')
        if not content_area:
            content_area = soup.find('div', class_='content')  # Fallback
        if not content_area:
            content_area = soup.find('main')  # Another fallback
        if not content_area:
            logging.warning(f"Could not find main content area for {article_url}")
            return None
            
        # Extract published date - simple null check
        date_element = content_area.find('div', class_='mb-0 text-base')
        if date_element:
            published_date = clean_text(date_element.text)
        else:
            logging.warning(f"Date element 'mb-0 text-base' not found for {article_url}")
            published_date = "Date not found"
        
        # Extract image with fallback
        image_tag = content_area.find('img')
        image_url = "N/A"
        if image_tag and image_tag.get('src'):
            src = image_tag['src']
            image_url = f"{BASE_URL}{src}" if src.startswith('/') else src

        # Find main text area with fallbacks
        main_text_area = content_area.find('div', class_='field--name-body')
        if not main_text_area:
            main_text_area = content_area.find('div', class_='body')
        if not main_text_area:
            main_text_area = content_area.find('div', class_='content-body')
        if not main_text_area:
            # Last resort: use the entire content area
            main_text_area = content_area
            logging.info(f"Using entire content area as fallback for main text in {article_url}")

        if not main_text_area:
            logging.warning(f"Main text area not found in {article_url}")
            return None

        # *** ALWAYS capture web content ***
        web_content = clean_text(main_text_area.get_text(separator='\n', strip=True))
        logging.info(f"Extracted web content for: {title}")

        # *** ALSO capture PDF content if available ***
        pdf_content = None
        pdf_link_tag = main_text_area.find('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
        if pdf_link_tag and pdf_link_tag.get('href'):
            logging.info("PDF link found, extracting PDF content as well.")
            pdf_url = pdf_link_tag['href']
            if not pdf_url.startswith('http'):
                pdf_url = f"{BASE_URL}{pdf_url}"
            pdf_content = extract_pdf_content(pdf_url, session)

        # Scrape links from the main text area ONLY
        links = []
        for a in main_text_area.find_all('a', href=True):
            href = a['href']
            if href.startswith('/'):
                href = f"{BASE_URL}{href}"
            links.append(href)

        article_data = {
            'title': title,
            'theme': "N/A",
            'published_date': published_date,
            'scraped_date': datetime.now().isoformat(),
            'web_content': web_content,    # Always populated
            'pdf_content': pdf_content,    # Populated if PDF exists
            'image_url': image_url,
            'links': list(set(links)),
            'url': article_url,
        }
        
        logging.info(f"Finished scraping: {title}")
        return article_data

    except Exception as e:
        logging.error(f"An error occurred while scraping {article_url}: {e}", exc_info=True)
        return None

def main():
    setup_logging()
    setup_directories()
    logging.info("Starting AMSA News Scraper")
    
    session = get_session()
    all_articles, processed_urls = load_existing_data()
    new_articles_count = 0

    for page_num in range(MAX_PAGE):
        page_url = f"{NEWS_URL}?page={page_num}"
        logging.info(f"Scraping listing page: {page_url}")
        try:
            response = session.get(page_url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            article_links = soup.select('.view-content .views-field-title a')
            if not article_links:
                logging.warning(f"No articles found on page {page_num + 1}. Last page reached.")
                break

            for link in article_links:
                if not link.has_attr('href'): 
                    continue
                
                article_url = f"{BASE_URL}{link['href']}"
                if article_url in processed_urls: 
                    continue

                article_data = scrape_article_page(article_url, session)
                if article_data:
                    all_articles.append(article_data)
                    processed_urls.add(article_url)
                    new_articles_count += 1
                time.sleep(0.5)

        except Exception as e:
            logging.error(f"An error occurred processing page {page_url}: {e}", exc_info=True)
            
    logging.info(f"Scraping complete. Found {new_articles_count} new articles.")

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_articles, f, indent=4, ensure_ascii=False)
    logging.info(f"Successfully saved {len(all_articles)} total articles to {DATA_FILE}")

if __name__ == "__main__":
    main()