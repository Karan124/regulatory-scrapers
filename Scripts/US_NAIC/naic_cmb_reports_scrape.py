"""
NAIC Capital Markets Bureau Scraper
Production-grade scraper for NAIC Capital Markets Bureau PDF reports.
Extracts PDFs with OCR for charts, graphs, and images.
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
from selenium_stealth import stealth
import PyPDF2
import pdfplumber
from PIL import Image
import pytesseract
import io
import re
import fitz  # PyMuPDF for better image extraction


# ==================== CONFIGURATION ====================
BASE_URL = "https://content.naic.org"
CMB_URL = f"{BASE_URL}/capital-markets-bureau"
DATA_DIR = Path("./data")
OUTPUT_FILE = DATA_DIR / "naic_cmb_reports.json"
MAX_PAGES = 1  # Configure number of pages to scrape
DELAY_MIN = 2  # Minimum delay between requests (seconds)
DELAY_MAX = 5  # Maximum delay between requests (seconds)

# Tesseract configuration for better OCR
TESSERACT_CONFIG = '--oem 3 --psm 6'  # LSTM OCR, assume uniform block of text


# ==================== UTILITY FUNCTIONS ====================
def create_session() -> requests.Session:
    """Create a requests session with browser-like headers."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': BASE_URL,
        'DNT': '1',
    })
    return session


def random_delay():
    """Sleep for a random duration to mimic human behavior."""
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


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


# ==================== ENHANCED PDF EXTRACTION WITH OCR ====================
def extract_images_from_pdf(pdf_content: bytes) -> List[Image.Image]:
    """Extract all images from PDF using PyMuPDF."""
    images = []
    
    try:
        # Open PDF with PyMuPDF
        pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
        
        for page_num in range(len(pdf_document)):
            page = pdf_document[page_num]
            
            # Get images from page
            image_list = page.get_images(full=True)
            
            for img_index, img in enumerate(image_list):
                xref = img[0]
                
                try:
                    # Extract image
                    base_image = pdf_document.extract_image(xref)
                    image_bytes = base_image["image"]
                    
                    # Convert to PIL Image
                    image = Image.open(io.BytesIO(image_bytes))
                    
                    # Only process reasonably sized images (likely to contain charts/text)
                    if image.width > 100 and image.height > 100:
                        images.append(image)
                
                except Exception as e:
                    print(f"    Error extracting image {img_index} from page {page_num}: {e}")
                    continue
        
        pdf_document.close()
    
    except Exception as e:
        print(f"    Error extracting images from PDF: {e}")
    
    return images


def ocr_image(image: Image.Image) -> str:
    """Perform OCR on an image to extract text."""
    try:
        # Convert to grayscale for better OCR
        if image.mode != 'L':
            image = image.convert('L')
        
        # Enhance contrast
        from PIL import ImageEnhance
        enhancer = ImageEnhance.Contrast(image)
        image = enhancer.enhance(2)
        
        # Perform OCR
        text = pytesseract.image_to_string(image, config=TESSERACT_CONFIG)
        
        return clean_text(text)
    
    except Exception as e:
        print(f"    Error performing OCR: {e}")
        return ""


def extract_text_from_pdf_with_ocr(pdf_content: bytes) -> str:
    """Extract text from PDF including OCR for images, charts, and graphs."""
    text_parts = []
    
    print("    Extracting text from PDF...")
    
    try:
        # First, extract regular text using pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                print(f"      Processing page {page_num}/{len(pdf.pages)}...")
                
                # Extract text
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(f"[PAGE {page_num}]\n{page_text}")
                
                # Extract tables separately for better structure
                tables = page.extract_tables()
                for table_num, table in enumerate(tables, 1):
                    if table:
                        # Convert table to text format
                        table_text = '\n'.join(['\t'.join([str(cell) if cell else '' for cell in row]) for row in table])
                        text_parts.append(f"\n[TABLE {table_num} - PAGE {page_num}]\n{table_text}\n")
    
    except Exception as e:
        print(f"    Error extracting text with pdfplumber: {e}")
        
        # Fallback to PyPDF2
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            for page_num, page in enumerate(pdf_reader.pages, 1):
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(f"[PAGE {page_num}]\n{page_text}")
        except Exception as e2:
            print(f"    Error with PyPDF2 fallback: {e2}")
    
    # Now extract and OCR images
    print("    Extracting images for OCR...")
    images = extract_images_from_pdf(pdf_content)
    
    if images:
        print(f"    Found {len(images)} images. Performing OCR...")
        
        for img_num, image in enumerate(images, 1):
            print(f"      OCR on image {img_num}/{len(images)}...")
            ocr_text = ocr_image(image)
            
            if ocr_text and len(ocr_text.strip()) > 20:  # Only include if meaningful text found
                text_parts.append(f"\n[IMAGE/CHART {img_num} - OCR TEXT]\n{ocr_text}\n")
    
    combined_text = clean_text('\n\n'.join(text_parts))
    print(f"    Extracted {len(combined_text)} characters total")
    
    return combined_text


# ==================== SCRAPING FUNCTIONS ====================
def scrape_report_metadata(driver: webdriver.Chrome, page_num: int) -> List[Dict[str, str]]:
    """Scrape report metadata from index page."""
    reports_metadata = []
    
    try:
        # Build URL with page parameter
        page_param = page_num - 1
        
        # Use the search parameter from the pagination links
        search_params = '%22990%22%20%22560%22%20%22561%22%20%22554%22'
        
        if page_num == 1:
            url = CMB_URL
        else:
            url = f"{CMB_URL}?search_api_fulltext={search_params}&page={page_param}"
        
        print(f"Loading URL: {url}")
        driver.get(url)
        
        # Wait for reports grid to load
        wait = WebDriverWait(driver, 15)
        
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.grid-3, div[class*='grid']")))
            time.sleep(2)  # Additional wait for content
        except Exception as e:
            print(f"Timeout waiting for reports grid: {e}")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find all report cards
        reports = soup.find_all('div', class_='relative')
        
        if not reports:
            reports = soup.find_all('div', class_=lambda x: x and 'shadow-normal' in x)
        
        for report in reports:
            link_tag = report.find('a', href=True)
            
            # Check if it's a PDF link
            if link_tag and '.pdf' in link_tag['href']:
                pdf_url = urljoin(BASE_URL, link_tag['href'])
                
                # Extract report type (e.g., "Special Reports", "Hot Spot", etc.)
                report_type = "Unknown"
                type_tag = link_tag.find('p', class_=lambda x: x and 'text-base' in x and 'color-blue' in x)
                if type_tag:
                    report_type = clean_text(type_tag.get_text())
                
                # Extract title
                title = "Unknown"
                title_tag = link_tag.find('h3', class_=lambda x: x and 'text-xl' in x)
                if title_tag:
                    title = clean_text(title_tag.get_text())
                
                # Extract date
                date = "Unknown"
                date_tag = link_tag.find('p', class_=lambda x: x and 'text-sm' in x and 'color-grey' in x)
                if date_tag:
                    date = clean_text(date_tag.get_text())
                
                metadata = {
                    'pdf_url': pdf_url,
                    'report_type': report_type,
                    'title': title,
                    'date': date
                }
                
                reports_metadata.append(metadata)
        
        print(f"Found {len(reports_metadata)} reports on page {page_num}")
    
    except Exception as e:
        print(f"Error scraping page {page_num}: {e}")
        import traceback
        traceback.print_exc()
    
    return reports_metadata


def scrape_pdf_report(session: requests.Session, metadata: Dict[str, str]) -> Optional[Dict]:
    """Download and extract content from a PDF report."""
    pdf_url = metadata['pdf_url']
    
    try:
        print(f"  Downloading PDF: {pdf_url}")
        
        # Download PDF
        response = session.get(pdf_url, timeout=120)  # Longer timeout for PDFs
        response.raise_for_status()
        
        # Extract text with OCR
        pdf_content = extract_text_from_pdf_with_ocr(response.content)
        
        if not pdf_content or len(pdf_content.strip()) < 100:
            print(f"  ⚠ Warning: Extracted content seems too short ({len(pdf_content)} chars)")
        
        report_data = {
            'title': metadata['title'],
            'report_type': metadata['report_type'],
            'published_date': metadata['date'],
            'scraped_date': datetime.now().isoformat(),
            'pdf_url': pdf_url,
            'report_text': pdf_content
        }
        
        print(f"✓ Scraped: {metadata['title'][:60]}...")
        return report_data
    
    except Exception as e:
        print(f"✗ Error scraping PDF {pdf_url}: {e}")
        import traceback
        traceback.print_exc()
        return None


# ==================== DEDUPLICATION & PERSISTENCE ====================
def load_existing_data() -> Dict[str, Dict]:
    """Load existing scraped reports from JSON file."""
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                reports_list = json.load(f)
                # Convert to dict with PDF URL as key
                return {report['pdf_url']: report for report in reports_list}
        except Exception as e:
            print(f"Error loading existing data: {e}")
    
    return {}


def save_data(reports: Dict[str, Dict]):
    """Save reports to JSON file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Convert dict back to list
    reports_list = list(reports.values())
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(reports_list, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Saved {len(reports_list)} reports to {OUTPUT_FILE}")


# ==================== MAIN SCRAPER ====================
def main():
    """Main scraper function."""
    print("=" * 70)
    print("NAIC Capital Markets Bureau Scraper")
    print("=" * 70)
    print(f"Target: {CMB_URL}")
    print(f"Max pages: {MAX_PAGES}")
    print(f"Output: {OUTPUT_FILE}")
    print("=" * 70)
    
    # Check if Tesseract is available
    try:
        pytesseract.get_tesseract_version()
        print("✓ Tesseract OCR detected")
    except Exception:
        print("⚠ Warning: Tesseract not found. OCR will not work.")
        print("  Install: sudo apt-get install tesseract-ocr (Linux)")
        print("  Install: brew install tesseract (macOS)")
        print("  Install: Download from https://github.com/UB-Mannheim/tesseract/wiki (Windows)")
    
    # Load existing data
    existing_reports = load_existing_data()
    print(f"\nLoaded {len(existing_reports)} existing reports")
    
    driver = None
    try:
        driver = create_driver()
        session = create_session()
        
        # Scrape report metadata from all pages
        all_reports_metadata = []
        for page_num in range(1, MAX_PAGES + 1):
            print(f"\n--- Scraping page {page_num} ---")
            reports_metadata = scrape_report_metadata(driver, page_num)
            
            if not reports_metadata:
                print(f"No reports found on page {page_num}. Stopping.")
                break
            
            all_reports_metadata.extend(reports_metadata)
            random_delay()
        
        # Remove duplicates
        seen_urls = set()
        unique_metadata = []
        for meta in all_reports_metadata:
            if meta['pdf_url'] not in seen_urls:
                seen_urls.add(meta['pdf_url'])
                unique_metadata.append(meta)
        
        print(f"\n✓ Total unique reports found: {len(unique_metadata)}")
        
        # Filter out already scraped reports
        new_reports_metadata = [meta for meta in unique_metadata if meta['pdf_url'] not in existing_reports]
        print(f"✓ New reports to scrape: {len(new_reports_metadata)}")
        
        # Scrape new reports
        if new_reports_metadata:
            print("\n--- Scraping PDF reports ---")
            for i, metadata in enumerate(new_reports_metadata, 1):
                print(f"\n[{i}/{len(new_reports_metadata)}]")
                report_data = scrape_pdf_report(session, metadata)
                
                if report_data:
                    existing_reports[metadata['pdf_url']] = report_data
                
                random_delay()
            
            # Save updated data
            save_data(existing_reports)
        else:
            print("\n✓ No new reports to scrape. Dataset is up to date.")
    
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