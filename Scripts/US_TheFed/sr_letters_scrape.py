"""
Federal Reserve Supervision and Regulation (SR) Letters Scraper
Scrapes SR Letters from the last 2 years for LLM analysis.

Usage:
    python3 sr_letters_scrape.py                # Scrape last 2 years
    python3 sr_letters_scrape.py --debug        # Verbose output
"""

import os
import json
import time
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin
import re
import io

from bs4 import BeautifulSoup
import PyPDF2
import pdfplumber
from PIL import Image
import pytesseract
import pandas as pd

# Configuration
BASE_URL = "https://www.federalreserve.gov"
SR_LETTERS_BASE = f"{BASE_URL}/supervisionreg/srletters"
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "fed_reg_letters.json"

# Create directories
OUTPUT_DIR.mkdir(exist_ok=True)


class SRLettersScraper:
    """Main scraper class for SR Letters."""
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.session = requests.Session()
        self.existing_data = self._load_existing_data()
        self.existing_ids = {item['id'] for item in self.existing_data}
        self.scraped_date = datetime.utcnow().isoformat() + 'Z'
        self.current_year = datetime.now().year
        
    def _load_existing_data(self) -> List[Dict]:
        """Load existing scraped data to avoid duplicates."""
        if OUTPUT_FILE.exists():
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load existing data: {e}")
        return []
    
    def _setup_session(self):
        """Set up requests session with browser-like headers."""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Visit homepage to establish session
        try:
            self.session.get(BASE_URL)
            time.sleep(2)
        except Exception as e:
            print(f"Warning: Could not establish session: {e}")
    
    def get_years_to_scrape(self) -> List[int]:
        """Get list of years to scrape (current year and previous year)."""
        return [self.current_year, self.current_year - 1]
    
    def scrape_year_index(self, year: int) -> List[Dict]:
        """
        Scrape the index page for a specific year to get all SR Letter links.
        
        Args:
            year: Year to scrape (e.g., 2025)
            
        Returns:
            List of SR Letter metadata dictionaries.
        """
        url = f"{SR_LETTERS_BASE}/{year}.htm"
        print(f"\nScraping SR Letters for {year}...")
        print(f"URL: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            time.sleep(1)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            letters = self._parse_year_index(soup, year)
            
            # Filter out already scraped letters
            new_letters = [l for l in letters if l['id'] not in self.existing_ids]
            print(f"Found {len(letters)} letters, {len(new_letters)} are new")
            
            return new_letters
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"  No SR Letters page found for {year} (404)")
            else:
                print(f"  Error: {e}")
            return []
        except Exception as e:
            print(f"  Error scraping {year}: {e}")
            return []
    
    def _parse_year_index(self, soup: BeautifulSoup, year: int) -> List[Dict]:
        """
        Parse a year's index page to extract SR Letter metadata.
        
        Args:
            soup: BeautifulSoup object of the page.
            year: Year being parsed.
            
        Returns:
            List of SR Letter metadata dictionaries.
        """
        letters = []
        
        # Find the article container
        article = soup.select_one('#article')
        if not article:
            return letters
        
        # Find all rows containing SR Letter information
        rows = article.select('div.row')
        
        for row in rows:
            try:
                # Each letter has a link in the first column and title in the second
                link_col = row.select_one('div.col-xs-3 a')
                title_col = row.select_one('div.col-xs-9 p')
                
                if not link_col or not title_col:
                    continue
                
                # Extract letter ID and URL
                letter_id = link_col.get_text(strip=True)
                href = link_col.get('href', '')
                url = urljoin(BASE_URL, href)
                
                # Extract title
                headline = title_col.get_text(strip=True)
                
                # Theme is always "Supervision & Regulation"
                theme = "Supervision & Regulation"
                
                letters.append({
                    'id': letter_id,
                    'url': url,
                    'headline': headline,
                    'theme': theme,
                    'published_date': str(year),  # Will be refined when scraping the letter
                })
                
            except Exception as e:
                if self.debug:
                    print(f"  Error parsing row: {e}")
                continue
        
        return letters
    
    def scrape_sr_letter(self, metadata: Dict) -> Dict:
        """
        Scrape a single SR Letter page and extract all content.
        
        Args:
            metadata: SR Letter metadata from index page.
            
        Returns:
            Complete SR Letter data dictionary.
        """
        print(f"\nScraping: {metadata['id']} - {metadata['headline'][:60]}...")
        
        try:
            response = self.session.get(metadata['url'], timeout=30)
            response.raise_for_status()
            time.sleep(1)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract publication date from the letter content
            published_date = self._extract_date(soup)
            
            # Extract main content
            main_text = self._extract_main_text(soup)
            if self.debug:
                print(f"  Main text: {len(main_text)} characters")
            
            # Extract linked pages (exclude social media)
            linked_pages = self._extract_linked_pages(soup, metadata['url'])
            print(f"  ✓ Linked pages: {len(linked_pages)}")
            
            # Extract attachments
            attachments = self._extract_attachments(soup, metadata['url'])
            total_att = len(attachments['pdfs']) + len(attachments['excels']) + len(attachments['csvs'])
            print(f"  ✓ Attachments: {total_att} files")
            
            return {
                'id': metadata['id'],
                'url': metadata['url'],
                'headline': metadata['headline'],
                'theme': metadata['theme'],
                'published_date': published_date,
                'scraped_date': self.scraped_date,
                'content': {
                    'main_page_text': main_text,
                    'linked_pages': linked_pages,
                },
                'attachments': attachments,
            }
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None
    
    def _extract_date(self, soup: BeautifulSoup) -> str:
        """Extract publication date from SR Letter page."""
        # Look for date in the letterhead section
        letterhead = soup.select_one('.sr-letter__letterhead')
        if letterhead:
            # Date is typically in the last column
            date_col = letterhead.select_one('.col-xs-12.col-sm-4')
            if date_col:
                # Find strong tags that might contain the date
                strongs = date_col.find_all('strong')
                for strong in strongs:
                    text = strong.get_text(strip=True)
                    # Try to parse as date
                    date_match = re.search(r'([A-Za-z]+\s+\d{1,2},\s+\d{4})', text)
                    if date_match:
                        try:
                            dt = datetime.strptime(date_match.group(1), '%B %d, %Y')
                            return dt.strftime('%Y-%m-%d')
                        except:
                            pass
        
        # Fallback: look for date in article__time
        time_elem = soup.select_one('.article__time')
        if time_elem:
            text = time_elem.get_text(strip=True)
            date_match = re.search(r'([A-Za-z]+\s+\d{1,2},\s+\d{4})', text)
            if date_match:
                try:
                    dt = datetime.strptime(date_match.group(1), '%B %d, %Y')
                    return dt.strftime('%Y-%m-%d')
                except:
                    pass
        
        # Return current year if date not found
        return str(datetime.now().year)
    
    def _extract_main_text(self, soup: BeautifulSoup) -> str:
        """Extract main text content from SR Letter page."""
        text_parts = []
        
        # Find the article container
        article = soup.select_one('#article')
        
        if article:
            article_copy = BeautifulSoup(str(article), 'html.parser')
            
            # Remove unwanted elements
            for tag in article_copy.select('script, style, nav, .breadcrumb, .sr-letter__letterhead'):
                tag.decompose()
            
            # Extract title
            title = article_copy.select_one('h3#title')
            if title:
                text_parts.append(title.get_text(separator=' ', strip=True))
            
            # Extract subject
            subject = article_copy.select_one('.sr-letter__subject')
            if subject:
                text_parts.append(subject.get_text(separator=' ', strip=True))
            
            # Extract applicability panel
            applicability = article_copy.select_one('.panel-padded')
            if applicability:
                text_parts.append(applicability.get_text(strip=True))
            
            # Extract main body paragraphs
            rows = article_copy.select('.row')
            for row in rows:
                # Skip if it's a structural row we already processed
                if row.select_one('.sr-letter__subject') or row.select_one('.panel-padded'):
                    continue
                
                paragraphs = row.select('p')
                for p in paragraphs:
                    para_text = p.get_text(strip=True)
                    if para_text and len(para_text) > 20:
                        text_parts.append(para_text)
            
            # Extract supersedes section
            supersedes_text = self._extract_section(article_copy, 'Supersedes:')
            if supersedes_text:
                text_parts.append(f"Supersedes: {supersedes_text}")
            
            # Extract attachments section (just the list, not download)
            attachments_text = self._extract_section(article_copy, 'Attachments:')
            if attachments_text:
                text_parts.append(f"Attachments: {attachments_text}")
            
            # Extract notes/footnotes
            footnotes = article_copy.select('.footnotes')
            for fn in footnotes:
                text_parts.append("Notes:")
                text_parts.append(fn.get_text(strip=True))
            
            full_text = '\n\n'.join(text_parts)
            return self._clean_text(full_text)
        
        return ""
    
    def _extract_section(self, soup: BeautifulSoup, section_title: str) -> str:
        """Extract text from a specific section."""
        # Find the div containing the section title
        for div in soup.select('div.col-xs-12'):
            if section_title in div.get_text():
                # Get the next sibling div which contains the content
                next_div = div.find_next_sibling('div')
                if next_div:
                    return next_div.get_text(strip=True)
        return ""
    
    def _extract_linked_pages(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract text from linked pages, only supervision/regulation-related content."""
        linked_pages = []
        seen_urls = set()
        
        # Only look for links within the main article content area
        article = soup.select_one('#article')
        if not article:
            article = soup.select_one('#content[role="main"]')
        
        if not article:
            return linked_pages
        
        # Remove navigation/footer sections before processing
        article_copy = BeautifulSoup(str(article), 'html.parser')
        for unwanted in article_copy.select('nav, footer, .breadcrumb, .sr-letter__letterhead, .stay-connected, [role="navigation"]'):
            unwanted.decompose()
        
        if self.debug:
            print(f"  DEBUG: Scanning links in cleaned article content")
        
        # Find all links within the cleaned article content
        all_links = article_copy.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '').strip()
            if not href:
                continue
            
            # Convert to absolute URL
            full_url = urljoin(BASE_URL, href)
            
            # Must be a Federal Reserve page
            if not full_url.startswith(BASE_URL):
                continue
            
            # Skip the main page itself
            if full_url == base_url:
                continue
            
            # Must end with .htm (not a file download)
            if not full_url.endswith('.htm'):
                continue
            
            # Skip duplicates
            if full_url in seen_urls:
                continue
            
            # CRITICAL: Only include supervision/regulation-related URLs
            # SR Letters typically reference:
            # - Other SR letters: /supervisionreg/srletters/
            # - CA letters: /supervisionreg/caletters/
            # - Legal developments: /supervisionreg/legaldev
            # - Enforcement actions: /supervisionreg/enforcement
            # - Regulations: /supervisionreg/reg
            # - Supervision topics: /supervisionreg/
            
            supervision_patterns = [
                '/supervisionreg/srletters/',
                '/supervisionreg/caletters/',
                '/supervisionreg/legaldev',
                '/supervisionreg/enforcement',
                '/supervisionreg/reg',
                '/supervisionreg/topics/',
                '/supervisionreg/Basel',
                '/newsevents/pressreleases/enforcement',
            ]
            
            # Check if URL matches supervision/regulation patterns
            if not any(pattern in full_url for pattern in supervision_patterns):
                if self.debug:
                    print(f"      Skipping non-supervision link: {full_url}")
                continue
            
            seen_urls.add(full_url)
            
            # Fetch and extract
            page_data = self._fetch_linked_page(full_url)
            if page_data:
                linked_pages.append(page_data)
        
        return linked_pages
    
    def _fetch_linked_page(self, url: str) -> Optional[Dict]:
        """Fetch and extract text from a linked page."""
        try:
            if self.debug:
                print(f"    → Fetching: {url}")
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            time.sleep(1)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = self._extract_main_text(soup)
            
            if page_text and len(page_text) > 50:
                if self.debug:
                    print(f"      ✓ Extracted {len(page_text)} characters")
                return {
                    'url': url,
                    'text': page_text,
                }
            else:
                if self.debug:
                    print(f"      ✗ Insufficient content")
        except Exception as e:
            if self.debug:
                print(f"      ✗ Error: {e}")
        
        return None
    
    def _extract_attachments(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Extract and process all attachments."""
        attachments = {
            'pdfs': [],
            'excels': [],
            'csvs': [],
        }
        
        file_links = soup.select('a[href]')
        processed_urls = set()
        
        for link in file_links:
            href = link.get('href', '')
            if not href:
                continue
            
            full_url = urljoin(BASE_URL, href)
            
            if full_url in processed_urls:
                continue
            
            # Process by file type
            if full_url.endswith('.pdf'):
                processed_urls.add(full_url)
                pdf_data = self._extract_pdf(full_url)
                if pdf_data:
                    attachments['pdfs'].append(pdf_data)
            
            elif full_url.endswith(('.xlsx', '.xls')):
                processed_urls.add(full_url)
                excel_data = self._extract_excel(full_url)
                if excel_data:
                    attachments['excels'].append(excel_data)
            
            elif full_url.endswith('.csv'):
                processed_urls.add(full_url)
                csv_data = self._extract_csv(full_url)
                if csv_data:
                    attachments['csvs'].append(csv_data)
        
        return attachments
    
    def _extract_pdf(self, url: str) -> Optional[Dict]:
        """Extract text from PDF including tables and OCR."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            file_name = url.split('/')[-1]
            
            extracted_text = []
            
            # Try pdfplumber (best for tables)
            try:
                with pdfplumber.open(pdf_file) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            extracted_text.append(text)
                        
                        # Extract tables
                        tables = page.extract_tables()
                        for table in tables:
                            table_text = self._format_table(table)
                            extracted_text.append(table_text)
            except:
                pass
            
            # Fallback to PyPDF2
            if not extracted_text:
                pdf_file.seek(0)
                try:
                    reader = PyPDF2.PdfReader(pdf_file)
                    for page in reader.pages:
                        text = page.extract_text()
                        if text:
                            extracted_text.append(text)
                except:
                    pass
            
            # OCR attempt
            try:
                pdf_file.seek(0)
                with pdfplumber.open(pdf_file) as pdf:
                    for page_num, page in enumerate(pdf.pages):
                        img = page.to_image(resolution=300)
                        pil_img = img.original
                        ocr_text = pytesseract.image_to_string(pil_img)
                        if ocr_text.strip():
                            extracted_text.append(f"[OCR Page {page_num + 1}]\n{ocr_text}")
            except:
                pass
            
            full_text = '\n\n'.join(extracted_text)
            full_text = self._clean_text(full_text)
            
            if self.debug:
                print(f"    PDF: {file_name} ({len(full_text)} chars)")
            
            return {
                'file_name': file_name,
                'url': url,
                'extracted_text': full_text,
            }
        except Exception as e:
            if self.debug:
                print(f"    Error extracting PDF {url}: {e}")
            return None
    
    def _extract_excel(self, url: str) -> Optional[Dict]:
        """Extract text from Excel files."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            file_name = url.split('/')[-1]
            excel_file = io.BytesIO(response.content)
            
            extracted_text = []
            xl = pd.ExcelFile(excel_file)
            
            for sheet_name in xl.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                sheet_text = f"[Sheet: {sheet_name}]\n{df.to_string(index=False)}"
                extracted_text.append(sheet_text)
            
            full_text = '\n\n'.join(extracted_text)
            full_text = self._clean_text(full_text)
            
            if self.debug:
                print(f"    Excel: {file_name} ({len(full_text)} chars)")
            
            return {
                'file_name': file_name,
                'url': url,
                'extracted_text': full_text,
            }
        except Exception as e:
            if self.debug:
                print(f"    Error extracting Excel {url}: {e}")
            return None
    
    def _extract_csv(self, url: str) -> Optional[Dict]:
        """Extract text from CSV files."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            file_name = url.split('/')[-1]
            df = pd.read_csv(io.StringIO(response.text))
            csv_text = df.to_string(index=False)
            csv_text = self._clean_text(csv_text)
            
            if self.debug:
                print(f"    CSV: {file_name} ({len(csv_text)} chars)")
            
            return {
                'file_name': file_name,
                'url': url,
                'extracted_text': csv_text,
            }
        except Exception as e:
            if self.debug:
                print(f"    Error extracting CSV {url}: {e}")
            return None
    
    def _format_table(self, table: List[List]) -> str:
        """Format extracted table data as text."""
        if not table:
            return ""
        
        lines = []
        for row in table:
            line = ' | '.join([str(cell) if cell else '' for cell in row])
            lines.append(line)
        
        return '\n'.join(lines)
    
    def _clean_text(self, text: str) -> str:
        """Clean text by removing extra whitespace and boilerplate."""
        # Remove common boilerplate phrases
        boilerplate_phrases = [
            r'Skip to main content',
            r'An official website of the United States Government',
            r"Here's how you know",
            r'Official websites use \.gov',
            r'A \.gov website belongs to an official government organization in the United States\.',
            r'Secure \.gov websites use HTTPS',
            r'A lock \( Lock Locked padlock icon \) or https:// means you\'ve safely connected to the \.gov website\.',
            r'Share sensitive information only on official, secure websites\.',
            r'Back to Home',
            r'Board of Governors of the Federal Reserve System',
            r'Stay Connected',
            r'Federal Reserve .* Page',
            r'Subscribe to RSS',
            r'Subscribe to Email',
            r'Recent Postings',
            r'Calendar',
            r'Publications',
            r'Site Map',
            r'A-Z index',
            r'Careers',
            r'FAQs',
            r'Videos',
            r'Contact',
            r'Search.*Button',
            r'Toggle.*Menu',
            r'Last Update:.*\d{4}',
        ]
        
        for phrase in boilerplate_phrases:
            text = re.sub(phrase, '', text, flags=re.IGNORECASE)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Split into lines and clean
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line and len(line) > 1]
        
        # Remove duplicate consecutive lines
        cleaned_lines = []
        prev_line = None
        for line in lines:
            if line != prev_line:
                cleaned_lines.append(line)
                prev_line = line
        
        return '\n'.join(cleaned_lines)
    
    def save_results(self, new_letters: List[Dict]):
        """Save scraped data to JSON file."""
        all_data = self.existing_data + new_letters
        all_data.sort(key=lambda x: x.get('published_date', ''), reverse=True)
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print(f"Saved {len(all_data)} total SR Letters to {OUTPUT_FILE}")
        print(f"Added {len(new_letters)} new letters in this run")
        print(f"{'='*60}")
    
    def run(self):
        """Main execution method."""
        print("=" * 60)
        print("Federal Reserve SR Letters Scraper")
        print("=" * 60)
        
        self._setup_session()
        
        # Get years to scrape
        years = self.get_years_to_scrape()
        print(f"\nScraping SR Letters for years: {', '.join(map(str, years))}")
        
        # Scrape each year's index
        all_letters_metadata = []
        for year in years:
            year_letters = self.scrape_year_index(year)
            all_letters_metadata.extend(year_letters)
        
        if not all_letters_metadata:
            print("\nNo new SR Letters found")
            return
        
        print(f"\n{'='*60}")
        print(f"Found {len(all_letters_metadata)} new SR Letters to scrape")
        print(f"{'='*60}")
        
        # Scrape each letter
        scraped_letters = []
        for i, metadata in enumerate(all_letters_metadata, 1):
            print(f"\n[{i}/{len(all_letters_metadata)}]", end=' ')
            letter_data = self.scrape_sr_letter(metadata)
            
            if letter_data:
                scraped_letters.append(letter_data)
            
            time.sleep(2)  # Rate limiting
        
        # Save results
        if scraped_letters:
            self.save_results(scraped_letters)
        
        print("\n" + "=" * 60)
        print("Scraping completed successfully!")
        print("=" * 60)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Scrape Federal Reserve SR Letters',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 sr_letters_scrape.py          # Scrape last 2 years
  python3 sr_letters_scrape.py --debug  # Verbose output
        """
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode for verbose output'
    )
    
    args = parser.parse_args()
    
    scraper = SRLettersScraper(debug=args.debug)
    scraper.run()


if __name__ == "__main__":
    main()