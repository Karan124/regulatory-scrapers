#!/usr/bin/env python3
"""
Enhanced AER News Scraper for LLM Analysis
------------------------------------------
Comprehensive scraper that extracts all content types, embedded files,
and related resources with proper categorization and error handling.
"""

import json
import os
import time
import logging
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Dict, Set
import random
import requests
from bs4 import BeautifulSoup
import hashlib

# Import Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# File processing imports
try:
    import PyPDF2
    import io
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("WARNING: PyPDF2 not found. PDF extraction will be disabled. Run: pip install PyPDF2")

try:
    import pandas as pd
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False
    print("WARNING: pandas/openpyxl not found. Excel/CSV extraction will be disabled. Run: pip install pandas openpyxl")


class EnhancedAERNewsScraper:
    """Comprehensive AER news scraper with enhanced content extraction and categorization"""
    
    def __init__(self, max_pages: int = 3):
        self.BASE_URL = "https://www.aer.gov.au"
        self.NEWS_URL = "https://www.aer.gov.au/news/articles"
        
        self.DATA_DIR = "data"
        self.JSON_FILE = os.path.join(self.DATA_DIR, "aer_news.json")
        
        # Create data directory
        os.makedirs(self.DATA_DIR, exist_ok=True)
        
        # Smart MAX_PAGES logic with better handling
        if max_pages is None:
            is_first_run = not os.path.exists(self.JSON_FILE)
            self.MAX_PAGES = 300 if is_first_run else 5
            run_type = "First Run" if is_first_run else "Daily Update"
            print(f"INFO: Detected '{run_type}'. Setting MAX_PAGES to {self.MAX_PAGES}.")
        else:
            self.MAX_PAGES = max_pages
        
        self.setup_logging()
        
        self.driver = None
        self.session = requests.Session()
        self.existing_articles = self.load_existing_data()
        self.processed_files: Set[str] = set()
        self.session_retry_count = 0
        self.max_session_retries = 3
        
        self.setup_session()
        self.logger.info(f"Enhanced AER News Scraper initialized. Max pages to scrape: {self.MAX_PAGES}")

    def setup_logging(self):
        """Setup console-only logging"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def setup_session(self):
        """Setup session with realistic browser headers and better retry handling"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def load_existing_data(self) -> Dict[str, Dict]:
        """Load existing articles for deduplication"""
        existing = {}
        if os.path.exists(self.JSON_FILE):
            try:
                with open(self.JSON_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for article in data:
                        if isinstance(article, dict) and 'url' in article:
                            existing[article['url']] = article
                self.logger.info(f"Loaded {len(existing)} existing articles for deduplication.")
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
        return existing

    def _setup_driver(self) -> Optional[webdriver.Chrome]:
        """Setup Chrome driver with enhanced stability"""
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument(f'--user-agent={self.session.headers["User-Agent"]}')
        
        try:
            service = Service()
            driver = webdriver.Chrome(service=service, options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.implicitly_wait(15)
            driver.set_page_load_timeout(90)
            return driver
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            return None

    def establish_session(self) -> bool:
        """Establish session with improved retry logic"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass

        self.driver = self._setup_driver()
        if not self.driver:
            return False
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Session establishment attempt {attempt + 1}/{max_retries}")
                
                self.driver.get(self.BASE_URL)
                time.sleep(random.uniform(2, 4))
                
                self.driver.get(self.NEWS_URL)
                WebDriverWait(self.driver, 45).until(
                    EC.any_of(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.view-content")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".views-layout__item"))
                    )
                )
                
                self.logger.info("Session established successfully.")
                return True
                
            except TimeoutException:
                self.logger.warning(f"Session establishment timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 10))
                    continue
            except Exception as e:
                self.logger.error(f"Session establishment failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 10))
                    continue
        
        self.logger.error("Failed to establish session after all retries")
        return False

    def clean_text_for_llm(self, text: str) -> str:
        """Clean text to make it maximally LLM-friendly"""
        if not text:
            return ""
        
        # Remove excessive whitespace and normalize spacing
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'(\n\s*)+\n', '\n', text)
        
        # Remove special characters that might interfere with JSON or LLM processing
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        # Clean up common HTML entities
        html_entities = {
            '&nbsp;': ' ', '&amp;': '&', '&lt;': '<', '&gt;': '>',
            '&quot;': '"', '&#39;': "'", '&apos;': "'", '&mdash;': '—',
            '&ndash;': '–', '&hellip;': '…', '&lsquo;': ''', '&rsquo;': ''',
            '&ldquo;': '"', '&rdquo;': '"', '&bull;': '•'
        }
        for entity, replacement in html_entities.items():
            text = text.replace(entity, replacement)
        
        # Clean up common artifacts
        unwanted_patterns = [
            r'Print this page', r'Share this page', r'Download PDF',
            r'Skip to main content', r'Back to top'
        ]
        for pattern in unwanted_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        text = text.strip()
        if text and not text.endswith(('.', '!', '?', ':', '"', "'")):
            text += '.'
        
        return text

    def extract_pdf_content(self, pdf_url: str) -> str:
        """Extract complete PDF text with enhanced error handling"""
        if not PDF_AVAILABLE:
            return ""
            
        try:
            pdf_hash = hashlib.md5(pdf_url.encode()).hexdigest()
            if pdf_hash in self.processed_files:
                return ""
            
            self.logger.info(f"Extracting PDF: {os.path.basename(pdf_url)}")
            
            response = self.session.get(pdf_url, timeout=120)
            response.raise_for_status()
            
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(response.content))
            if len(pdf_reader.pages) == 0:
                return ""
            
            full_text_parts = []
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text and page_text.strip():
                    full_text_parts.append(self.clean_text_for_llm(page_text))
            
            full_text = ' '.join(full_text_parts)
            self.processed_files.add(pdf_hash)
            return full_text
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF {pdf_url}: {e}")
            return ""

    def extract_excel_csv_content(self, file_url: str) -> str:
        """Extract content from Excel/CSV files"""
        if not EXCEL_AVAILABLE:
            return ""
            
        try:
            file_hash = hashlib.md5(file_url.encode()).hexdigest()
            if file_hash in self.processed_files:
                return ""

            self.logger.info(f"Extracting spreadsheet: {os.path.basename(file_url)}")
            
            response = self.session.get(file_url, timeout=120)
            response.raise_for_status()
            
            file_extension = os.path.splitext(urlparse(file_url).path)[1].lower()
            
            if file_extension == '.csv':
                df = pd.read_csv(io.BytesIO(response.content), encoding='utf-8')
            else:
                df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')

            content = f"File: {os.path.basename(file_url)}\nColumns: {', '.join(df.columns)}\nRows: {len(df)}\nSample data:\n{df.head(3).to_string(index=False)}"
            
            self.processed_files.add(file_hash)
            return self.clean_text_for_llm(content)
            
        except Exception as e:
            self.logger.error(f"Error extracting spreadsheet {file_url}: {e}")
            return ""

    def extract_article_type_from_index(self, article_card_soup: BeautifulSoup) -> str:
        """Extract article type from index page"""
        try:
            type_elem = article_card_soup.select_one('div.field--name-field-article-type .field__item')
            if type_elem:
                return type_elem.get_text(strip=True)
        except:
            pass
        return ""

    def extract_segments_from_index(self, article_card_soup: BeautifulSoup) -> List[str]:
        """Extract segments from index page"""
        segments = []
        try:
            segment_items = article_card_soup.select('div.field--name-field-segments .field__item')
            for item in segment_items:
                segment = item.get_text(strip=True)
                if segment:
                    segments.append(segment)
        except:
            pass
        return segments

    def extract_sectors_from_index(self, article_card_soup: BeautifulSoup) -> List[str]:
        """Extract sectors from index page"""
        sectors = []
        try:
            if article_card_soup.select_one('.field__item-electricity'):
                sectors.append('Electricity')
            if article_card_soup.select_one('.field__item-gas'):
                sectors.append('Gas')
        except:
            pass
        return sectors

    def extract_all_embedded_links(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract ALL links from article content area - simplified approach"""
        embedded_links = []
        
        # Find the main content area
        content_area = soup.find('div', class_='field--name-field-body')
        if not content_area:
            return embedded_links
        
        # Get ALL links within the content area
        all_links = content_area.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '').strip()
            if not href:
                continue
            
            # Skip only these specific patterns
            if href.startswith(('#', 'mailto:', 'tel:', 'javascript:')):
                continue
            
            # Convert relative URLs to absolute
            if href.startswith('/'):
                full_url = urljoin(self.BASE_URL, href)
            elif href.startswith('http'):
                full_url = href
            else:
                continue
            
            link_text = link.get_text(strip=True)
            if link_text:
                embedded_links.append({
                    'url': full_url,
                    'text': link_text
                })
        
        return embedded_links

    def extract_related_content(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract related content from bottom section"""
        related_content = []
        
        # Find related content section
        related_section = soup.select_one('section.page__related, .views-element-container.block-views-block-content-related, #block-views-block-content-related')
        if not related_section:
            return related_content
        
        # Find all card links in related section
        cards = related_section.select('.card__title a')
        
        for card_link in cards:
            href = card_link.get('href', '').strip()
            if not href:
                continue
            
            # Convert to absolute URL
            full_url = urljoin(self.BASE_URL, href) if href.startswith('/') else href
            title = card_link.get_text(strip=True)
            
            # Get card container for additional metadata
            card_container = card_link.find_parent('.card__inner') or card_link.find_parent('.views-layout__item')
            
            description = ""
            content_type = ""
            sectors = []
            segments = []
            date = ""
            
            if card_container:
                # Extract description
                summary_elem = card_container.select_one('.field--name-field-summary, .card__body')
                if summary_elem:
                    description = summary_elem.get_text(strip=True)
                
                # Extract content type
                type_elem = card_container.select_one('.field--name-field-report-type .field__item, .field--name-field-article-type .field__item')
                if type_elem:
                    content_type = type_elem.get_text(strip=True)
                
                # Extract sectors
                if card_container.select_one('.field__item-electricity'):
                    sectors.append('Electricity')
                if card_container.select_one('.field__item-gas'):
                    sectors.append('Gas')
                
                # Extract segments
                segment_items = card_container.select('.field--name-field-segments .field__item')
                for item in segment_items:
                    segment = item.get_text(strip=True)
                    if segment:
                        segments.append(segment)
                
                # Extract date
                date_elem = card_container.select_one('time[datetime]')
                if date_elem:
                    date = date_elem.get('datetime', date_elem.get_text(strip=True))
            
            if title and full_url:
                related_content.append({
                    'url': full_url,
                    'title': title,
                    'description': description,
                    'type': content_type,
                    'sectors': sectors,
                    'segments': segments,
                    'date': date
                })
        
        return related_content

    def process_embedded_files(self, embedded_links: List[Dict]) -> Dict:
        """Process embedded links for PDF and Excel content"""
        pdf_content = []
        spreadsheet_content = []
        
        for link_info in embedded_links:
            href = link_info['url']
            href_lower = href.lower()
            
            if href_lower.endswith('.pdf'):
                pdf_text = self.extract_pdf_content(href)
                if pdf_text:
                    pdf_content.append(pdf_text)
            elif href_lower.endswith(('.xlsx', '.xls', '.csv')):
                ss_text = self.extract_excel_csv_content(href)
                if ss_text:
                    spreadsheet_content.append(ss_text)
        
        return {
            'pdf_content': pdf_content,
            'spreadsheet_content': spreadsheet_content
        }

    def get_article_links_with_metadata(self, page_num=0) -> List[Dict]:
        """Get article links with metadata from index page"""
        try:
            url = f"{self.NEWS_URL}?page={page_num}"
            self.logger.info(f"Fetching page {page_num + 1}: {url}")
            
            self.driver.get(url)
            WebDriverWait(self.driver, 45).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h3.card__title a")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".views-layout__item"))
                )
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            article_cards = soup.select('.views-layout__item')
            
            articles_info = []
            for card in article_cards:
                link_elem = card.select_one('h3.card__title a')
                if not link_elem or not link_elem.get('href', '').startswith('/news/articles/'):
                    continue
                
                article_url = urljoin(self.BASE_URL, link_elem['href'])
                article_info = {
                    'url': article_url,
                    'title': link_elem.get_text(strip=True),
                    'article_type': self.extract_article_type_from_index(card),
                    'sectors': self.extract_sectors_from_index(card),
                    'segments': self.extract_segments_from_index(card)
                }
                articles_info.append(article_info)
            
            self.logger.info(f"Found {len(articles_info)} valid articles on page {page_num + 1}")
            return articles_info
            
        except Exception as e:
            self.logger.error(f"Failed to get links for page {page_num + 1}: {e}")
            return []

    def parse_article(self, article_info: Dict) -> Optional[Dict]:
        """Parse article with comprehensive content extraction"""
        url = article_info['url']
        try:
            self.logger.info(f"Parsing: {article_info.get('title', 'Unknown')}")
            
            self.driver.get(url)
            WebDriverWait(self.driver, 45).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # Extract basic information
            title = soup.find('h1').get_text(strip=True) if soup.find('h1') else article_info.get('title', 'N/A')
            
            # Extract date
            date = ""
            date_elem = soup.select_one('div.field--name-field-date time, time[datetime]')
            if date_elem:
                date = date_elem.get('datetime', date_elem.get_text(strip=True))
            
            # Extract main content
            main_content = ""
            body_elem = soup.find('div', class_='field--name-field-body')
            if body_elem:
                main_content = self.clean_text_for_llm(body_elem.get_text(strip=True))

            # Extract article type from content page
            article_type = ""
            type_elem = soup.select_one('div.field--name-field-article-type .field__item')
            if type_elem:
                article_type = type_elem.get_text(strip=True)
            else:
                article_type = article_info.get('article_type', '')

            # Extract sectors
            sectors = []
            sector_items = soup.select('div.field--name-field-sectors .field__item')
            for item in sector_items:
                sector = item.get_text(strip=True)
                if sector:
                    sectors.append(sector)
            if not sectors:
                sectors = article_info.get('sectors', [])

            # Extract segments
            segments = []
            segment_items = soup.select('div.field--name-field-segments .field__item')
            for item in segment_items:
                segment = item.get_text(strip=True)
                if segment:
                    segments.append(segment)
            if not segments:
                segments = article_info.get('segments', [])

            # Extract theme from breadcrumbs
            theme = ""
            breadcrumbs = soup.select('nav.breadcrumb a')
            if len(breadcrumbs) > 1:
                theme = breadcrumbs[-1].get_text(strip=True)

            # Extract image
            image_url = ""
            img_elem = soup.select_one('.field--name-field-body img')
            if img_elem and img_elem.get('src'):
                image_url = urljoin(self.BASE_URL, img_elem['src'])

            # Extract tables
            tables_content = []
            for table in soup.select('.field--name-field-body table'):
                table_text = self.clean_text_for_llm(table.get_text())
                if table_text:
                    tables_content.append(table_text)

            # Extract ALL embedded links from content
            embedded_links = self.extract_all_embedded_links(soup)
            
            # Extract related content
            related_content = self.extract_related_content(soup)
            
            # Process files from embedded links
            file_content = self.process_embedded_files(embedded_links)

            # Build clean article data structure
            article_data = {
                'url': url,
                'headline': title,
                'published_date': date,
                'scraped_date': datetime.now().isoformat(),
                'article_type': article_type,
                'theme': theme,
                'sectors': sectors,
                'segments': segments,
                'image_url': image_url,
                'main_content': main_content,
                'embedded_links': embedded_links,
                'related_content': related_content
            }
            
            # Only add optional fields if they contain data
            if tables_content:
                article_data['tables_and_data'] = ' '.join(tables_content)
            
            if file_content['pdf_content']:
                article_data['pdf_content'] = ' '.join(file_content['pdf_content'])
            
            if file_content['spreadsheet_content']:
                article_data['spreadsheet_content'] = ' '.join(file_content['spreadsheet_content'])
            
            self.logger.info(f"Successfully parsed: {title[:50]}... (Type: {article_type}, Links: {len(embedded_links)})")
            return article_data
            
        except Exception as e:
            self.logger.error(f"Failed to parse article {url}: {e}")
            return None

    def save_results(self):
        """Save results to single JSON file"""
        try:
            all_articles = list(self.existing_articles.values())
            
            with open(self.JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, indent=2, ensure_ascii=False, sort_keys=True)
            
            # Generate summary for console
            stats = {'total_articles': len(all_articles), 'by_type': {}}
            for article in all_articles:
                article_type = article.get('article_type', 'Unknown')
                stats['by_type'][article_type] = stats['by_type'].get(article_type, 0) + 1
            
            self.logger.info(f"Saved {stats['total_articles']} articles")
            self.logger.info(f"Types: {dict(stats['by_type'])}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")

    def handle_session_recovery(self) -> bool:
        """Handle session recovery when driver fails"""
        self.session_retry_count += 1
        if self.session_retry_count >= self.max_session_retries:
            self.logger.error(f"Max session retries reached. Stopping.")
            return False
        
        self.logger.warning(f"Session recovery attempt {self.session_retry_count}")
        time.sleep(random.uniform(10, 20))
        return self.establish_session()

    def scrape_all_articles(self):
        """Main scraping method"""
        if not self.establish_session():
            self.logger.error("Failed to establish initial session. Exiting.")
            return
        
        self.logger.info("Starting enhanced news scraping...")
        new_articles_count = 0
        consecutive_empty_pages = 0
        
        try:
            for page_num in range(self.MAX_PAGES):
                self.logger.info(f"--- Processing page {page_num + 1}/{self.MAX_PAGES} ---")
                
                try:
                    articles_info = self.get_article_links_with_metadata(page_num)
                    
                    if not articles_info:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= 3:
                            self.logger.info("Reached end of available pages.")
                            break
                        continue
                    
                    consecutive_empty_pages = 0

                    for article_info in articles_info:
                        url = article_info['url']
                        
                        if url in self.existing_articles:
                            continue
                        
                        self.processed_files.clear()  # Reset for each article
                        
                        try:
                            article = self.parse_article(article_info)
                            if article:
                                self.existing_articles[url] = article
                                new_articles_count += 1
                        except Exception as article_error:
                            self.logger.error(f"Error processing article {url}: {article_error}")
                        
                        # Save progress periodically
                        if new_articles_count > 0 and new_articles_count % 25 == 0:
                            self.save_results()

                        time.sleep(random.uniform(1, 3))
                    
                    time.sleep(random.uniform(2, 5))
                
                except Exception as page_error:
                    self.logger.error(f"Error processing page {page_num + 1}: {page_error}")
                    if not self.handle_session_recovery():
                        break
                
        except KeyboardInterrupt:
            self.logger.warning("Scraping interrupted by user.")
        except Exception as e:
            self.logger.error(f"Critical error: {e}")
        finally:
            self.logger.info(f"Scraping completed. New articles: {new_articles_count}")
            self.save_results()
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        self.logger.info("Cleaning up...")
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
        if self.session:
            try:
                self.session.close()
            except:
                pass

    def get_scraper_stats(self) -> Dict:
        """Get statistics about scraped data"""
        if not os.path.exists(self.JSON_FILE):
            return {"error": "No data file found"}
        
        try:
            with open(self.JSON_FILE, 'r', encoding='utf-8') as f:
                articles = json.load(f)
            
            stats = {
                'total_articles': len(articles),
                'article_types': {},
                'with_embedded_links': 0,
                'with_related_content': 0,
                'with_files': 0
            }
            
            for article in articles:
                article_type = article.get('article_type', 'Unknown')
                stats['article_types'][article_type] = stats['article_types'].get(article_type, 0) + 1
                
                if article.get('embedded_links'):
                    stats['with_embedded_links'] += 1
                if article.get('related_content'):
                    stats['with_related_content'] += 1
                if article.get('pdf_content') or article.get('spreadsheet_content'):
                    stats['with_files'] += 1
            
            return stats
            
        except Exception as e:
            return {"error": f"Failed to generate stats: {e}"}


def main():
    print("=" * 80)
    print("Enhanced AER News Scraper for LLM Analysis")
    print("=" * 80)
    print("Features:")
    print("• Extracts article types (Communication, News Release, Speech)")
    print("• Captures ALL embedded links within article content")
    print("• Enhanced PDF and Excel/CSV content extraction")
    print("• Extracts related content with full metadata")
    print("• Clean output structure for LLM processing")
    print("=" * 80)
    
    scraper = EnhancedAERNewsScraper()
    
    try:
        scraper.scrape_all_articles()
    except Exception as e:
        print(f"Critical error in main: {e}")
    finally:
        print("\n" + "=" * 80)
        print("SCRAPING COMPLETED - FINAL STATISTICS")
        print("=" * 80)
        
        stats = scraper.get_scraper_stats()
        if 'error' not in stats:
            print(f"Total articles: {stats['total_articles']}")
            print(f"Article types: {dict(stats['article_types'])}")
            print(f"Articles with embedded links: {stats['with_embedded_links']}")
            print(f"Articles with related content: {stats['with_related_content']}")
            print(f"Articles with files: {stats['with_files']}")
        else:
            print(f"Stats error: {stats['error']}")
        
        print(f"Output saved to: {scraper.JSON_FILE}")
        print("=" * 80)


if __name__ == "__main__":
    main()