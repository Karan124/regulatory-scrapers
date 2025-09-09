import os
import json
import time
import logging
import sys
import re
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin, urlparse, unquote
import urllib3
from pathlib import Path
import io

# Suppress urllib3 warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration for PRODUCTION
BASE_URL = "https://www.accc.gov.au"
NEWS_CENTRE_URL = f"{BASE_URL}/news-centre"

# Use script directory for paths
SCRIPT_DIR = Path(__file__).parent
DATA_FOLDER = SCRIPT_DIR / "data"
OUTPUT_JSON = DATA_FOLDER / "accc_news_complete.json"
LOG_FILE = SCRIPT_DIR / "accc_scraper_production.log"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Request settings
REQUEST_TIMEOUT = 30
RETRY_DELAY = 2
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 1.5  # Slightly slower for production

# PRODUCTION CONFIGURATION
MAX_PAGES = 2  # For 398 pages + buffer
SAVE_INTERVAL = 20  # Save progress every 20 pages

# Content extraction settings
MIN_CONTENT_LENGTH = 100
MAX_PDF_SIZE_MB = 100  # Increased for production

# Ensure data folder exists
DATA_FOLDER.mkdir(exist_ok=True)

class ACCCScraperProduction:
    def __init__(self):
        self.session = None
        self.setup_logging()
        self.setup_session()
        self.all_articles = []
        self.processed_urls = set()
        self.failed_urls = []
        self.total_scraped = 0
        self.total_pdfs_processed = 0
        self.total_excel_processed = 0
        self.start_time = time.time()

    def setup_logging(self):
        """Setup production logging"""
        self.logger = logging.getLogger("accc_scraper_production")
        self.logger.setLevel(logging.INFO)
        
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        
        # File handler
        fh = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        
        # Console handler
        if not os.environ.get('RUNNING_FROM_ORCHESTRATOR'):
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def setup_session(self):
        """Configure session for production scraping"""
        try:
            if self.session:
                self.session.close()
            
            self.session = requests.Session()
            
            retry_strategy = Retry(
                total=MAX_RETRIES,
                backoff_factor=RETRY_DELAY,
                status_forcelist=[403, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524],
                allowed_methods=["HEAD", "GET", "OPTIONS"]
            )
            
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=10,
                pool_maxsize=20,
                pool_block=False
            )
            
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
            
            self.session.headers.update({
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "DNT": "1"
            })
            
            self.logger.info("Production session configured successfully")
            
        except Exception as e:
            self.logger.error(f"Error setting up session: {e}")
            raise

    def save_data(self, force_save=False):
        """Save data to single JSON file - NO STATS"""
        try:
            if not self.all_articles and not force_save:
                self.logger.warning("No data to save")
                return
            
            # Sort by scraped_date (newest first)
            try:
                self.all_articles.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
            except:
                pass
            
            # Clean structure - only essential metadata
            output_data = {
                "scrape_info": {
                    "total_articles": len(self.all_articles),
                    "scrape_date": datetime.now().isoformat(),
                    "scraper_version": "production_llm_ready"
                },
                "articles": self.all_articles
            }
            
            with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(self.all_articles)} articles")
            
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            raise

    def get_page(self, url, binary=False):
        """Fetch page with robust error handling"""
        for attempt in range(MAX_RETRIES + 1):
            try:
                time.sleep(RATE_LIMIT_DELAY)
                
                response = self.session.get(
                    url, 
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                    verify=True
                )
                
                response.raise_for_status()
                
                if binary:
                    return response.content
                else:
                    if not response.text.strip():
                        continue
                    return response.text
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout {url} (attempt {attempt + 1})")
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"Connection error {url} (attempt {attempt + 1})")
                self.setup_session()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.logger.warning(f"404 Not Found: {url}")
                    return None
                elif e.response.status_code in [403, 429]:
                    self.logger.warning(f"Rate limited: {url}")
                    time.sleep(RETRY_DELAY * (attempt + 2))
                else:
                    self.logger.warning(f"HTTP {e.response.status_code}: {url}")
            except Exception as e:
                self.logger.error(f"Unexpected error {url}: {e}")
            
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * (attempt + 1))
        
        self.logger.error(f"Failed to fetch {url}")
        self.failed_urls.append(url)
        return None

    def parse_news_listing(self, html, page_url):
        """Parse news listing to find article URLs"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            articles = []
            
            # Comprehensive selectors for ACCC news
            selectors = [
                'div[data-type="accc-news"] a',
                '.accc-date-card a',
                '.news-item a',
                '.article-card a',
                'article a',
                '.view-content a',
                '.field--name-field-acccgov-body a'
            ]
            
            for selector in selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href:
                        article_url = urljoin(BASE_URL, href)
                        if self.is_valid_accc_news_url(article_url):
                            articles.append(article_url)
            
            # Also check all links for comprehensive coverage
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                if href:
                    article_url = urljoin(BASE_URL, href)
                    if self.is_valid_accc_news_url(article_url) and article_url not in articles:
                        articles.append(article_url)
            
            # Remove duplicates
            unique_articles = list(dict.fromkeys(articles))
            self.logger.info(f"Found {len(unique_articles)} articles on page")
            return unique_articles
            
        except Exception as e:
            self.logger.error(f"Error parsing listing: {e}")
            return []

    def is_valid_accc_news_url(self, url):
        """Validate ACCC news URL"""
        try:
            parsed = urlparse(url)
            
            if parsed.netloc not in ['www.accc.gov.au', 'accc.gov.au']:
                return False
            
            path = parsed.path.lower()
            
            news_patterns = [
                '/news/', '/media-release/', '/speech/', '/update/',
                '/media-updates/', '/about-us/news/', '/about-us/publications/',
                '/media/', '/determination/', '/authorisation/', '/investigation/',
                '/announcement/', '/report/', '/consultation/', '/inquiry/'
            ]
            
            return any(pattern in path for pattern in news_patterns)
            
        except Exception as e:
            self.logger.error(f"Error validating URL {url}: {e}")
            return False

    def extract_pdf_content(self, pdf_url):
        """Extract PDF content"""
        try:
            self.logger.info(f"Processing PDF: {pdf_url}")
            
            pdf_content = self.get_page(pdf_url, binary=True)
            if not pdf_content:
                return None
            
            if len(pdf_content) > MAX_PDF_SIZE_MB * 1024 * 1024:
                self.logger.warning(f"PDF too large: {pdf_url}")
                return None
            
            try:
                import PyPDF2
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                
                text_content = []
                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        text = page.extract_text()
                        if text.strip():
                            text_content.append(f"--- Page {page_num + 1} ---\n{text.strip()}")
                    except Exception:
                        continue
                
                if text_content:
                    self.total_pdfs_processed += 1
                    return {
                        "url": pdf_url,
                        "type": "PDF",
                        "content": "\n\n".join(text_content)
                    }
                    
            except ImportError:
                self.logger.warning("PyPDF2 not available")
            except Exception as e:
                self.logger.error(f"PDF processing error: {e}")
            
        except Exception as e:
            self.logger.error(f"PDF error {pdf_url}: {e}")
        
        return None

    def extract_excel_content(self, excel_url):
        """Extract Excel/CSV content"""
        try:
            self.logger.info(f"Processing Excel/CSV: {excel_url}")
            
            file_content = self.get_page(excel_url, binary=True)
            if not file_content:
                return None
            
            try:
                import pandas as pd
                
                if excel_url.lower().endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(file_content))
                    content_text = df.to_string(index=False)
                    content_type = "CSV"
                else:
                    df_dict = pd.read_excel(io.BytesIO(file_content), sheet_name=None)
                    content_type = "Excel"
                    
                    if isinstance(df_dict, dict):
                        combined_data = []
                        for sheet_name, sheet_df in df_dict.items():
                            combined_data.append(f"=== Sheet: {sheet_name} ===")
                            combined_data.append(sheet_df.to_string(index=False))
                        content_text = "\n\n".join(combined_data)
                    else:
                        content_text = df_dict.to_string(index=False)
                
                self.total_excel_processed += 1
                return {
                    "url": excel_url,
                    "type": content_type,
                    "content": content_text
                }
                
            except ImportError:
                self.logger.warning("pandas not available")
            except Exception as e:
                self.logger.error(f"Excel processing error: {e}")
                
        except Exception as e:
            self.logger.error(f"Excel error {excel_url}: {e}")
        
        return None

    def extract_related_content_links(self, article_soup, base_url):
        """Extract ALL relevant links from article content - FIXED VERSION"""
        try:
            # Find the main content area - use the whole article if needed
            content_area = (
                article_soup.select_one('.field--name-field-acccgov-body') or 
                article_soup.select_one('.field--name-field-acccgov-speech-transcript') or
                article_soup.select_one('.article-body') or 
                article_soup.select_one('.content-body') or
                article_soup.select_one('main') or
                article_soup
            )
            
            if not content_area:
                return []
            
            # Get ALL links in the content area
            all_links = content_area.find_all('a', href=True)
            relevant_links = []
            
            # MINIMAL exclusions - only obvious non-content
            exclude_patterns = [
                'mailto:', 'tel:', '#', 'javascript:',
                '/help', '/feedback', '/accessibility', '/sitemap', '/privacy', '/copyright',
                '/search', '/subscribe', '/newsletter',
                '/twitter', '/facebook', '/linkedin', '/youtube', '/instagram',
                '/home$', '/news-centre$', '/media-centre$'
            ]
            
            for link in all_links:
                href = link.get('href', '').strip()
                if not href:
                    continue
                
                # Convert to absolute URL
                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
                
                # Skip external links (unless they're documents)
                if parsed.netloc and parsed.netloc not in ['www.accc.gov.au', 'accc.gov.au']:
                    if not any(href.lower().endswith(ext) for ext in ['.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx']):
                        continue
                
                # Simple exclusion check
                should_exclude = any(pattern in href.lower() for pattern in exclude_patterns)
                
                if not should_exclude:
                    link_text = link.get_text().strip()
                    
                    # Clean up link text
                    if not link_text and link.parent:
                        link_text = link.parent.get_text().strip()[:100]
                    
                    if link_text and len(link_text) > 2:
                        # Determine link type
                        is_document = any(href.lower().endswith(ext) for ext in ['.pdf', '.xlsx', '.xls', '.csv', '.doc', '.docx', '.txt'])
                        link_type = "document" if is_document else "related_content"
                        
                        relevant_links.append({
                            "url": full_url,
                            "text": link_text,
                            "type": link_type
                        })
            
            # Remove duplicates
            seen = set()
            unique_links = []
            for link in relevant_links:
                link_key = (link["url"], link["text"])
                if link_key not in seen:
                    unique_links.append(link)
                    seen.add(link_key)
            
            return unique_links
            
        except Exception as e:
            self.logger.error(f"Error extracting links: {e}")
            return []

    def scrape_article_page(self, url):
        """Scrape individual article - FIXED content extraction"""
        try:
            html = self.get_page(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # FIRST: Remove the site-wide notification banner that contains warnings
            for warning_banner in soup.select('.region-site-notification-bar, .accc-site-notification, [data-id="13"]'):
                warning_banner.decompose()
            
            # Find main article
            article = soup.select_one('article.accc-full-view') or soup.select_one('article') or soup.find('main')
            if not article:
                article = soup.find('body')
            
            # Extract data - NO STATS
            data = {
                'url': url,
                'scraped_date': datetime.now().isoformat(),
                'title': self.get_title(article, soup),
                'published_date': self.get_date(article, soup),
                'article_type': self.get_article_type(url, article, soup),
                'summary': self.get_summary(article, soup),
                'content': self.get_main_content(article, soup),
                'topics': self.get_topics(article, soup),
                'related_content_links': [],
                'embedded_documents': []
            }
            
            # Extract links
            related_links = self.extract_related_content_links(article, url)
            data['related_content_links'] = related_links
            
            # Process documents
            document_links = [link for link in related_links if link.get('type') == 'document']
            
            for doc_link in document_links:
                doc_url = doc_link['url']
                doc_content = None
                
                if doc_url.lower().endswith('.pdf'):
                    doc_content = self.extract_pdf_content(doc_url)
                elif any(doc_url.lower().endswith(ext) for ext in ['.xlsx', '.xls', '.csv']):
                    doc_content = self.extract_excel_content(doc_url)
                
                if doc_content:
                    doc_content['link_text'] = doc_link['text']
                    data['embedded_documents'].append(doc_content)
            
            # Validate content
            if not data['title'] and (not data['content'] or len(data['content']) < MIN_CONTENT_LENGTH):
                self.logger.warning(f"Insufficient content: {url}")
                return None
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {e}")
            return None

    def get_title(self, article, soup):
        """Extract title"""
        selectors = [
            'h1.page-title span',
            'h1.page-title',
            'h1',
            '.title',
            '.article-title'
        ]
        
        for selector in selectors:
            element = article.select_one(selector) or soup.select_one(selector)
            if element:
                text = element.get_text().strip()
                if text and len(text) > 3:
                    return text
        return ""

    def get_main_content(self, article, soup):
        """FIXED: Extract main content properly - avoiding site warnings"""
        try:
            # Content-specific selectors for different ACCC content types
            content_selectors = [
                # Speech-specific content
                '.field--name-field-acccgov-speech-transcript .field__item',
                '.field--name-field-acccgov-speech-transcript',
                # News article content  
                '.field--name-field-acccgov-body .field__item',
                '.field--name-field-acccgov-body',
                # General content
                '.article-body',
                '.content-body', 
                '.field--name-body .field__item',
                '.field--name-body',
                # Main content area
                '.accc-field__section .field__item',
                'main .field__item'
            ]
            
            content_element = None
            
            for selector in content_selectors:
                element = article.select_one(selector) or soup.select_one(selector)
                if element:
                    text_preview = element.get_text().strip()
                    
                    # Skip elements containing warning text
                    warning_indicators = [
                        'warning: we\'ve had reports of scammers',
                        'scammers using accc phone numbers',
                        'do not provide this information and hang up'
                    ]
                    
                    is_warning = any(indicator in text_preview.lower() for indicator in warning_indicators)
                    
                    if len(text_preview) > MIN_CONTENT_LENGTH and not is_warning:
                        content_element = element
                        break
            
            if not content_element:
                # Fallback: clean paragraphs excluding warnings
                paragraphs = article.select('p')
                clean_paragraphs = []
                
                for p in paragraphs:
                    p_text = p.get_text().strip()
                    if p_text and len(p_text) > 20:
                        warning_indicators = [
                            'warning: we\'ve had reports',
                            'scammers using accc phone numbers',
                            'do not provide this information'
                        ]
                        
                        is_warning = any(indicator in p_text.lower() for indicator in warning_indicators)
                        if not is_warning:
                            clean_paragraphs.append(p_text)
                
                if clean_paragraphs:
                    return '\n\n'.join(clean_paragraphs)
                
                return ""
            
            # Clean up content element
            content_copy = BeautifulSoup(str(content_element), 'html.parser')
            
            # Remove unwanted elements including warnings
            unwanted_selectors = [
                'script', 'style', 'nav', 'aside', '.social-share',
                '.scam-warning', '.warning', '.alert-warning', 
                '.accc-site-notification', '.region-site-notification-bar',
                '[data-id="13"]'
            ]
            
            for selector in unwanted_selectors:
                for unwanted in content_copy.select(selector):
                    unwanted.decompose()
            
            # Extract structured content
            content_parts = []
            
            for element in content_copy.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol', 'blockquote', 'div']):
                if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    text = element.get_text().strip()
                    if text and len(text) > 2:
                        level = element.name[1]
                        content_parts.append(f"\n{'#' * int(level)} {text}")
                        
                elif element.name == 'p':
                    text = element.get_text().strip()
                    # Skip warning paragraphs
                    warning_indicators = [
                        'warning: we\'ve had reports',
                        'scammers using accc phone numbers',
                        'do not provide this information'
                    ]
                    
                    is_warning = any(indicator in text.lower() for indicator in warning_indicators)
                    
                    if text and len(text) > 10 and not is_warning:
                        content_parts.append(f"\n{text}")
                        
                elif element.name in ['ul', 'ol']:
                    items = []
                    for li in element.find_all('li'):
                        li_text = li.get_text().strip()
                        if li_text:
                            items.append(f"• {li_text}")
                    if items:
                        content_parts.append(f"\n{chr(10).join(items)}")
                        
                elif element.name == 'blockquote':
                    text = element.get_text().strip()
                    if text:
                        content_parts.append(f"\n> {text}")
                        
                elif element.name == 'div' and not element.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol']):
                    text = element.get_text().strip()
                    # Skip warning divs
                    warning_indicators = [
                        'warning: we\'ve had reports',
                        'scammers using accc phone numbers'
                    ]
                    
                    is_warning = any(indicator in text.lower() for indicator in warning_indicators)
                    
                    if text and len(text) > 20 and not is_warning:
                        content_parts.append(f"\n{text}")
            
            if content_parts:
                full_content = '\n'.join(content_parts)
                # Clean up excessive whitespace
                full_content = re.sub(r'\n{3,}', '\n\n', full_content)
                full_content = re.sub(r' {2,}', ' ', full_content)
                return full_content.strip()
            
            # Final fallback - filter warnings from raw text
            fallback_content = content_element.get_text(separator='\n').strip()
            
            lines = fallback_content.split('\n')
            clean_lines = []
            
            for line in lines:
                line = line.strip()
                if line:
                    warning_indicators = [
                        'warning: we\'ve had reports',
                        'scammers using accc phone numbers',
                        'do not provide this information'
                    ]
                    
                    is_warning = any(indicator in line.lower() for indicator in warning_indicators)
                    if not is_warning:
                        clean_lines.append(line)
            
            if clean_lines:
                clean_content = '\n'.join(clean_lines)
                clean_content = re.sub(r'\n{3,}', '\n\n', clean_content)
                clean_content = re.sub(r' {2,}', ' ', clean_content)
                return clean_content.strip()
            
            return ""
            
        except Exception as e:
            self.logger.error(f"Error extracting content: {e}")
            return ""

    def get_summary(self, article, soup):
        """Extract summary"""
        selectors = [
            '.field--name-field-summary .field__item',
            '.field--name-field-summary',
            '.summary',
            '.excerpt',
            '.lead'
        ]
        
        for selector in selectors:
            element = article.select_one(selector) or soup.select_one(selector)
            if element:
                text = element.get_text().strip()
                if text and len(text) > 10:
                    return text
        return ""

    def get_date(self, article, soup):
        """Extract date"""
        selectors = [
            '.field--name-field-accc-news-published-date time[datetime]',
            'time[datetime]',
            '.published-date',
            '.date'
        ]
        
        for selector in selectors:
            element = article.select_one(selector) or soup.select_one(selector)
            if element:
                if element.get('datetime'):
                    return element['datetime']
                date_text = element.get_text().strip()
                if date_text:
                    return date_text
        return ""

    def get_article_type(self, url, article, soup):
        """Get article type"""
        # Try explicit markers
        type_selectors = ['.article-type', '.content-type', '.news-type']
        
        for selector in type_selectors:
            element = article.select_one(selector) or soup.select_one(selector)
            if element:
                type_text = element.get_text().strip()
                if type_text:
                    return type_text
        
        # Infer from URL
        url_lower = url.lower()
        type_mapping = {
            '/media-release/': 'Media release',
            '/speech/': 'Speech',
            '/media-updates/': 'Media update',
            '/update/': 'Update',
            '/determination/': 'Determination',
            '/authorisation/': 'Authorisation',
            '/investigation/': 'Investigation',
            '/inquiry/': 'Inquiry',
            '/consultation/': 'Consultation',
            '/report/': 'Report'
        }
        
        for pattern, article_type in type_mapping.items():
            if pattern in url_lower:
                return article_type
        
        return "News"

    def get_topics(self, article, soup):
        """Extract topics"""
        topics = []
        selectors = [
            '.field--name-field-acccgov-topic .terms-badge a',
            '.field--name-field-acccgov-topic a',
            '.topics a',
            '.tags a'
        ]
        
        for selector in selectors:
            elements = article.select(selector) or soup.select(selector)
            for element in elements:
                topic = element.get_text().strip()
                if topic and topic not in topics:
                    topics.append(topic)
        
        return topics

    def scrape_all_pages(self):
        """Main production scraping method"""
        self.logger.info(f"Starting production ACCC scraper - up to {MAX_PAGES} pages")
        print("=" * 60)
        print("ACCC NEWS SCRAPER - PRODUCTION VERSION")
        print(f"Target: Up to {MAX_PAGES} pages")
        print("Features: LLM-ready content, PDF/Excel extraction, comprehensive link capture")
        print("=" * 60)
        
        try:
            page = 0
            total_articles_found = 0
            
            while page < MAX_PAGES:
                page_url = NEWS_CENTRE_URL if page == 0 else f"{NEWS_CENTRE_URL}?page={page}"
                
                elapsed = time.time() - self.start_time
                hours, remainder = divmod(elapsed, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                print(f"\nPage {page + 1}/{MAX_PAGES} | Time: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
                print(f"Processing: {page_url}")
                
                html = self.get_page(page_url)
                if not html:
                    self.logger.error(f"Failed to fetch page {page + 1}")
                    print(f"Failed to fetch page {page + 1}")
                    break
                
                article_urls = self.parse_news_listing(html, page_url)
                if not article_urls:
                    self.logger.info(f"No articles found on page {page + 1} - end of content")
                    print(f"No articles found - reached end at page {page + 1}")
                    break
                
                total_articles_found += len(article_urls)
                articles_scraped_this_page = 0
                
                print(f"Found {len(article_urls)} articles on this page")
                
                for i, url in enumerate(article_urls, 1):
                    if url in self.processed_urls:
                        continue
                    
                    print(f"  [{i}/{len(article_urls)}] Scraping article {self.total_scraped + 1}...")
                    
                    article_data = self.scrape_article_page(url)
                    
                    if article_data:
                        self.all_articles.append(article_data)
                        self.processed_urls.add(url)
                        articles_scraped_this_page += 1
                        self.total_scraped += 1
                        
                        # Show brief progress
                        title = article_data.get('title', 'No title')[:40]
                        content_len = len(article_data.get('content', ''))
                        docs_count = len(article_data.get('embedded_documents', []))
                        links_count = len(article_data.get('related_content_links', []))
                        
                        status = f"{title}... ({content_len}c"
                        if docs_count > 0:
                            status += f", {docs_count}d"
                        if links_count > 0:
                            status += f", {links_count}l"
                        status += ")"
                        print(f"    {status}")
                        
                        # Progress summary every 50 articles
                        if self.total_scraped % 50 == 0:
                            print(f"\nProgress: {self.total_scraped} articles, {self.total_pdfs_processed} PDFs, {self.total_excel_processed} Excel/CSV files")
                    else:
                        print(f"    Failed to scrape")
                
                print(f"Page {page + 1} complete: {articles_scraped_this_page} articles scraped")
                
                # Save progress periodically
                if (page + 1) % SAVE_INTERVAL == 0:
                    print(f"Saving progress after page {page + 1}...")
                    self.save_data()
                    print(f"Saved {len(self.all_articles)} articles so far")
                
                page += 1
                
        except KeyboardInterrupt:
            print("\nScraping interrupted by user")
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            print(f"\nError during scraping: {e}")
            self.logger.error(f"Error in scrape_all_pages: {e}")
            raise

    def print_final_summary(self):
        """Print comprehensive final summary"""
        elapsed = time.time() - self.start_time
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print("\n" + "=" * 60)
        print("FINAL SCRAPING SUMMARY")
        print("=" * 60)
        print(f"Total articles scraped: {len(self.all_articles)}")
        print(f"Total PDFs processed: {self.total_pdfs_processed}")
        print(f"Total Excel/CSV processed: {self.total_excel_processed}")
        print(f"Failed URLs: {len(self.failed_urls)}")
        print(f"Total runtime: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        
        if self.all_articles:
            # Calculate totals
            total_main_content = sum(len(article.get('content', '')) for article in self.all_articles)
            total_embedded_content = sum(
                sum(len(doc.get('content', '')) for doc in article.get('embedded_documents', []))
                for article in self.all_articles
            )
            total_documents = sum(len(article.get('embedded_documents', [])) for article in self.all_articles)
            total_links = sum(len(article.get('related_content_links', [])) for article in self.all_articles)
            
            print(f"\nContent Statistics:")
            print(f"Main content: {total_main_content:,} characters")
            print(f"Embedded content: {total_embedded_content:,} characters")
            print(f"Total content: {total_main_content + total_embedded_content:,} characters")
            print(f"Embedded documents: {total_documents}")
            print(f"Related links: {total_links}")
            
            # Article type breakdown
            type_counts = {}
            for article in self.all_articles:
                article_type = article.get('article_type', 'Unknown')
                type_counts[article_type] = type_counts.get(article_type, 0) + 1
            
            print(f"\nArticle Types:")
            for article_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  {article_type}: {count}")
            
            # Recent articles sample
            print(f"\nRecent Articles (sample):")
            for i, article in enumerate(self.all_articles[:3], 1):
                title = article.get('title', 'No title')[:60]
                pub_date = article.get('published_date', 'No date')[:10]
                print(f"  {i}. {title}... ({pub_date})")
        
        print(f"\nOutput file: {OUTPUT_JSON}")
        print(f"Log file: {LOG_FILE}")
        
        if self.failed_urls:
            print(f"\nFailed URLs (first 10):")
            for url in self.failed_urls[:10]:
                print(f"  - {url}")
            if len(self.failed_urls) > 10:
                print(f"  ... and {len(self.failed_urls) - 10} more")
        
        print("\nProduction scraping completed successfully!")
        self.logger.info(f"Production scraping completed: {len(self.all_articles)} articles")

    def run_production(self):
        """Run the production scraper"""
        try:
            self.scrape_all_pages()
            
        except Exception as e:
            print(f"\nProduction scraping failed: {e}")
            self.logger.error(f"Production run failed: {e}")
            sys.exit(1)
        finally:
            try:
                print(f"\nSaving final data...")
                self.save_data(force_save=True)
                print(f"Final data saved: {len(self.all_articles)} articles")
                
                self.print_final_summary()
                
                if self.session:
                    self.session.close()
                    
            except Exception as e:
                print(f"Error in final cleanup: {e}")
                sys.exit(1)

if __name__ == "__main__":
    print("ACCC News Scraper - Production Version")
    print("This will scrape all 398+ pages from ACCC news centre")
    print("Make sure you've tested with the test version first!")
    
    
    # Check for dependencies
    missing_deps = []
    try:
        import PyPDF2
    except ImportError:
        missing_deps.append("PyPDF2")
    
    try:
        import pandas
    except ImportError:
        missing_deps.append("pandas")
    
    if missing_deps:
        print(f"Missing required dependencies: {', '.join(missing_deps)}")
        print("Install with: pip install " + " ".join(missing_deps))
        sys.exit(1)
    
    # Set environment variable if running from orchestrator
    if len(sys.argv) > 1 and sys.argv[1] == "--orchestrator":
        os.environ['RUNNING_FROM_ORCHESTRATOR'] = 'true'
    
    try:
        scraper = ACCCScraperProduction()
        scraper.run_production()
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)