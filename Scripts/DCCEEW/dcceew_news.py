#!/usr/bin/env python3
"""
DCCEEW News Scraper
Comprehensive scraper for Department of Climate Change, Energy, the Environment and Water news articles
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import re
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from pathlib import Path
import hashlib
from typing import Dict, List, Optional, Set
import random

# Document processing libraries
try:
    import PyPDF2
    from PyPDF2 import PdfReader
except ImportError:
    print("PyPDF2 not found. Install with: pip install PyPDF2")
    
try:
    import pandas as pd
    import openpyxl
except ImportError:
    print("pandas/openpyxl not found. Install with: pip install pandas openpyxl")

try:
    from fake_useragent import UserAgent
except ImportError:
    print("fake_useragent not found. Install with: pip install fake-useragent")


class DCCEEWNewsScraper:
    def __init__(self, max_pages: Optional[int] = None):
        self.base_url = "https://www.dcceew.gov.au"
        self.news_url = f"{self.base_url}/about/news/all"
        self.max_pages = max_pages  # None = all pages, int = limit pages
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.json_file = self.data_dir / "dcceew_news.json"
        self.downloads_dir = self.data_dir / "downloads"
        self.downloads_dir.mkdir(exist_ok=True)
        
        # Allowed and excluded article types
        self.allowed_types = {"News", "Have your say", "Media Release", "Case Study", "Statement", "Newsletter"}
        self.excluded_types = {"Video", "Digital Story", "Podcast"}
        
        # Initialize session with anti-bot measures
        self.session = self._init_session()
        
        # Setup logging
        self._setup_logging()
        
        # Load existing data
        self.existing_articles = self._load_existing_data()
        
    def _init_session(self) -> requests.Session:
        """Initialize session with anti-bot measures"""
        session = requests.Session()
        
        # Try to get a random user agent
        try:
            ua = UserAgent()
            user_agent = ua.chrome
        except:
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        
        # Browser-like headers
        session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        return session
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_file = self.data_dir / "dcceew_scraper.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='a'),  # Append mode
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("DCCEEW News Scraper initialized")
    
    def _load_existing_data(self) -> Dict[str, Dict]:
        """Load existing articles for deduplication"""
        if self.json_file.exists():
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.logger.info(f"Loaded {len(data)} existing articles")
                return data
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                return {}
        return {}
    
    def _save_data(self):
        """Save articles data to JSON file"""
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(self.existing_articles, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"Saved {len(self.existing_articles)} articles to {self.json_file}")
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
    
    def _generate_article_id(self, url: str, title: str) -> str:
        """Generate unique article ID"""
        content = f"{url}_{title}".encode('utf-8')
        return hashlib.md5(content).hexdigest()
    
    def _visit_homepage(self):
        """Visit homepage to collect cookies and establish session"""
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            self.logger.info("Successfully visited homepage to establish session")
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            self.logger.warning(f"Could not visit homepage: {e}")
    
    def _make_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Random delay to appear human-like
                time.sleep(random.uniform(2, 5))
                return response
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(5, 10))
                else:
                    self.logger.error(f"All retries failed for URL: {url}")
                    return None
    
    def _extract_pdf_text(self, pdf_path: str) -> str:
        """Extract text from PDF file"""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PdfReader(file)
                text = ""
                
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                
                # Clean up text
                text = re.sub(r'\s+', ' ', text).strip()
                return text
                
        except Exception as e:
            self.logger.error(f"Error extracting PDF text from {pdf_path}: {e}")
            return ""
    
    def _extract_excel_text(self, excel_path: str) -> str:
        """Extract text from Excel file"""
        try:
            # Read all sheets
            excel_file = pd.ExcelFile(excel_path)
            all_data = []
            
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Convert DataFrame to readable text format
                sheet_text = f"Sheet: {sheet_name}\n"
                sheet_text += df.to_string(index=False, na_rep='')
                all_data.append(sheet_text)
            
            return "\n\n".join(all_data)
            
        except Exception as e:
            self.logger.error(f"Error extracting Excel text from {excel_path}: {e}")
            return ""
    
    def _extract_csv_text(self, csv_path: str) -> str:
        """Extract text from CSV file"""
        try:
            df = pd.read_csv(csv_path)
            return df.to_string(index=False, na_rep='')
        except Exception as e:
            self.logger.error(f"Error extracting CSV text from {csv_path}: {e}")
            return ""
    
    def _download_and_extract_file(self, file_url: str, filename: str) -> Dict[str, str]:
        """Download and extract content from file"""
        file_path = self.downloads_dir / filename
        
        try:
            # Download the file
            response = self._make_request(file_url)
            if not response:
                return {"filename": filename, "content": "", "error": "Download failed"}
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            self.logger.info(f"Downloaded: {filename}")
            
        except Exception as e:
            self.logger.error(f"Error downloading {file_url}: {e}")
            return {"filename": filename, "content": "", "error": str(e)}
        
        # Extract content based on file type
        file_ext = filename.lower().split('.')[-1]
        content = ""
        
        try:
            if file_ext == 'pdf':
                content = self._extract_pdf_text(str(file_path))
            elif file_ext in ['xlsx', 'xls']:
                content = self._extract_excel_text(str(file_path))
            elif file_ext == 'csv':
                content = self._extract_csv_text(str(file_path))
            else:
                self.logger.warning(f"Unsupported file type: {file_ext}")
                return {"filename": filename, "content": "", "error": f"Unsupported file type: {file_ext}"}
                
        except Exception as e:
            self.logger.error(f"Error extracting content from {filename}: {e}")
            return {"filename": filename, "content": "", "error": str(e)}
        finally:
            # Always delete the file after extraction attempt
            try:
                if file_path.exists():
                    file_path.unlink()
                    self.logger.debug(f"Deleted temporary file: {filename}")
            except Exception as e:
                self.logger.warning(f"Could not delete temporary file {filename}: {e}")
        
        return {"filename": filename, "content": content, "error": None}
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text for LLM processing"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove unwanted characters but keep basic punctuation
        text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)\[\]\"\'\/\@\#\$\%\&\*\+\=\<\>\{\}\|\~\`]', '', text)
        
        # Clean up multiple spaces
        text = re.sub(r'\s{2,}', ' ', text)
        
        return text.strip()
    
    def _extract_links_from_content(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract links from article content"""
        links = []
        content_area = soup.find('div', class_='field--type-text-with-summary')
        
        if content_area:
            for link in content_area.find_all('a', href=True):
                href = link.get('href')
                text = link.get_text(strip=True)
                
                if href and text:
                    full_url = urljoin(self.base_url, href)
                    links.append({
                        'text': text,
                        'url': full_url
                    })
        
        return links
    
    def _extract_related_articles(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract related articles from the bottom of the page"""
        related = []
        
        # Look for the related articles container
        related_section = soup.find('div', id='block-views-block-news-media-block-related-cards')
        
        if related_section:
            # Find all card items within the related section
            card_items = related_section.find_all('div', class_='card-item')
            
            for item in card_items:
                # Look for the title link within each card
                title_link = item.find('h3', class_='cta-link')
                if title_link:
                    link = title_link.find('a')
                    if link:
                        title = link.get_text(strip=True)
                        url = urljoin(self.base_url, link.get('href', ''))
                        
                        # Extract date from the publish-date div
                        date_elem = item.find('div', class_='publish-date')
                        date = None
                        if date_elem:
                            time_elem = date_elem.find('time')
                            if time_elem:
                                date = time_elem.get('datetime')
                        
                        # Extract article type/theme from news-tag
                        article_type = None
                        theme = None
                        news_tag = item.find('div', class_='news-tag')
                        if news_tag:
                            spans = news_tag.find_all('span')
                            if spans:
                                # First span is usually the type, second is theme
                                if len(spans) >= 1:
                                    article_type = spans[0].get_text(strip=True)
                                if len(spans) >= 2:
                                    theme = spans[1].get_text(strip=True)
                        
                        related.append({
                            'title': title,
                            'url': url,
                            'date': date,
                            'type': article_type,
                            'theme': theme
                        })
        
        return related
    
    def _extract_article_content(self, article_url: str) -> Optional[Dict]:
        """Extract full content from an article page"""
        response = self._make_request(article_url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract basic metadata
        title_elem = soup.find('h1', class_='page-title')
        title = title_elem.get_text(strip=True) if title_elem else "No Title"
        
        # Extract date
        date_elem = soup.find('time')
        published_date = date_elem.get('datetime') if date_elem else None
        
        # Extract article type and theme
        article_type = "News"  # default
        theme = None
        
        news_tags = soup.find_all('div', class_='news-tag')
        for tag_div in news_tags:
            links = tag_div.find_all('a')
            for link in links:
                text = link.get_text(strip=True)
                href = link.get('href', '')
                
                if 'type:' in href:
                    article_type = text
                elif 'topic:' in href:
                    theme = text
        
        # Skip if excluded type
        if article_type in self.excluded_types:
            self.logger.debug(f"Skipping excluded article type '{article_type}': {title}")
            return None
        
        # Extract main content
        content_div = soup.find('div', class_='field--type-text-with-summary')
        main_content = ""
        
        if content_div:
            # Remove script and style elements
            for script in content_div(["script", "style"]):
                script.decompose()
            
            main_content = content_div.get_text(separator=' ', strip=True)
        
        # Extract image
        image_url = None
        img_elem = soup.find('img', class_='image-style-cards-responsive')
        if img_elem:
            image_url = urljoin(self.base_url, img_elem.get('src', ''))
        
        # Extract and process downloadable files
        file_contents = []
        file_links = soup.find_all('a', href=True)
        
        for link in file_links:
            href = link.get('href', '')
            if any(ext in href.lower() for ext in ['.pdf', '.xlsx', '.xls', '.csv']):
                file_url = urljoin(self.base_url, href)
                filename = os.path.basename(urlparse(file_url).path)
                
                # Skip audio files
                if any(ext in filename.lower() for ext in ['.mp3', '.wav', '.m4a']):
                    continue
                
                file_data = self._download_and_extract_file(file_url, filename)
                file_contents.append(file_data)
        
        # Extract links from content
        content_links = self._extract_links_from_content(soup)
        
        # Extract related articles
        related_articles = self._extract_related_articles(soup)
        
        # Combine all content
        all_content = main_content
        for file_data in file_contents:
            if file_data['content']:
                all_content += f"\n\n--- Content from {file_data['filename']} ---\n"
                all_content += file_data['content']
        
        article_data = {
            'headline': title,
            'article_type': article_type,
            'theme': theme,
            'published_date': published_date,
            'scraped_date': datetime.now(timezone.utc).isoformat(),
            'url': article_url,
            'image_url': image_url,
            'main_content': self._clean_text(all_content),
            'content_links': content_links,
            'related_articles': related_articles,
            'downloaded_files': file_contents
        }
        
        return article_data
    
    def _extract_articles_from_page(self, page_url: str) -> List[Dict[str, str]]:
        """Extract article links from a news listing page"""
        response = self._make_request(page_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = []
        
        # Find all article links in the views-row divs
        rows = soup.find_all('div', class_='views-row')
        
        for row in rows:
            title_elem = row.find('h3', class_='field-content')
            if title_elem:
                link = title_elem.find('a')
                if link:
                    title = link.get_text(strip=True)
                    url = urljoin(self.base_url, link.get('href', ''))
                    
                    # Extract preview/summary
                    summary_elem = row.find('div', class_='views-field-body')
                    summary = ""
                    if summary_elem:
                        summary = summary_elem.get_text(strip=True)
                    
                    articles.append({
                        'title': title,
                        'url': url,
                        'summary': summary
                    })
        
        return articles
    
    def _get_total_pages(self) -> int:
        """Get total number of pages from pagination"""
        response = self._make_request(self.news_url)
        if not response:
            return 1
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for last page link
        last_link = soup.find('a', title='Go to last page')
        if last_link:
            href = last_link.get('href', '')
            match = re.search(r'page=(\d+)', href)
            if match:
                return int(match.group(1)) + 1  # Pages are 0-indexed
        
        return 1
    
    def scrape_all_articles(self):
        """Main scraping method"""
        self.logger.info("Starting DCCEEW news scraping")
        
        # Visit homepage first for session establishment
        self._visit_homepage()
        
        # Determine how many pages to scrape
        if self.max_pages is None:
            total_pages = self._get_total_pages()
            self.logger.info(f"Found {total_pages} total pages, scraping all")
            pages_to_scrape = total_pages
        else:
            pages_to_scrape = self.max_pages
            self.logger.info(f"Limiting scrape to {pages_to_scrape} pages")
        
        new_articles_count = 0
        skipped_count = 0
        
        # Scrape each page
        for page_num in range(pages_to_scrape):
            page_url = f"{self.news_url}?page={page_num}" if page_num > 0 else self.news_url
            self.logger.info(f"Scraping page {page_num + 1}/{pages_to_scrape}: {page_url}")
            
            articles = self._extract_articles_from_page(page_url)
            self.logger.info(f"Found {len(articles)} articles on page {page_num + 1}")
            
            for article_info in articles:
                article_id = self._generate_article_id(article_info['url'], article_info['title'])
                
                # Skip if already exists
                if article_id in self.existing_articles:
                    self.logger.debug(f"Skipping existing article: {article_info['title']}")
                    skipped_count += 1
                    continue
                
                # Extract full article content
                self.logger.info(f"Processing: {article_info['title']}")
                article_data = self._extract_article_content(article_info['url'])
                
                if article_data:
                    self.existing_articles[article_id] = article_data
                    new_articles_count += 1
                    self.logger.info(f"Successfully processed: {article_data['headline']}")
                    
                    # Save periodically to avoid data loss
                    if new_articles_count % 10 == 0:
                        self._save_data()
                else:
                    # Only log warning if it's not an excluded type (which returns None silently)
                    if article_info['title'] not in [art.get('headline', '') for art in self.existing_articles.values()]:
                        self.logger.warning(f"Failed to extract content for: {article_info['title']}")
        
        # Final save
        self._save_data()
        
        self.logger.info(f"Scraping completed. New articles: {new_articles_count}, Skipped: {skipped_count}, Total: {len(self.existing_articles)}")
        
        return {
            'new_articles': new_articles_count,
            'skipped_articles': skipped_count,
            'total_articles': len(self.existing_articles)
        }


def main():
    """Main function to run the scraper"""
    print("DCCEEW News Scraper")
    print("==================")
    
    # Configuration: Set MAX_PAGES here
    # None = scrape all pages
    # Integer = limit to specific number of pages
    MAX_PAGES = 2  # Change this value as needed
    
    print(f"Max pages to scrape: {'All pages' if MAX_PAGES is None else MAX_PAGES}")
    
    # Initialize and run scraper
    scraper = DCCEEWNewsScraper(max_pages=MAX_PAGES)
    
    try:
        results = scraper.scrape_all_articles()
        
        print("\nScraping Results:")
        print(f"New articles: {results['new_articles']}")
        print(f"Skipped articles: {results['skipped_articles']}")
        print(f"Total articles in database: {results['total_articles']}")
        print(f"Data saved to: {scraper.json_file}")
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        scraper._save_data()
        print("Data saved before exit")
        
    except Exception as e:
        print(f"Error during scraping: {e}")
        logging.error(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()