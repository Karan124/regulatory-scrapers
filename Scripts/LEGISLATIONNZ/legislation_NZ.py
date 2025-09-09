#!/usr/bin/env python3
"""
New Zealand Legislation Scraper - Complete Fixed Version
Handles deemed regulations with external content extraction by default
"""

import argparse
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Store:
    """Simple JSON storage with deduplication"""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.dedupe_index = {}
        self._load_existing_data()
    
    def _load_existing_data(self):
        """Load existing data for deduplication"""
        for file_type in ['acts', 'bills', 'secondary_legislation']:
            file_path = self.output_dir / f"{file_type}_nz.json"
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    for item in data:
                        if all(key in item for key in ['type', 'id', 'content_hash']):
                            key = f"{item['type']}:{item['id']}"
                            self.dedupe_index[key] = item['content_hash']
                        
                    logger.info(f"Loaded {len(data)} existing items from {file_type}_nz.json")
                except Exception as e:
                    logger.error(f"Failed to load {file_path}: {e}")
    
    def should_skip(self, item_type: str, item_id: str, content_hash: str) -> bool:
        """Check if item should be skipped"""
        key = f"{item_type}:{item_id}"
        return self.dedupe_index.get(key) == content_hash
    
    def save_item(self, item: Dict) -> str:
        """Save item with atomic write"""
        # Determine file type
        if item['type'] == 'Act':
            filename = 'acts_nz.json'
        elif item['type'] == 'Bill':
            filename = 'bills_nz.json'
        else:
            filename = 'secondary_legislation_nz.json'
        
        file_path = self.output_dir / filename
        
        # Load existing data
        data = []
        if file_path.exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        
        # Check for existing item
        existing_idx = None
        for i, existing_item in enumerate(data):
            if existing_item.get('id') == item['id']:
                existing_idx = i
                break
        
        # Determine action
        if existing_idx is not None:
            if data[existing_idx].get('content_hash') == item['content_hash']:
                return "skipped"
            else:
                data[existing_idx] = item
                action = "updated"
        else:
            data.append(item)
            action = "new"
        
        # Atomic write
        temp_path = file_path.with_suffix('.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        temp_path.replace(file_path)
        
        # Update deduplication index
        key = f"{item['type']}:{item['id']}"
        self.dedupe_index[key] = item['content_hash']
        
        return action


class NZLegislationScraper:
    """NZ Legislation Scraper with comprehensive deemed regulation handling"""
    
    def __init__(self, args):
        self.args = args
        self.driver = None
        self.session = requests.Session()
        self.store = Store(args.out_dir)
        
        # Setup session
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        
        # Stats tracking
        self.stats = {
            'acts': {'new': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'broken_links': 0, 'external_content': 0},
            'bills': {'new': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'broken_links': 0, 'external_content': 0},
            'secondary_legislation': {'new': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'broken_links': 0, 'external_content': 0}
        }
    
    def setup_driver(self):
        """Setup Chrome driver"""
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(
            service=webdriver.chrome.service.Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        
        self.driver.implicitly_wait(10)
        self.driver.set_page_load_timeout(30)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logger.info("Chrome driver setup completed")
    
    def get_search_url(self, page: int = 1) -> str:
        """Get search URL with proper parameters"""
        base_url = "https://www.legislation.govt.nz/all/results.aspx"
        search_params = "search=ad_act%40bill%40regulation%40deemedreg______25_ac%40bc%40rc%40dc%40apub%40aloc%40apri%40apro%40aimp%40bgov%40bloc%40bpri%40bmem%40rpub%40rimp_ac%40bc%40rc%40ainf%40anif%40aaif%40bcur%40bena%40rinf%40rnif%40raif_y_aw_se_"
        return f"{base_url}?{search_params}&p={page}"
    
    def extract_page_links(self, page_num: int) -> List[Dict]:
        """Extract all legislation links from a results page"""
        try:
            url = self.get_search_url(page_num)
            logger.info(f"Extracting links from page {page_num}")
            
            self.driver.get(url)
            
            # Wait for results table
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table[id*='mixedTable']"))
            )
            
            # Get results table
            results_table = self.driver.find_element(By.CSS_SELECTOR, "table[id*='mixedTable']")
            title_links = results_table.find_elements(By.CSS_SELECTOR, "td.resultsTitle a")
            
            links = []
            for link_elem in title_links:
                href = link_elem.get_attribute('href')
                title = link_elem.text.strip()
                
                if href and title:
                    clean_url = href.split('?')[0] if '?' in href else href
                    links.append({
                        'url': clean_url,
                        'title': title
                    })
            
            logger.info(f"Page {page_num}: Found {len(links)} links")
            
            # Log first few for verification
            for i, link in enumerate(links[:3]):
                logger.info(f"  Sample {i+1}: {link['url']}")
            
            time.sleep(self.args.delay_ms / 1000)
            return links
            
        except Exception as e:
            logger.error(f"Failed to extract links from page {page_num}: {e}")
            return []
    
    def extract_item_content(self, item_url: str, item_title: str) -> Optional[Dict]:
        """Extract content from legislation item with comprehensive error handling"""
        try:
            logger.debug(f"Extracting: {item_title[:50]}...")
            
            # Get main page
            response = self.session.get(item_url, timeout=30)
            response.raise_for_status()
            html_content = response.text
            
            # Check if this is a "Page Missing" error page
            if self._is_broken_link(html_content):
                logger.warning(f"Broken link detected: {item_url}")
                # Try alternative URLs for deemed regulations
                if '/deemedreg/' in item_url:
                    alternative_content = self._try_alternative_deemedreg_urls(item_url, item_title)
                    if alternative_content:
                        html_content = alternative_content
                    else:
                        logger.error(f"All alternatives failed for: {item_title[:50]}...")
                        return None
                else:
                    logger.error(f"Page missing for: {item_title[:50]}...")
                    return None
            
            # Determine type and metadata
            item_type = self._determine_type(item_url, item_title)
            year = self._extract_year(item_url, item_title)
            
            # Find whole document URL (skip for deemed regulations)
            whole_url = self._find_whole_url(item_url, html_content)
            
            # Extract content with improved fallback handling
            text_content = ""
            if whole_url:
                text_content = self._extract_whole_content(whole_url)
            
            # Enhanced fallback to main page content
            if len(text_content) < 100:
                logger.debug(f"Using main page content for: {item_title[:50]}...")
                text_content = self._html_to_text(html_content)
            
            # For deemed regulations, try additional extraction methods
            if len(text_content) < 100 and '/deemedreg/' in item_url:
                text_content = self._extract_deemedreg_content(item_url, html_content)
                
            # Extract external content for deemed regulations by default (unless metadata-only flag is set)
            external_content = ""
            external_url = None
            if '/deemedreg/' in item_url and not getattr(self.args, 'metadata_only', False):
                external_url = self._extract_external_url(html_content)
                if external_url:
                    external_content = self._scrape_external_content(external_url, item_title)
                    if external_content:
                        # Combine deemed reg metadata with external content
                        text_content = f"{text_content}\n\n=== EXTERNAL DOCUMENT CONTENT ===\n\n{external_content}"
                        logger.info(f"External content extracted from: {external_url}")
                        stats_key = self._get_stats_key(item_type)
                        self.stats[stats_key]['external_content'] += 1
            
            # Clean up footer content
            text_content = self._clean_footer_content(text_content)
            
            if len(text_content) < 50:
                logger.warning(f"Minimal content for: {item_title}")
                return None
            
            # Create item
            item = {
                "id": self._create_stable_id(item_url),
                "title": item_title.strip(),
                "type": item_type,
                "year": year,
                "status": self._extract_status(html_content),
                "source_url": item_url,
                "whole_text_url": whole_url or item_url,
                "text_content": text_content,
                "content_hash": hashlib.sha256(text_content.encode('utf-8')).hexdigest(),
                "scraped_at": datetime.now().isoformat(),
                "metadata": {
                    "jurisdiction": "NZ",
                    "series": self._extract_series(item_url),
                    "version_label": "latest" if '/latest/' in item_url else None,
                    "amendment_flag": 'amendment' in item_title.lower(),
                    "is_deemed_regulation": '/deemedreg/' in item_url,
                    "external_document_url": external_url,
                    "administering_agency": self._extract_administering_agency(html_content) if '/deemedreg/' in item_url else None
                },
                "related_links": self._extract_related_links(html_content)
            }
            
            time.sleep(self.args.delay_ms / 1000)
            return item
            
        except Exception as e:
            logger.error(f"Content extraction failed for {item_url}: {e}")
            return None
    
    def _is_broken_link(self, html_content: str) -> bool:
        """Detect if the page is a 'Page Missing' error page"""
        indicators = [
            'Page Missing',
            'The page you requested cannot be displayed',
            'page you requested cannot be displayed',
            '<h3 id="ctl00_Cnt_ContentHeader_ContentHeading">Page Missing</h3>'
        ]
        
        content_lower = html_content.lower()
        return any(indicator.lower() in content_lower for indicator in indicators)
    
    def _try_alternative_deemedreg_urls(self, original_url: str, item_title: str) -> Optional[str]:
        """Try alternative URL patterns for deemed regulations"""
        logger.info(f"Trying alternative URLs for: {item_title[:50]}...")
        
        # Extract components from the original URL
        url_parts = original_url.strip('/').split('/')
        
        if len(url_parts) >= 6:
            base_domain = '/'.join(url_parts[:3])  # https://www.legislation.govt.nz
            doc_type = url_parts[3]  # deemedreg
            year = url_parts[4] if len(url_parts) > 4 else None
            number = url_parts[5] if len(url_parts) > 5 else None
            
            if year and number:
                # The CORRECT deemed regulation endpoint patterns
                alternative_patterns = [
                    f"{base_domain}/{doc_type}/{year}/{number}/latest/viewdr.aspx",  # PRIMARY FIX!
                    f"{base_domain}/{doc_type}/{year}/{number}/asmade/viewdr.aspx",
                    f"{base_domain}/{doc_type}/{year}/{number}/viewdr.aspx",
                    f"{base_domain}/{doc_type}/{year}/{number}/asmade/",
                    f"{base_domain}/{doc_type}/{year}/{number}/",
                    f"{base_domain}/regulation/{year}/{number}/latest/",
                    f"{base_domain}/regulation/{year}/{number}/asmade/",
                ]
                
                # Try each alternative
                for alt_url in alternative_patterns:
                    try:
                        logger.debug(f"Trying alternative: {alt_url}")
                        response = self.session.get(alt_url, timeout=30)
                        response.raise_for_status()
                        
                        if not self._is_broken_link(response.text):
                            logger.info(f"Alternative URL successful: {alt_url}")
                            return response.text
                            
                    except Exception as e:
                        logger.debug(f"Alternative failed {alt_url}: {e}")
                        continue
        
        logger.warning(f"No working alternatives found for: {original_url}")
        return None
    
    def _determine_type(self, url: str, title: str) -> str:
        """Determine legislation type"""
        if '/act/' in url:
            return "Act"
        elif '/bill/' in url:
            return "Bill"
        elif '/regulation/' in url or '/deemedreg/' in url:
            return "Secondary Legislation"
        else:
            # Fallback to title analysis
            title_lower = title.lower()
            if 'bill' in title_lower:
                return "Bill"
            elif any(word in title_lower for word in ['regulation', 'order', 'rules', 'notice']):
                return "Secondary Legislation"
            else:
                return "Act"
    
    def _extract_year(self, url: str, title: str) -> Optional[str]:
        """Extract year from URL or title"""
        # URL first
        match = re.search(r'/(\d{4})/', url)
        if match:
            return match.group(1)
        
        # Title fallback
        match = re.search(r'\b(19|20)\d{2}\b', title)
        if match:
            return match.group(0)
        
        return None
    
    def _extract_status(self, html_content: str) -> str:
        """Extract status from HTML content"""
        content_lower = html_content.lower()
        if 'in force' in content_lower:
            return "In force"
        elif 'not yet in force' in content_lower:
            return "Not yet in force"
        elif 'current' in content_lower:
            return "Current"
        else:
            return "Unknown"
    
    def _extract_series(self, url: str) -> Optional[str]:
        """Extract series from URL"""
        if '/public/' in url:
            return "Public"
        elif '/private/' in url:
            return "Private"
        elif '/local/' in url:
            return "Local"
        elif '/government/' in url:
            return "Government"
        elif '/deemedreg/' in url:
            return "Deemed Regulation"
        return None
    
    def _find_whole_url(self, item_url: str, html_content: str) -> Optional[str]:
        """Find whole document URL with special handling for deemed regulations"""
        # For deemed regulations, don't try whole.html as it usually doesn't exist
        if '/deemedreg/' in item_url:
            logger.debug(f"Skipping whole.html for deemed regulation: {item_url}")
            return None
        
        patterns = [
            r'href="([^"]*whole\.html[^"]*)"',
            r'tabWholeAct[^>]+href="([^"]+)"'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                whole_path = match.group(1)
                return urljoin(item_url, whole_path)
        
        # Construct from URL (but not for deemed regulations)
        if '/latest/' in item_url and '/deemedreg/' not in item_url:
            base = item_url.split('/latest/')[0]
            return f"{base}/latest/whole.html"
        
        return None
    
    def _extract_whole_content(self, whole_url: str) -> str:
        """Extract content from whole document with fallback handling"""
        try:
            response = self.session.get(whole_url, timeout=30)
            response.raise_for_status()
            return self._html_to_text(response.text)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Whole content extraction failed: {e}")
                # Try removing '/whole.html' and get main content
                if whole_url.endswith('/whole.html'):
                    main_url = whole_url.replace('/whole.html', '')
                    return self._extract_fallback_content(main_url)
            else:
                logger.warning(f"Whole content extraction failed: {e}")
            return ""
        except Exception as e:
            logger.warning(f"Whole content extraction failed: {e}")
            return ""
    
    def _extract_fallback_content(self, main_url: str) -> str:
        """Extract content from main URL when whole.html fails"""
        try:
            response = self.session.get(main_url, timeout=30)
            response.raise_for_status()
            return self._html_to_text(response.text)
        except Exception as e:
            logger.warning(f"Fallback content extraction failed for {main_url}: {e}")
            return ""
    
    def _extract_deemedreg_content(self, item_url: str, html_content: str) -> str:
        """Special handling for deemed regulation content extraction"""
        
        # Check if this is a deemed regulation pointer page
        if 'deemedRegContent' in html_content or 'deemedRegDisclaimer' in html_content:
            return self._extract_deemedreg_metadata(html_content)
        
        # Otherwise, try pattern matching for content
        patterns_to_extract = [
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*id="[^"]*content[^"]*"[^>]*>(.*?)</div>',
            r'<section[^>]*>(.*?)</section>',
            r'<article[^>]*>(.*?)</article>',
            r'<main[^>]*>(.*?)</main>'
        ]
        
        for pattern in patterns_to_extract:
            matches = re.findall(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if matches:
                content = ' '.join(matches)
                text = self._html_to_text(content)
                if len(text) > 100:
                    logger.debug("Extracted deemed regulation content using pattern matching")
                    return text
        
        # If pattern matching fails, just return the full page content cleaned
        return self._html_to_text(html_content)
    
    def _extract_deemedreg_metadata(self, html_content: str) -> str:
        """Extract metadata from deemed regulation pointer pages"""
        logger.debug("Extracting deemed regulation metadata")
        
        metadata_parts = []
        
        # Extract external URL
        external_url_match = re.search(r'<a[^>]*class="deemedRegDescr"[^>]*href="([^"]+)"[^>]*>([^<]+)</a>', html_content)
        if external_url_match:
            url = external_url_match.group(1)
            url_text = external_url_match.group(2)
            metadata_parts.append(f"External Document URL: {url}")
            metadata_parts.append(f"External URL Display: {url_text}")
        
        # Extract administrative metadata
        metadata_fields = {
            'Authorising Act': r'Authorising Act:.*?<span[^>]*>([^<]+)</span>',
            'Category': r'Category:.*?<span[^>]*>([^<]+)</span>',
            'Year': r'Year:.*?<span[^>]*>([^<]+)</span>',
            'Administered by': r'Administered by:.*?<span[^>]*>([^<]+)</span>',
            'Address for printed copies': r'Address for printed copies:.*?<span[^>]*>([^<]+)</span>'
        }
        
        for field_name, pattern in metadata_fields.items():
            match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if match:
                value = match.group(1).strip()
                metadata_parts.append(f"{field_name}: {value}")
        
        # Extract disclaimer text
        disclaimer_match = re.search(r'<div class="deemedRegDisclaimer"[^>]*>(.*?)</div>', html_content, re.DOTALL | re.IGNORECASE)
        if disclaimer_match:
            disclaimer_text = self._html_to_text(disclaimer_match.group(1))
            metadata_parts.append(f"Important Notice: {disclaimer_text}")
        
        # Combine all metadata
        if metadata_parts:
            content = "DEEMED REGULATION METADATA:\n\n" + "\n\n".join(metadata_parts)
            content += "\n\nNote: This is a deemed regulation that points to external documents. The actual regulatory content is hosted by the administering agency at the external URL provided above."
            return content
        else:
            return self._html_to_text(html_content)
    
    def _scrape_external_content(self, external_url: str, item_title: str) -> str:
        """Scrape content from external deemed regulation documents"""
        try:
            logger.debug(f"Scraping external content: {external_url}")
            
            # Get the external page
            response = self.session.get(external_url, timeout=30)
            response.raise_for_status()
            html_content = response.text
            
            # Extract structured content based on the site
            if 'fma.govt.nz' in external_url:
                return self._extract_fma_content(html_content)
            elif 'legislation.govt.nz' in external_url:
                return self._html_to_text(html_content)
            elif 'gazette.govt.nz' in external_url:
                return self._extract_gazette_content(html_content)
            else:
                # Generic extraction for other government sites
                return self._extract_generic_external_content(html_content)
                
        except Exception as e:
            logger.warning(f"External content extraction failed for {external_url}: {e}")
            return ""
    
    def _extract_customs_content(self, html_content: str, item_title: str) -> str:
        """Extract content from NZ Customs website"""
        logger.debug(f"Extracting Customs content for: {item_title}")
        
        # If this is a document listing page, try to find the specific document
        if 'legal-documents' in html_content.lower() and 'showing' in html_content.lower():
            logger.debug("Detected Customs document listing page - searching for specific document")
            
            # Try to find links that might match our document title
            # Look for parts of the title in links
            title_keywords = self._extract_title_keywords(item_title)
            
            # Find all document links on the page
            doc_links = re.findall(r'href="([^"]*(?:\.pdf|/order|/notice|/document)[^"]*)"[^>]*>([^<]+)', html_content, re.IGNORECASE)
            
            best_match = None
            best_score = 0
            
            for url, link_text in doc_links:
                # Score based on keyword matches
                score = self._score_document_match(title_keywords, link_text.lower())
                if score > best_score:
                    best_score = score
                    best_match = url
            
            if best_match and best_score > 2:  # Require at least 2 keyword matches
                logger.info(f"Found specific document link: {best_match}")
                return self._fetch_specific_customs_document(best_match, item_title)
        
        # Fallback: extract what we can from the current page
        content_parts = []
        
        # Look for main content areas
        main_content_patterns = [
            r'<main[^>]*>(.*?)</main>',
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
            r'<article[^>]*>(.*?)</article>',
        ]
        
        for pattern in main_content_patterns:
            matches = re.findall(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if matches:
                main_content = ' '.join(matches)
                # Remove navigation and filter elements
                main_content = re.sub(r'<nav[^>]*>.*?</nav>', '', main_content, flags=re.DOTALL | re.IGNORECASE)
                main_content = re.sub(r'<div[^>]*class="[^"]*filter[^"]*"[^>]*>.*?</div>', '', main_content, flags=re.DOTALL | re.IGNORECASE)
                
                text = self._html_to_text(main_content)
                if len(text) > 200:
                    content_parts.append(f"CUSTOMS WEBSITE CONTENT:\n{text}")
                    break
        
        # Look for document lists if we couldn't find the specific document
        if not content_parts:
            list_pattern = r'<(?:ul|ol)[^>]*class="[^"]*document[^"]*"[^>]*>(.*?)</(?:ul|ol)>'
            matches = re.findall(list_pattern, html_content, re.DOTALL | re.IGNORECASE)
            if matches:
                for match in matches:
                    list_text = self._html_to_text(match)
                    if len(list_text) > 100:
                        content_parts.append(f"AVAILABLE DOCUMENTS:\n{list_text}")
        
        if content_parts:
            result = "\n\n".join(content_parts)
            result += f"\n\nNote: This content was extracted from the Customs website listing page. The specific document '{item_title}' may require direct access through the Customs website."
            return result
        else:
            return f"No specific content found for '{item_title}' on Customs website. This may be a document listing page that requires manual navigation to find the specific regulatory document."
    
    def _extract_title_keywords(self, title: str) -> List[str]:
        """Extract meaningful keywords from document title"""
        # Remove common words and extract meaningful terms
        stop_words = {'and', 'or', 'the', 'a', 'an', 'in', 'on', 'at', 'by', 'for', 'with', 'to', 'of', 'act', 'order', 'notice', 'amendment'}
        
        # Split title and clean
        words = re.findall(r'\b\w+\b', title.lower())
        keywords = [word for word in words if len(word) > 2 and word not in stop_words]
        
        return keywords[:6]  # Return top 6 keywords
    
    def _score_document_match(self, title_keywords: List[str], link_text: str) -> int:
        """Score how well a document link matches our title keywords"""
        score = 0
        for keyword in title_keywords:
            if keyword in link_text:
                score += 1
        return score
    
    def _fetch_specific_customs_document(self, doc_url: str, item_title: str) -> str:
        """Fetch a specific document from Customs website"""
        try:
            # Ensure URL is absolute
            if not doc_url.startswith('http'):
                doc_url = f"https://www.customs.govt.nz{doc_url}"
            
            logger.debug(f"Fetching specific Customs document: {doc_url}")
            response = self.session.get(doc_url, timeout=30)
            response.raise_for_status()
            
            # If it's a PDF, note that
            if doc_url.lower().endswith('.pdf'):
                return f"SPECIFIC DOCUMENT FOUND:\nPDF Document: {item_title}\nDirect Link: {doc_url}\n\nNote: This is a PDF document that contains the full regulatory text."
            else:
                # Extract content from the specific document page
                content = self._html_to_text(response.text)
                return f"SPECIFIC DOCUMENT CONTENT:\n{content}"
                
        except Exception as e:
            logger.warning(f"Failed to fetch specific Customs document {doc_url}: {e}")
            return f"Found potential document link but could not access: {doc_url}"
        """Extract structured content from FMA website"""
        content_parts = []
        
        # Extract metadata table
        table_match = re.search(r'<table class="table"[^>]*>(.*?)</table>', html_content, re.DOTALL)
        if table_match:
            table_html = table_match.group(1)
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
            
            metadata = []
            for row in rows:
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                if len(cells) == 2:
                    key = self._html_to_text(cells[0]).strip()
                    value = self._html_to_text(cells[1]).strip()
                    metadata.append(f"{key}: {value}")
            
            if metadata:
                content_parts.append("METADATA:\n" + "\n".join(metadata))
        
        # Extract main content from elemental area
        content_match = re.search(r'<div class="element dnadesign__elemental__models__elementcontent"[^>]*>(.*?)</div>', html_content, re.DOTALL)
        if content_match:
            main_content = self._html_to_text(content_match.group(1))
            if main_content.strip():
                content_parts.append("CONTENT:\n" + main_content.strip())
        
        # Extract PDF links
        pdf_links = re.findall(r'href="([^"]*\.pdf[^"]*)"[^>]*>([^<]+)', html_content, re.IGNORECASE)
        if pdf_links:
            pdf_info = []
            for url, description in pdf_links:
                if not url.startswith('http'):
                    url = f"https://www.fma.govt.nz{url}"
                pdf_info.append(f"PDF Document: {description.strip()} - {url}")
            content_parts.append("DOCUMENTS:\n" + "\n".join(pdf_info))
        
        return "\n\n".join(content_parts) if content_parts else ""
    
    def _extract_gazette_content(self, html_content: str) -> str:
        """Extract content from NZ Gazette"""
        # Look for gazette-specific content patterns
        content_patterns = [
            r'<div[^>]*class="[^"]*gazette[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*notice[^"]*"[^>]*>(.*?)</div>',
            r'<article[^>]*>(.*?)</article>',
        ]
        
        for pattern in content_patterns:
            matches = re.findall(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if matches:
                content = ' '.join(matches)
                text = self._html_to_text(content)
                if len(text) > 100:
                    return text
        
        return self._html_to_text(html_content)
    
    def _extract_generic_external_content(self, html_content: str) -> str:
        """Generic extraction for government external sites"""
        # Remove common navigation and footer elements
        content = html_content
        
        # Remove nav, header, footer, sidebar elements
        remove_patterns = [
            r'<nav[^>]*>.*?</nav>',
            r'<header[^>]*>.*?</header>',
            r'<footer[^>]*>.*?</footer>',
            r'<aside[^>]*>.*?</aside>',
            r'<div[^>]*class="[^"]*nav[^"]*"[^>]*>.*?</div>',
            r'<div[^>]*class="[^"]*menu[^"]*"[^>]*>.*?</div>',
        ]
        
        for pattern in remove_patterns:
            content = re.sub(pattern, '', content, flags=re.DOTALL | re.IGNORECASE)
        
        # Try to find main content areas
        main_patterns = [
            r'<main[^>]*>(.*?)</main>',
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*main[^"]*"[^>]*>(.*?)</div>',
            r'<article[^>]*>(.*?)</article>',
        ]
        
        for pattern in main_patterns:
            matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
            if matches:
                main_content = ' '.join(matches)
                text = self._html_to_text(main_content)
                if len(text) > 200:
                    return text
        
        # Fallback to full page content
        return self._html_to_text(content)
    
    def _extract_external_url(self, html_content: str) -> Optional[str]:
        """Extract external document URL from deemed regulation"""
        match = re.search(r'<a[^>]*class="deemedRegDescr"[^>]*href="([^"]+)"', html_content)
        return match.group(1) if match else None
    
    def _extract_administering_agency(self, html_content: str) -> Optional[str]:
        """Extract administering agency from deemed regulation"""
        match = re.search(r'Administered by:.*?<span[^>]*>([^<]+)</span>', html_content, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else None
    
    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML to text"""
        # Remove scripts, styles, nav
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<nav[^>]*>.*?</nav>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Convert to text
        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _clean_footer_content(self, text: str) -> str:
        """Remove footer content that was contaminating the output"""
        if not text:
            return ""
        
        # Remove the specific footer pattern
        footer_patterns = [
            r'The Parliamentary Counsel Office www\.govt\.nz Home Advanced search Browse About this site Contact us News Site map Glossary Accessibility Copyright Privacy Disclaimer',
            r'The Parliamentary Counsel Office.*?Privacy Disclaimer.*?',
            r'www\.govt\.nz.*?Disclaimer.*?',
            r'Home Advanced search Browse.*?Disclaimer.*?'
        ]
        
        for pattern in footer_patterns:
            text = re.sub(pattern, '', text, flags=re.DOTALL | re.IGNORECASE)
        
        # Clean up whitespace
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _extract_related_links(self, html_content: str) -> List[str]:
        """Extract related links"""
        pattern = r'href="([^"]*(?:act|bill|regulation)[^"]*)"'
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        
        links = []
        for match in matches:
            url = urljoin('https://www.legislation.govt.nz', match) if match.startswith('/') else match
            if (url.startswith('https://www.legislation.govt.nz') and
                any(path in url for path in ['/act/', '/bill/', '/regulation/']) and
                'results.aspx' not in url):
                links.append(url)
        
        return list(dict.fromkeys(links))[:10]
    
    def _create_stable_id(self, url: str) -> str:
        """Create stable ID"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        if len(path_parts) >= 4:
            return '/'.join(path_parts[:5])
        return parsed.path
    
    def run(self) -> bool:
        """Run the scraper"""
        start_time = datetime.now()
        logger.info(f"Starting scrape at {start_time.isoformat()}")
        
        try:
            self.setup_driver()
            
            # Extract all links from all pages
            all_links = []
            for page in range(1, self.args.max_page + 1):
                page_links = self.extract_page_links(page)
                if not page_links:
                    logger.info(f"No results found on page {page}")
                    break
                all_links.extend(page_links)
            
            if not all_links:
                logger.error("No links found")
                return False
            
            # Remove duplicates
            unique_links = list({link['url']: link for link in all_links}.values())
            logger.info(f"Processing {len(unique_links)} unique items")
            
            # Show breakdown by type
            type_counts = {}
            for link in unique_links:
                item_type = self._determine_type(link['url'], link['title'])
                type_counts[item_type] = type_counts.get(item_type, 0) + 1
            
            logger.info(f"Link breakdown: {type_counts}")
            
            # Process each item
            processed = 0
            for i, link in enumerate(unique_links, 1):
                logger.info(f"Processing {i}/{len(unique_links)}: {link['title'][:50]}...")
                
                item = self.extract_item_content(link['url'], link['title'])
                if item:
                    stats_key = self._get_stats_key(item['type'])
                    
                    if self.store.should_skip(item['type'], item['id'], item['content_hash']):
                        self.stats[stats_key]['skipped'] += 1
                        logger.info(f"Skipped: {item['title'][:40]}...")
                    else:
                        action = self.store.save_item(item)
                        self.stats[stats_key][action] += 1
                        logger.info(f"{action.capitalize()}: {item['title'][:40]}...")
                    
                    processed += 1
                else:
                    stats_key = self._get_stats_key_from_url(link['url'])
                    if '/deemedreg/' in link['url']:
                        self.stats[stats_key]['broken_links'] += 1
                        logger.warning(f"Broken link: {link['title'][:40]}...")
                    else:
                        self.stats[stats_key]['errors'] += 1
            
            # Log final stats
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=" * 50)
            logger.info("SCRAPING COMPLETED")
            logger.info("=" * 50)
            logger.info(f"Processed: {processed}")
            logger.info(f"Duration: {duration}")
            
            for item_type, stats in self.stats.items():
                if any(stats.values()):
                    logger.info(f"{item_type.replace('_', ' ').title()}:")
                    for stat_name, count in stats.items():
                        logger.info(f"  {stat_name.capitalize()}: {count}")
            
            return True
            
        except Exception as e:
            logger.error(f"Scraper error: {e}")
            return False
        finally:
            if self.driver:
                self.driver.quit()
    
    def _get_stats_key(self, item_type: str) -> str:
        """Map item type to stats key"""
        mapping = {
            'Act': 'acts',
            'Bill': 'bills',
            'Secondary Legislation': 'secondary_legislation'
        }
        return mapping.get(item_type, 'secondary_legislation')
    
    def _get_stats_key_from_url(self, url: str) -> str:
        """Get stats key from URL"""
        if '/act/' in url:
            return 'acts'
        elif '/bill/' in url:
            return 'bills'
        else:
            return 'secondary_legislation'


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="NZ Legislation Scraper")
    parser.add_argument('--max-page', type=int, default=5,
                       help='Maximum pages to process (default: 100)')
    parser.add_argument('--delay-ms', type=int, default=2000,
                       help='Delay between requests (ms) (default: 2000)')
    parser.add_argument('--out-dir', default='./data',
                       help='Output directory (default: ./data)')
    parser.add_argument('--metadata-only', action='store_true',
                       help='Extract only metadata for deemed regulations (faster, no external content)')
    
    args = parser.parse_args()
    
    scraper = NZLegislationScraper(args)
    success = scraper.run()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())