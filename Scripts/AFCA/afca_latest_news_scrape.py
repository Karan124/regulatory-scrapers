import cloudscraper
import json
import os
import logging
import pandas as pd
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import time

# --- Configuration ---
BASE_URL = "https://www.afca.org.au"
START_URL = f"{BASE_URL}/news/latest-news"
DATA_FOLDER = "data"
JSON_FILE = os.path.join(DATA_FOLDER, "afca_latest_news.json")
CSV_FILE = os.path.join(DATA_FOLDER, "afca_latest_news.csv")
LOG_FILE = "scraper.log"

# --- Logging Setup ---
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

# --- Helper Functions ---

def ensure_data_folder():
    """Create the data folder if it doesn't exist."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        logging.info(f"Created data folder: {DATA_FOLDER}")

def load_existing_data():
    """Loads existing data from JSON for deduplication."""
    if not os.path.exists(JSON_FILE):
        return [], set()
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if not isinstance(data, list): return [], set()
            return data, {item.get('source_url') for item in data if item.get('source_url')}
    except (json.JSONDecodeError, FileNotFoundError):
        return [], set()

def scrape_article_page(url: str, scraper: cloudscraper.CloudScraper) -> dict | None:
    """
    Scrapes an individual news article page using the cloudscraper session.
    """
    logging.info(f"Scraping article: {url}")
    try:
        response = scraper.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'lxml')

        # --- FIX: Use robust selectors on the whole soup object and add checks ---
        title_element = soup.select_one('h1 span.field--name-title')
        if not title_element:
            logging.warning(f"Could not find title element on page: {url}. Skipping article.")
            return None
        title = title_element.get_text(strip=True)

        date_element = soup.select_one('div.field--name-field-news-publish-date')
        if not date_element:
            logging.warning(f"Could not find date element on page: {url}. Skipping article.")
            return None
        publish_date_raw = date_element.get_text(strip=True)
        publish_date = publish_date_raw.removeprefix('Updated:').strip()
        
        content_html_element = soup.select_one('div.field.field--name-body')
        if not content_html_element:
            logging.warning(f"Could not find content body on page: {url}. Skipping article.")
            return None
        # --- END FIX ---

        # --- Extract Content Text and Tables ---
        content_parts = []
        for element in content_html_element.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol', 'table']):
            if element.name == 'table':
                try:
                    df = pd.read_html(str(element), flavor='lxml')[0].fillna('')
                    table_md = df.to_markdown(index=False)
                    content_parts.append(f"\n--- TABLE DATA ---\n{table_md}\n--- END TABLE ---\n")
                except Exception as e:
                    logging.warning(f"Could not parse table on {url}, getting raw text. Error: {e}")
                    content_parts.append(element.get_text(separator='\n', strip=True))
            else:
                content_parts.append(element.get_text(separator=' ', strip=True))

        content_text = "\n\n".join(filter(None, content_parts))
        
        # --- Extract Related Links ---
        related_links = sorted(list(set(
            urljoin(BASE_URL, a['href'])
            for a in content_html_element.find_all('a', href=True)
            if a['href'] and not a['href'].startswith(('#', 'mailto:'))
        )))
        
        return {
            "title": title,
            "publish_date": publish_date,
            "source_url": url,
            "content_text": content_text.strip(),
            "related_links": related_links,
        }

    except Exception as e:
        logging.error(f"An unexpected error occurred while scraping page {url}: {e}")
        return None

# --- Main Scraper Function ---
def main():
    """Main function to orchestrate the scraping process."""
    ensure_data_folder()
    existing_data, existing_urls = load_existing_data()
    logging.info(f"Loaded {len(existing_urls)} existing article URLs for deduplication.")
    
    newly_scraped_articles = []

    scraper = cloudscraper.create_scraper()
    
    current_page_num = 0
    stop_scraping = False

    while not stop_scraping:
        list_page_url = f"{START_URL}?page={current_page_num}"
        logging.info(f"Requesting list page: {list_page_url}")
        
        try:
            response = scraper.get(list_page_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            
            article_rows = soup.select('div.news-row')
            
            if not article_rows:
                if current_page_num == 0:
                    logging.warning("No articles found on the first page. The website structure may have changed.")
                logging.info("No more articles found on this page. Ending pagination.")
                break
            
            found_new_on_this_page = False
            for row in article_rows:
                link_tag = row.select_one('h5.card-title a')
                if not link_tag or not link_tag.has_attr('href'):
                    continue

                relative_url = link_tag['href']
                full_url = urljoin(BASE_URL, relative_url)

                if full_url in existing_urls:
                    logging.debug(f"Skipping already scraped article: {full_url}")
                    continue
                
                found_new_on_this_page = True
                existing_urls.add(full_url)
                
                time.sleep(1) 
                
                article_data = scrape_article_page(full_url, scraper)
                if article_data:
                    newly_scraped_articles.append(article_data)

            if not found_new_on_this_page and current_page_num > 0:
                logging.info("No new articles found on this page (all are duplicates). Stopping pagination.")
                stop_scraping = True
            else:
                current_page_num += 1

        except Exception as e:
            logging.error(f"Failed to process list page {list_page_url}: {e}")
            logging.error("This could be due to a failed Cloudflare challenge or a network issue.")
            stop_scraping = True
    
    # --- Save Data ---
    if newly_scraped_articles:
        logging.info(f"Successfully scraped {len(newly_scraped_articles)} new articles.")
        all_data = existing_data + newly_scraped_articles
        df = pd.DataFrame(all_data)
        try:
            df['publish_date_dt'] = pd.to_datetime(df['publish_date'], format='%d %B %Y', errors='coerce')
            df = df.sort_values(by='publish_date_dt', ascending=False).drop(columns=['publish_date_dt'])
        except Exception as e:
            logging.warning(f"Could not sort data by date: {e}")
        
        df = df.reindex(columns=["title", "publish_date", "source_url", "content_text", "related_links"])
        df.to_json(JSON_FILE, orient='records', indent=4, force_ascii=False)
        logging.info(f"Saved {len(df)} total articles to {JSON_FILE}")
        
        df_csv = df.copy()
        df_csv['related_links'] = df_csv['related_links'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
        df_csv.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        logging.info(f"Saved {len(df_csv)} total articles to {CSV_FILE}")
    else:
        logging.info("No new articles were scraped.")

# --- Execution ---
if __name__ == "__main__":
    main()