import requests
from bs4 import BeautifulSoup
import json  # noqa: F401
import time
import concurrent.futures  # noqa: F401
import os  # noqa: F401
import pandas as pd  # noqa: F401
from datetime import datetime  # noqa: F401
from tqdm import tqdm  # noqa: F401
from fake_useragent import UserAgent

# --- Required modules ---
# python -m pip install requests beautifulsoup4 lxml tqdm fake_useragent

# --- Configuration ---
SITEMAP_URL = "https://www.drogaraia.com.br/sitemap/2/sitemap.xml"

# Set the maximum number of worker threads for multi-threading
MAX_WORKERS = 5 # You can adjust this value based on your system's capabilities and website's tolerance

# Control scraping scope: Set to True to scrape all unique URLs, False to scrape a sample
TEST_RUN = True
SAMPLE_SIZE = 100 # Number of URLs to scrape if SCRAPE_ALL_URLS is False
# Selectors for data extraction
PRICE_SELECTOR = 'meta[property="product:price:amount"]'
NAME_SELECTOR = 'meta[property="og:image:alt"]'
EAN_SELECTOR = 'script[type="application/ld+json"]'

# Headers to mimic a browser request and avoid being blocked
ua = UserAgent()
HEADERS = {
    'User-Agent': ua.random,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}


print('\n --- DrogaRaia Scraper ---\n')

# --- Funções acessórias ---

def fetch_url(url):
    """
    Baixa o conteúdo de uma URL com o User-Agent
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException:
        return None


def extract_product_urls_from_sitemap(sitemap_url):
    """
    Extrai as URLs de produtos de um sitemap XML
    O sitemap deve ter a tag <loc> nas URLs
    """
    print(f"Baixando sitemap: {sitemap_url}")
    xml_content = fetch_url(sitemap_url)
    if not xml_content:
        return []

    soup = BeautifulSoup(xml_content, 'xml')

    urls = []
    for url_tag in soup.find_all('url'):
        loc_tag = url_tag.find('loc')
        priority_tag = url_tag.find('priority')

        if priority_tag.get_text() == '1.0':
            urls.append(loc_tag.get_text())
        
    time.sleep(1)
    return urls


# -----------------

def main():
    # start_time = time.perf_counter()

    # Extrair todas as URLs de produtos
    urls_from_sitemap = extract_product_urls_from_sitemap(SITEMAP_URL)

    # Remover duplicados
    unique_product_urls = list(set(urls_from_sitemap))
    print(f"\nEncontradas {len(unique_product_urls)} URLs de produtos.")

    # scraped_products = []
    # no_ean = []
    # total_failed_products = 0

    # Iniciar teste ou scraping
    if TEST_RUN:
        urls_to_scrape = unique_product_urls[:SAMPLE_SIZE]
        print(f"Extraindo {len(urls_to_scrape)} URLs de produtos para teste...")
    else:
        urls_to_scrape = unique_product_urls
        print(f"Extraindo {len(urls_to_scrape)} URLs de produtos...")

# -----------------

if __name__ == "__main__":
    main()

# response = requests.get(SITEMAP_URL, headers=HEADERS, timeout=10)
# print(response)

