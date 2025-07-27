import requests
from bs4 import BeautifulSoup
import re
import json
import time
import concurrent.futures
import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm

# --- Módulos necessários ---
# python -m pip install requests beautifulsoup4 lxml tqdm pandas

# --- Configuração ---
ROOT_SITEMAP_URL = "https://www.drogal.com.br/sitemap.xml"
PRODUCT_SITEMAP_REGEX = r"https://www\.drogal\.com\.br/sitemap/product-\d+\.xml"

# Número máximo de threads para scraping paralelo
MAX_WORKERS = 200  # Ajuste conforme a capacidade do seu sistema e tolerância do site

# Testar scraping: True para testar, False para scraping normal
TEST_RUN = True
SAMPLE_SIZE = 100  # Número de URLs a serem utilizadas no modo de teste

# Seletores CSS para extração de dados
PRICE_SELECTOR = ".undefined.drogal-product-page-0-x-drogal-product-page-product-base-price div"
FALLBACK_PRICE_SELECTOR = "div.drogal-product-page-0-x-drogal-product-page-product-base-price span.drogal-product-page-0-x-drogal-product-page-selling-price"
GENERAL_PRICE_CONTAINER = "div.drogal-product-page-0-x-drogal-product-page-product-base-price"
TAG_SELECTOR = 'script[type="application/ld+json"]'
EAN_SELECTOR = 'template[data-type="json"][data-varname="__STATE__"] > script'

# Headers para simular um navegador e evitar bloqueio
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}

print('\n --- Drogal Scraper ---\n')

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

def get_product_sitemap_urls(root_sitemap_url):
    """
    Lê o sitemap base e extrai as URLs dos sitemaps de produtos
    """
    print(f"Extraindo sitemap base: {root_sitemap_url}")
    xml_content = fetch_url(root_sitemap_url)
    if not xml_content:
        print("Não foi possível baixar o sitemap base.")
        return []

    soup = BeautifulSoup(xml_content, 'xml')
    product_sitemap_urls = []
    for loc_tag in soup.find_all('loc'):
        sitemap_url = loc_tag.get_text()
        if re.match(PRODUCT_SITEMAP_REGEX, sitemap_url):
            product_sitemap_urls.append(sitemap_url)
    time.sleep(1)
    return product_sitemap_urls

def extract_product_urls_from_sitemap(sitemap_url):
    """
    Extrai as URLs de produtos de um sitemap XML
    Os sitemaps devem ter a tahg <loc> nas URLs
    """
    print(f"Baixando sitemap: {sitemap_url}")
    xml_content = fetch_url(sitemap_url)
    if not xml_content:
        return []

    soup = BeautifulSoup(xml_content, 'xml')
    urls = [loc_tag.get_text() for loc_tag in soup.find_all('loc')]
    time.sleep(1)
    return urls

def extract_price_element(soup_obj):
    """
    Extrai o elemento de preço a partir do objeto BeautifulSoup
    """
    price_element = soup_obj.select_one(PRICE_SELECTOR)
    if price_element:
        return price_element

    price_element = soup_obj.select_one(FALLBACK_PRICE_SELECTOR)
    if price_element:
        return price_element

    general_container = soup_obj.select_one(GENERAL_PRICE_CONTAINER)
    if not general_container:
        return None

    potential_price_elements = general_container.find_all(['span', 'div'])
    for element in potential_price_elements:
        price_text = element.get_text(strip=True)
        match = re.search(r'(\d[\d.,]*)', price_text)
        if match:
            return element
    return None

def parse_product_page(html_content, url):
    """
    Lê a página do produto e extrai preço, EAN e nome a partir de uma URL
    Retorna um dicionário com as informações do produto ou None se o produto não estiver disponível
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    product_data = {"url": url, "price": None, "ean": None, "name": None}

    price_element = extract_price_element(soup)
    if price_element:
        price_text = price_element.get_text(strip=True)
        match = re.search(r'(\d[\d.,]*)', price_text)
        if match:
            cleaned_price = match.group(1).replace('.', '').replace(',', '.')
            try:
                product_data["price"] = float(cleaned_price)
            except ValueError:
                product_data["price"] = price_text
        else:
            product_data["price"] = price_text

    json_ld_content = None
    json_string = soup.select_one(TAG_SELECTOR)
    if json_string and json_string.string:
        try:
            json_ld_content = json.loads(json_string.string)
            product_data["name"] = json_ld_content.get('name')
        except (json.JSONDecodeError, AttributeError):
            pass

    ean_script = soup.select_one(EAN_SELECTOR)
    try:
        ean_json = json.loads(ean_script.string)
        for key, value in ean_json.items():
            if not key.endswith('.items.0') or not isinstance(value, dict):
                continue
            ean_candidate = value.get('ean')
            if ean_candidate:
                product_data["ean"] = ean_candidate
                break
    except json.JSONDecodeError:
        pass

    if (product_data["ean"] or product_data["name"]) and (product_data["price"] is None or product_data["price"] == ""):
        return None

    return product_data

def scrape_single_product(url):
    """
    Função para o worker baixar e processar a URL de um um único produto
    Retorna product_info ou None se o produto não estiver disponível
    """
    html_content = fetch_url(url)
    if not html_content:
        return None

    product_info = parse_product_page(html_content, url)
    time.sleep(2) # Pausa entre os requests
    return product_info

def save_data_to_files(data, output_dir="output"):
    """
    Salva os dados em JSON, CSV e XLSX, com a data no nome
    """
    os.makedirs(output_dir, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    json_filepath = os.path.join(output_dir, f"Preços Drogal {date_str}.json")
    csv_filepath = os.path.join(output_dir, f"Preços Drogal {date_str}.csv")
    xlsx_filepath = os.path.join(output_dir, f"Preços Drogal {date_str}.xlsx")

    with open(json_filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nDados salvos em: {json_filepath}")

    if data:
        df = pd.DataFrame(data)

        df.rename(columns={
            "url": "Link",
            "price": "Preço (R$)",
            "ean": "EAN",
            "name": "Produto"
        }, inplace=True)

        df = df[["EAN", "Produto", "Preço (R$)", "Link"]]

        df.to_csv(csv_filepath, sep=';', index=False)
        print(f"Dados salvos em: {csv_filepath}.")

        df.to_excel(xlsx_filepath, index=False)
        print(f"Dados salvos em: {xlsx_filepath}.")
    else:
        print("Nenhum dado para salvar.")

def main():
    start_time = time.perf_counter()

    # 1: Extrair todas as URLs de sitemap
    product_sitemap_urls = get_product_sitemap_urls(ROOT_SITEMAP_URL)
    if not product_sitemap_urls:
        print("Nenhum sitemap de produto encontrado. Encerrando...")
        return
    
    # 2: Extrair todas as URLs de produtos
    all_product_urls = []
    for sitemap_url in product_sitemap_urls:
        urls_from_sitemap = extract_product_urls_from_sitemap(sitemap_url)
        all_product_urls.extend(urls_from_sitemap)

    # Remover duplicados
    unique_product_urls = list(set(all_product_urls))
    print(f"\nEncontradas {len(unique_product_urls)} URLs de produtos.")

    scraped_products = []
    total_failed_products = 0

    # Iniciar teste ou scraping
    if TEST_RUN:
        urls_to_scrape = unique_product_urls[:SAMPLE_SIZE]
        print(f"Extraindo {len(urls_to_scrape)} URLs de produtos para teste...")
    else:
        urls_to_scrape = unique_product_urls
        print(f"Extraindo {len(urls_to_scrape)} URLs de produtos...")

    # Usar workers para scraping em paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for product_info in tqdm(executor.map(scrape_single_product, urls_to_scrape), total=len(urls_to_scrape), desc="Extraindo Produtos..."):
            if product_info:
                scraped_products.append(product_info)
            else:
                total_failed_products += 1

    # Salvar em arquivos
    save_data_to_files(scraped_products)

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"\nTempo total: {total_time:.2f} segundos.")
    print(f"Total de produtos com sucesso: {len(scraped_products)}.")
    print(f"Total de produtos com falha: {total_failed_products}.")

if __name__ == "__main__":
    main()

