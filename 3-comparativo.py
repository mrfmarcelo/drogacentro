import json
import os
import time
import pandas as pd
from datetime import datetime
import warnings

# Directories
INPUT_DIR = 'input'
OUTPUT_DIR = 'output'

# External catalog containing compared prices.
OUTSIDE_CATALOG = 'Scrape_concorrentes_2025-08-15.json'
# Internal spreadsheet containing source data.
INTERNAL_SHEET = 'Dados_sistema.xlsx'

def dict_handler(external, internal):

    internal_compared_catalog = {}
    
    for product in internal:
        if product in external:
            if float(external[product]['price']) < internal[product]['price']:
                winner = external[product]['source']
                best_price = external[product]['price']
            else:
                winner = 'DROGACENTRO'
                best_price = internal[product]['price']
            internal_compared_catalog[product] = {
                'ean': product,
                'category': internal[product]['category'],
                'name': internal[product]['name'],
                'best_external': external[product]['source'],
                'best_external_price': float(external[product]['price']),
                'internal_price': internal[product]['price'],
                'best': winner,
                'best_price': best_price,
                'class': internal[product]['class'],
            }
    
    return internal_compared_catalog


def save_data_to_files(data, output_dir="output"):
    """
    Salva os dados em JSON, CSV e XLSX, com a data no nome
    """
    os.makedirs(output_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    script_dir = os.path.dirname(__file__)

    json_filepath = os.path.join(script_dir, output_dir, f"Comparativo_{date_str}.json")
    csv_filepath = os.path.join(script_dir, output_dir, f"Comparativo_{date_str}.csv")
    xlsx_filepath = os.path.join(script_dir, output_dir, f"Comparativo_{date_str}.xlsx")

    with open(json_filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nData saved as: {json_filepath}")

    if data:
        df = pd.DataFrame(data).T
        df.columns = ['EAN', 'Curva', 'Produto', 'Concorrente', 'Preço concorrente', 'Nosso preço', 'Ganhador', 'Melhor preço', 'Classificação']

        df.to_csv(csv_filepath, sep=';', index=False)
        print(f"Data saved as: {csv_filepath}.")

        df.to_excel(xlsx_filepath, index=False)
        print(f"Data saved as: {xlsx_filepath}.")
    else:
        print("No data to save.")


def main():
    start_time = time.perf_counter()
    
    script_dir = os.path.dirname(__file__)
    catalog_file = os.path.join(script_dir, INPUT_DIR, OUTSIDE_CATALOG)
    sheet_file = os.path.join(script_dir, INPUT_DIR, INTERNAL_SHEET)

    # Import external catalog
    with open(catalog_file, 'r', encoding='utf-8') as f:
        external_dict = json.load(f)

    # Import internal spreadsheet
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default",
            category=UserWarning
        )
        df = pd.read_excel(sheet_file, usecols='A, B, C, J, Q', dtype=str, engine='openpyxl')
    # Renames columns
    column_names = ['ean', 'category', 'name', 'price', 'class']
    df.columns = column_names
    df = df.astype({'price': float})
    # Import as list of dicts and convert to dict of dicts
    internal_as_list = df.to_dict(orient='records')
    internal_as_dict = {product_row['ean']: product_row for product_row in internal_as_list}

    # Compares
    processed_catalog = dict_handler(external_dict, internal_as_dict)

    # Save data to files
    save_data_to_files(processed_catalog, OUTPUT_DIR)

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"""
          Comparação:
    Tempo total: {total_time:.2f} segundos
        """)
    

if __name__ == "__main__":
    main()

