import json
import os
import time
import pandas as pd
from datetime import datetime
import warnings

# External catalog containing compared prices.
OUTSIDE_CATALOG = 'menor_preco.json'
# Internal spreadsheet containing source data.
INTERNAL_SHEET = 'Comparativo/caderno.xlsx'

def dict_handler(external, internal):

    internal_compared_catalog = {}
    
    for product in internal:
        if product in external:
            if float(external[product]['price']) < internal[product]['price']:
                winner = external[product]['source']
            else:
                winner = 'DROGACENTRO'
            internal_compared_catalog[product] = {
                'ean': product,
                'category': internal[product]['category'],
                'best_external_price': float(external[product]['price']),
                'best_external': external[product]['source'],
                'best': winner,
                'internal_price': internal[product]['price'],
                'best_price': min(float(external[product]['price']), internal[product]['price']),
                'name': internal[product]['name'],
            }
    
    return internal_compared_catalog


def save_data_to_files(data, output_dir="FINAL"):
    """
    Salva os dados em JSON, CSV e XLSX, com a data no nome
    """
    os.makedirs(output_dir, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    json_filepath = os.path.join(output_dir, f"Comparativo_{date_str}.json")
    csv_filepath = os.path.join(output_dir, f"Comparativo_{date_str}.csv")
    xlsx_filepath = os.path.join(output_dir, f"Comparativo_{date_str}.xlsx")

    with open(json_filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nData saved as: {json_filepath}")

    if data:
        df = pd.DataFrame(data).T
        column_names = ['Cod. Barras', 'Curva', 'Preço Concorrente', 'Concorrente', 'Vencedor', 'Nosso Preço', 'Menor Preço', 'Produto']
        df.columns = column_names


        df.to_csv(csv_filepath, sep=';', index=False)
        print(f"Data saved as: {csv_filepath}.")

        df.to_excel(xlsx_filepath, index=False)
        print(f"Data saved as: {xlsx_filepath}.")
    else:
        print("No data to save.")


def main():
    start_time = time.perf_counter()
   

    # Import external catalog
    with open(OUTSIDE_CATALOG, 'r', encoding='utf-8') as f:
        external_dict = json.load(f)

    # Import internal spreadsheet
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default",
            category=UserWarning
        )
        df = pd.read_excel(INTERNAL_SHEET, usecols='A, B, C, J', dtype=str, engine='openpyxl')
    # Renames columns
    column_names = ['ean', 'category', 'name', 'price']
    df.columns = column_names
    df = df.astype({'price': float})
    # Import as list of dicts and convert to dict of dicts
    internal_as_list = df.to_dict(orient='records')
    internal_as_dict = {product_row['ean']: product_row for product_row in internal_as_list}

    # Compares
    processed_catalog = dict_handler(external_dict, internal_as_dict)

    # Save data to files
    save_data_to_files(processed_catalog)

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"""
          Comparação:
    Tempo total: {total_time:.2f} segundos
        """)
    

if __name__ == "__main__":
    main()

