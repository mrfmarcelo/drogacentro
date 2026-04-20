import json
import os
import pandas as pd
from datetime import datetime
import warnings

# JSON filenames to be used for building the master catalog.
SOURCE_FILES = 'Drogal/output/Precos_Drogal.json', 'Droga Raia/output/Precos_DrogaRaia.json', 'DrogaVen/output/Precos_Drogaven.json'
# # Text file containing EANs.
# TARGET_EANS = 'Comparativo/Lista_EAN.txt'
# Internal spreadsheet containing source data.
INTERNAL_SHEET = 'Comparativo/caderno.xlsx'


def build_catalog(json_individual_catalog):
    """
    Consolidates data from a JSON file.
a
    Args:
        json_individual_catalog (str): A filename (string) to process.

    Returns:
        dict: A master dictionary with consolidated product information.
    """
    individual_catalog = {}

    with open(json_individual_catalog, 'r', encoding='utf-8') as f:

        try:
            products = json.load(f)
            source_catalog_file = os.path.basename(json_individual_catalog) # Get the filename as the source
            source_catalog_name_ext = (source_catalog_file.split('_')[1]).upper()
            source_catalog_name = (source_catalog_name_ext.split('.')[0]).upper()
            pass
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {json_individual_catalog}. Skipping.")
            return {}
        
    for item in products:
        price = item.get("price")
        ean = item.get("ean")
        name = item.get("name")

        if ean and price:
            individual_catalog[ean] = {
                'ean': ean,
                'name': name,
                'price': price,
                'source': source_catalog_name
            }
    
    return individual_catalog


def find_lowest_price(catalog_list):
    """
    Compares multiple catalogs for the lowest price.
    """
    lowest_catalog = {}

    for catalog in catalog_list:
        if not catalog:
            continue

        for ean_key in catalog:
            if not lowest_catalog.get(ean_key):
                lowest_catalog[ean_key] = catalog[ean_key]
                continue

            if catalog[ean_key]['price'] < lowest_catalog[ean_key]['price']:
                lowest_catalog[ean_key] = catalog[ean_key]

    return lowest_catalog


def filter_catalog(master_catalog, target_eans_file):
    """
    Filters the master catalog to include only products with EANs
    from the specified text file.

    Args:
        master_catalog (dict): The consolidated dictionary of all products.
        target_eans_file (str): The filename of the text file containing
                                 the EANs to filter by.

    Returns:
        dict: A new dictionary containing only the filtered products.
    """
    if not os.path.exists(target_eans_file):
        print(f"Error: Target EANs file not found - {target_eans_file}")
        return {}
    






    
    target_eans = set()
            # Import internal spreadsheet
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default",
            category=UserWarning
        )
        df = pd.read_excel(target_eans_file, usecols=['Cod. Barras'], dtype=str, engine='openpyxl')
    # Renames columns
    column_names = ['ean']
    df.columns = column_names
    df_cleaned = df.dropna()
    # Import as list of dicts and convert to dict of dicts
    internal_as_list = df_cleaned.to_dict(orient='records')

    for excel_line in internal_as_list:
        value_from_entry = excel_line['ean']
        target_eans.add(value_from_entry)

    filtered_catalog = {}
    for ean, data in master_catalog.items():
        if ean in target_eans:
            filtered_catalog[ean] = data
            
    return filtered_catalog


def compare(*scraped_data):

    individual_catalogs = [build_catalog(json_filename) for json_filename in scraped_data]
    compared_catalog = find_lowest_price(individual_catalogs)
    processed_catalog = filter_catalog(compared_catalog, INTERNAL_SHEET)
        
    return processed_catalog


def main():


    for source_file in (SOURCE_FILES):
        if not os.path.exists(source_file):
            print(f"Warning: File not found - {source_file}. Stopping.")
            return

    consolidated_data = compare(*SOURCE_FILES)


    date_str = datetime.now().strftime("%Y-%m-%d")
    json_filepath = (f"menor_preco.json")
    csv_filepath = (f"menor_preco.csv")
    xlsx_filepath = (f"menor_preco.xlsx")

    if consolidated_data:
        with open(json_filepath, 'w', encoding='utf-8') as f:
            json.dump(consolidated_data, f, indent=2, ensure_ascii=False)
        print(f"\nDados salvos em: {json_filepath}")

        data_list = [v for v in consolidated_data.values()]
        df = pd.DataFrame(data_list)
        df.rename(columns={
            "price": "Menor preço (R$)",
            "ean": "EAN",
            "name": "Produto",
            "source": "Origem"
        }, inplace=True)

        df = df[["EAN", "Menor preço (R$)", "Origem", "Produto"]]

        df.to_csv(csv_filepath, sep=';', index=False)
        print(f"Dados salvos em: {csv_filepath}.")

        df.to_excel(xlsx_filepath, index=False)
        print(f"Dados salvos em: {xlsx_filepath}.")
    else:
        print("Nenhum dado para salvar.")


if __name__ == "__main__":
    main()


