import json
import os

def build_catalog(json_individual_catalog):
    """
    Consolidates data from a JSON file.

    Args:
        json_individual_catalog (str): A filename (string) to process.

    Returns:
        dict: A master dictionary with consolidated product information.
    """
    individual_catalog = {}

    if not os.path.exists(json_individual_catalog):
        print(f"Warning: File not found - {json_individual_catalog}. Skipping.")
        return
    
    with open(json_individual_catalog, 'r', encoding='utf-8') as f:

        try:
            products = json.load(f)
            source_catalog_name = os.path.basename(json_individual_catalog) # Get the filename as the source
            pass
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {json_individual_catalog}. Skipping.")
            return
        
    for item in products:
        url = item.get("url")
        price = item.get("price")
        ean = item.get("ean")
        name = item.get("name")

        if ean and price:
            individual_catalog[ean] = {
                 'name': name,
                 'price': price,
                 'url': url,
                 'source': source_catalog_name
            }
    
    return individual_catalog


def find_lowest_price(catalog_list):
    """
    Compares multiple catalogs for the lowest price.

    Args:
        catalog_list (list): list of catalog dicts.

    Returns:
        dict: new catalog with the lowest value for price.
    """
    lowest_catalog = {}

    for catalog in catalog_list:
        for ean_key in catalog:
            if not lowest_catalog[ean_key]:
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
    with open(target_eans_file, 'r', encoding='utf-8') as f:
        for line in f:
            # Strip whitespace and add to the set
            target_eans.add(line.strip())

    filtered_catalog = {}
    for ean, data in master_catalog.items():
        if ean in target_eans:
            filtered_catalog[ean] = data
            
    return filtered_catalog


def compare(*scraped_data):
    pass


# if __name__ == "__main__":
#     # Task 1: Consolidate data and find lowest prices
#     files_to_process = ['site_a.json', 'site_b.json', 'site_c.json']
#     consolidated_data = consolidate_prices(files_to_process)

#     # Save the consolidated data to a new file
#     with open('consolidated_prices.json', 'w', encoding='utf-8') as f:
#         json.dump(consolidated_data, f, ensure_ascii=False, indent=2)
    
#     print("Consolidation complete. Output saved to 'consolidated_prices.json'.")

#     # Task 2: Filter for a subset of items
#     # For this example, let's assume 'ean_list.txt' contains a list of EANs
#     # e.g., 'A00000', 'B00001', 'C00002' on separate lines.
#     target_list_file = 'ean_list.txt'
    
#     filtered_data = filter_catalog(consolidated_data, target_list_file)
    
#     # Save the filtered data to a separate file
#     with open('filtered_prices.json', 'w', encoding='utf-8') as f:
#         json.dump(filtered_data, f, ensure_ascii=False, indent=2)
    
#     print("Filtering complete. Output saved to 'filtered_prices.json'.")
