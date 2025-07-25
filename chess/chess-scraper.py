from bs4 import BeautifulSoup
import requests
import json
import csv
import time
import os
import concurrent.futures
from tqdm import tqdm
import re # Import the re module for regular expressions


# --- Configuration ---
# Base URL for tournament listings with a page placeholder
TOURNAMENT_LIST_URL_TEMPLATE = "https://www.chess.com/tournaments/all?page={}"
# Starting page for scraping
START_PAGE = 1
# Maximum number of pages to check (as chess.com seems to have a limit around 100)
# This acts as a safeguard to prevent infinite loops if the "no results" detection fails.
MAX_PAGES_TO_CHECK = 200 # Set slightly above 100 to confirm empty pages

# Set the maximum number of worker threads for multi-threading
MAX_WORKERS = 10 # Adjust based on your system and website's tolerance

# Selectors for data extraction from a tournament listing page
TABLE_SELECTOR = "table.table-component.table-hover.table-clickable.tournaments-table-component"

# Selectors for data within each <tr> (tournament row)
TOURNAMENT_NAME_SELECTOR = 'td.tournaments-table-name div.tournaments-table-tournament-title'
HREF_SELECTOR = 'td.tournaments-table-name a.tournaments-table-full-td-link'
V_TOOLTIP_SELECTOR = 'td.tournaments-table-name div.tournaments-table-icons span[v-tooltip]'
TEXT_CENTER_TDS_SELECTOR = 'td.table-text-center' # Selects all <td> with this class in a row

# Headers to mimic a browser request and avoid being blocked
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}

# --- Utility Functions ---
def fetch_url(url):
    """
    Fetches the content of a given URL with a User-Agent header.
    Returns the response text or None if an error occurs.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.text
    except requests.exceptions.RequestException:
        return None


def parse_tournament_listing_page(html_content, url):
    """
    Parses the HTML content of a tournament listing page to extract all tournaments.
    Returns a list of dictionaries, where each dictionary represents a tournament.
    Returns an empty list if no tournaments are found on the page or parsing fails.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    tournaments_on_page = []

    tournament_table = soup.select_one(TABLE_SELECTOR)

    if not tournament_table:
        return [] # Return empty list if table is not found

    rows = tournament_table.select('tbody tr') # Use tbody tr for rows within the table body

    if not rows:
        return [] # Return empty list if no rows are found in the table

    for row_num, tournament_row in enumerate(rows):
        # --- Get tournament name ---
        tournament_name_element = tournament_row.select_one(TOURNAMENT_NAME_SELECTOR)
        tournament_name = tournament_name_element.get_text(strip=True) if tournament_name_element else None

        # --- Get table items (Players count, Time control, Rating) ---
        text_center_tds = tournament_row.select(TEXT_CENTER_TDS_SELECTOR)

        players_count_raw = None
        if len(text_center_tds) > 0:
            players_element = text_center_tds[0].select_one('a.tournaments-table-full-td-link')
            players_count_raw = players_element.get_text(strip=True) if players_element else None

        time_control = None
        if len(text_center_tds) > 1:
            time_element = text_center_tds[1].select_one('a.tournaments-table-full-td-link')
            time_control = time_element.get_text(strip=True) if time_element else None

        rating = None
        if len(text_center_tds) > 2:
            rating_element = text_center_tds[2].select_one('a.tournaments-table-full-td-link')
            rating = rating_element.get_text(strip=True) if rating_element else None

        # --- Parse players_count_raw into structured fields ---
        registered_players = None
        total_slots = None
        percentage = None
        empty_slots = None

        if players_count_raw:
            parts = players_count_raw.split('/')
            if len(parts) == 2:
                try:
                    registered_players = int(parts[0].strip())
                except ValueError:
                    registered_players = None

                total_slots_str = parts[1].strip()
                if total_slots_str == '∞':
                    total_slots = '∞'
                    # Percentage and empty_slots are not applicable for infinite slots
                else:
                    try:
                        total_slots = int(total_slots_str)
                        if registered_players is not None:
                            if total_slots > 0:
                                percentage = (registered_players / total_slots) * 100
                                empty_slots = total_slots - registered_players
                            else: # Handle total_slots being 0 to avoid division by zero
                                percentage = 0.0
                                empty_slots = 0
                    except ValueError:
                        total_slots = None


        # --- Get categories (v-tooltip entries) ---
        category_elements = tournament_row.select(V_TOOLTIP_SELECTOR)
        categories = [element['v-tooltip'] for element in category_elements if 'v-tooltip' in element.attrs]

        # --- Get URL ---
        url_element = tournament_row.select_one(HREF_SELECTOR)
        tournament_url = url_element['href'] if url_element and 'href' in url_element.attrs else None

        # --- Filter tournaments and append data ---
        # Only append if it's a "Standard Tournament", "No Vacation" is absent,
        # has a title, and players_count is not "0/" or "1/"
        if (
            "Standard Tournament" in categories and
            "No Vacation" not in categories and
            tournament_name and
            players_count_raw and # Ensure players_count_raw is not None or empty string
            not re.search(r"^(0|1)/", players_count_raw) # Exclude if players_count_raw starts with "0/" or "1/"
        ):
            tournaments_on_page.append({
                "Tournament name": tournament_name,
                "Time control": time_control,
                "Rating": rating,
                "Registered Players": registered_players,
                "Total Slots": total_slots,
                "Percentage Full": '%05.2f'%percentage,
                "Empty Slots": empty_slots,
                "Categories": categories,
                "URL": tournament_url
            })

    return tournaments_on_page


def scrape_single_page_tournaments(page_num):
    """
    Worker function to fetch and parse a single tournament listing page.
    Returns a tuple: (list of tournament dictionaries, page_num)
    Returns ([], page_num) if the page cannot be fetched or contains no tournaments.
    """
    page_url = TOURNAMENT_LIST_URL_TEMPLATE.format(page_num)
    html_content = fetch_url(page_url)
    if not html_content:
        return [], page_num # Return empty list if page fetch fails

    tournaments = parse_tournament_listing_page(html_content, page_url)
    time.sleep(2) # Be polite, wait after fetching and parsing each page
    return tournaments, page_num


def save_data_to_files(data, output_dir="output"):
    """
    Saves the scraped data to JSON and CSV files.
    """
    os.makedirs(output_dir, exist_ok=True) # Create output directory if it doesn't exist

    json_filepath = os.path.join(output_dir, "tournaments.json")
    csv_filepath = os.path.join(output_dir, "tournaments.csv")

    # Save to JSON
    with open(json_filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nScraped data saved to: {json_filepath}")

    # Save to CSV
    if data: # Only proceed if there is data to write
        # Dynamically get headers from the first dictionary
        headers = list(data[0].keys())
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        print(f"Scraped data saved to: {csv_filepath}")
    else:
        print("No data to save to CSV.")


def main():
    start_time = time.perf_counter()

    all_tournaments = []
    total_pages_processed = 0
    total_failed_pages = 0 # Pages that failed to fetch or yielded no tournaments

    print(f"Starting tournament scraping from page {START_PAGE} up to {MAX_PAGES_TO_CHECK} pages.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {} # Maps Future objects to their page numbers

        # Submit initial batch of tasks
        for page_num in range(START_PAGE, START_PAGE + MAX_WORKERS):
            if page_num > MAX_PAGES_TO_CHECK:
                break
            futures[executor.submit(scrape_single_page_tournaments, page_num)] = page_num

        # Use tqdm to monitor the progress of completed futures
        # We set total to MAX_PAGES_TO_CHECK to show progress towards the limit
        with tqdm(total=MAX_PAGES_TO_CHECK, desc="Scraping Pages") as pbar:
            processed_pages = set() # Keep track of pages already processed to avoid double counting
            while futures:
                # Wait for any future to complete
                done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)

                for future in done:
                    page_num = futures.pop(future) # Get the page number for the completed future
                    
                    if page_num not in processed_pages: # Only update pbar once per page
                        pbar.update(1)
                        processed_pages.add(page_num)

                    try:
                        tournaments_from_page, completed_page_num = future.result()
                        total_pages_processed += 1

                        if tournaments_from_page:
                            all_tournaments.extend(tournaments_from_page)
                            # If tournaments were found, submit the next page in sequence
                            next_page_to_submit = max(futures.values(), default=completed_page_num) + 1 if futures else completed_page_num + 1
                            
                            # Ensure we don't submit pages beyond the MAX_PAGES_TO_CHECK
                            if next_page_to_submit <= MAX_PAGES_TO_CHECK:
                                # Only submit if this page hasn't been submitted already (e.g., by another branch)
                                if next_page_to_submit not in processed_pages: # Check against processed_pages set
                                    futures[executor.submit(scrape_single_page_tournaments, next_page_to_submit)] = next_page_to_submit
                        else:
                            # If no tournaments found, it's likely the end of pagination for this branch.
                            # We don't submit new tasks from this branch.
                            print(f"\nInfo: Page {completed_page_num} yielded no tournaments. Assuming end of content for this branch.")
                            total_failed_pages += 1 # Count as a failed page for content, not fetch error
                    except Exception as exc:
                        print(f"\nPage {page_num} generated an exception: {exc}")
                        total_failed_pages += 1
                
                # If no new tasks were submitted and all current tasks are done, break
                # This ensures we stop if we hit the end of content before MAX_PAGES_TO_CHECK
                if not futures and pbar.n < MAX_PAGES_TO_CHECK:
                    # If all futures are done, and we haven't reached MAX_PAGES_TO_CHECK,
                    # it means we've exhausted all available content.
                    break
                
                time.sleep(0.5) # Be polite, add a small delay between processing batches

    save_data_to_files(all_tournaments)

    end_time = time.perf_counter()
    total_time = end_time - start_time

    print(f"\nTotal scraping time: {total_time:.2f} seconds")
    print(f"Total pages processed: {total_pages_processed}")
    print(f"Total tournaments scraped: {len(all_tournaments)}")
    print(f"Total pages with no tournaments/errors: {total_failed_pages}")

if __name__ == "__main__":
    main()
