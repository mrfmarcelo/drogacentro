import requests
import time
import json
import os
import csv
from math import ceil
from datetime import timedelta

# Assuming get_oauth_token_password_grant and refresh_oauth_token are in a separate module
# For simplicity in this single file, let's include them here.
# In a real application, you'd import these from your token management module.

def get_oauth_token_password_grant(client_id, client_secret, username, password, user_agent):
    """
    Obtains a Reddit OAuth access token and refresh token using the password grant type.
    (Included for completeness, typically from a separate token management script)
    """
    token_url = "https://www.reddit.com/api/v1/access_token"
    client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)

    data = {
        "grant_type": "password",
        "username": username,
        "password": password
    }

    headers = {"User-Agent": user_agent}

    try:
        response = requests.post(token_url, auth=client_auth, data=data, headers=headers)
        response.raise_for_status()
        token_info = response.json()
        return token_info
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error during token acquisition: {e}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Network or other request error during token acquisition: {e}")
        return None

def refresh_oauth_token(client_id, client_secret, refresh_token, user_agent):
    """
    Refreshes an expired Reddit OAuth access token using the refresh token.
    (Included for completeness, typically from a separate token management script)
    """
    token_url = "https://www.reddit.com/api/v1/access_token"
    client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    headers = {"User-Agent": user_agent}

    try:
        response = requests.post(token_url, auth=client_auth, data=data, headers=headers)
        response.raise_for_status()
        token_info = response.json()
        return token_info
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error during token refresh: {e}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Network or other request error during token refresh: {e}")
        return None


def load_tokens(filepath="reddit_tokens.json"):
    """Loads tokens from a JSON file."""
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return None

def save_tokens(tokens, filepath="reddit_tokens.json"):
    """Saves tokens to a JSON file."""
    with open(filepath, 'w') as f:
        json.dump(tokens, f, indent=4)

def check_subreddit_status(subreddit_id, user_agent, access_token):
    """
    Checks the status of a single subreddit using Reddit's about.json endpoint.

    Args:
        subreddit_id (str): The ID of the subreddit (e.g., "banned_sub").
        user_agent (str): A descriptive User-Agent string for your requests.
        access_token (str): Your current Reddit OAuth access token.

    Returns:
        dict: A dictionary containing the subreddit ID, status, and HTTP code.
    """
    url = f"https://oauth.reddit.com/r/{subreddit_id['Name']}/about.json" # Use oauth.reddit.com for authenticated requests
    headers = {
        "User-Agent": user_agent,
        "Authorization": f"Bearer {access_token}"
    }

    try:
        response = requests.get(url, headers=headers)
        http_status = response.status_code

        if http_status == 200:
            # Subreddit exists and is accessible.
            return {
                "subreddit_id": subreddit_id,
                "status": "Active",
                "http_code": http_status
            }
        if http_status == 404:
            # 404 indicates the subreddit is banned, deleted, or never existed.
            return {
                "subreddit_id": subreddit_id,
                "status": "Banned/Non-existent",
                "http_code": http_status
            }
        if http_status == 403:
            # 403 Forbidden: Could be due to invalid/expired token, incorrect User-Agent,
            # or the authenticated user doesn't have access to a private/restricted sub.
            return {
                "subreddit_id": subreddit_id,
                "status": "Forbidden (Check Token/User-Agent/Permissions)",
                "http_code": http_status
            }
        if http_status == 401:
            # 401 Unauthorized: Token is likely expired or invalid.
            return {
                "subreddit_id": subreddit_id,
                "status": "Unauthorized (Token Expired/Invalid)",
                "http_code": http_status
            }
        if http_status == 429:
            # Too Many Requests - Rate limit hit
            return {
                "subreddit_id": subreddit_id,
                "status": "Rate Limited (429)",
                "http_code": http_status
            } 
        # Other HTTP errors
        return {
            "subreddit_id": subreddit_id,
            "status": f"Error (HTTP {http_status})",
            "http_code": http_status
        }
    except requests.exceptions.RequestException as e:
        # Handle network errors, connection issues, etc.
        return {
            "subreddit_id": subreddit_id,
            "status": f"Network Error: {e}",
            "http_code": None
        }

def load_subreddit_list_from_file(filepath):
    """
    Loads a list of subreddit IDs from a text file.
    Each subreddit ID should be on a new line.
    """
    subreddit_dicts = []
    if not os.path.exists(filepath):
        print(f"Error: Subreddit list file '{filepath}' not found.")
        print("Please create a 'subreddits.csv' file with one subreddit ID per line.")
        return []

    with open(filepath, 'r', newline='', encoding='utf-8-sig') as csvfile:
        subreddit_reader = csv.DictReader(csvfile, delimiter='|', quotechar='"')
        for row in subreddit_reader:
            subreddit_dicts.append(row)

    return subreddit_dicts

def main():
    """
    Main function to process a list of subreddits with rate limiting and token refresh.
    """
    # --- Configuration ---
    # IMPORTANT: Replace with your actual Reddit username.
    # This is required for the User-Agent header and helps Reddit identify your requests.
    YOUR_REDDIT_USERNAME = "fabiorzfreitas" # <--- REPLACE THIS!
    USER_AGENT = f"desktop:my-python-subreddit-checker:v1.0 (by /u/{YOUR_REDDIT_USERNAME})"

    # File containing your list of subreddits (one per line)
    SUBREDDIT_FILE = "RedditNSFWIndex.10-WORKING.csv"
    TOKEN_FILE = "reddit_tokens.json" # File where your tokens are saved

    # --- Reddit App Credentials (Needed for token refresh if access token expires) ---
    # These should match the ones you used in the reddit-oauth-token-getter script.
    # If you only run the token getter once and then manually copy the access token,
    # you might not need these here, but they are crucial for automatic refreshing.
    CLIENT_ID = "Q0VC6ikEVxmDrvV0v83x6Q" # e.g., "abcdef123456" <--- REPLACE THIS!
    CLIENT_SECRET = "H040vTDZfTsExUA-Qp_sPG1tXZN3yg" # e.g., "xyz7890abc" <--- REPLACE THIS!
    REDDIT_PASSWORD = "ggokussj4" # <--- REPLACE THIS! (Only needed if you need to re-authenticate from scratch)


    # --- Rate Limiting Parameters ---
    REQUESTS_PER_MINUTE = 100
    SECONDS_PER_REQUEST = 60 / REQUESTS_PER_MINUTE
    DELAY_BUFFER = 0 # Small buffer to be safe
    EFFECTIVE_DELAY = SECONDS_PER_REQUEST + DELAY_BUFFER

    print(f"Loading subreddits from '{SUBREDDIT_FILE}'...\n")
    subreddit_dicts = load_subreddit_list_from_file(SUBREDDIT_FILE)

    if not subreddit_dicts:
        print("No subreddits found in the file. Exiting.")
        return

    print(f"Found {len(subreddit_dicts)} subreddits to check.")
    print(f"Making requests with a delay of approximately {EFFECTIVE_DELAY:.2f} seconds per request.")
    print("-" * 32)

    # --- Load or Obtain Tokens ---
    tokens = load_tokens(TOKEN_FILE)
    if not tokens:
        print(f"No tokens found in '{TOKEN_FILE}'. Attempting to obtain new tokens...")
        # You might need to prompt for username/password here if not hardcoded
        tokens = get_oauth_token_password_grant(
            CLIENT_ID, CLIENT_SECRET, YOUR_REDDIT_USERNAME, REDDIT_PASSWORD, USER_AGENT
        )
        if not tokens:
            print("Failed to obtain initial tokens. Exiting.")
            return
        save_tokens(tokens, TOKEN_FILE)
        print("New tokens obtained and saved.")
    else:
        print(f"Tokens loaded from '{TOKEN_FILE}'.\n")

    access_token = tokens.get('access_token')
    refresh_token = tokens.get('refresh_token')
    expires_at = time.time() + tokens.get('expires_in', 3600) # Default to 1 hour if not specified

    results = []
    start_time_minute_batch = time.time()
    start_time = time.time()

    for i, subreddit_id in enumerate(subreddit_dicts):
        # Check if access token is expired or about to expire (e.g., within 5 minutes)
        if time.time() >= expires_at - 300: # Refresh if less than 5 minutes left
            print("\nAccess token is expired or close to expiring. Attempting to refresh...")
            if not refresh_token:
                print("No refresh token available. Please re-run get_reddit_token.py to get new tokens.")
                break # Exit if no refresh token
            new_tokens = refresh_oauth_token(CLIENT_ID, CLIENT_SECRET, refresh_token, USER_AGENT)
            if not new_tokens:
                print("Failed to refresh token. Please check tokens.json to get new tokens.")
                break # Exit if token refresh fails
            tokens.update(new_tokens) # Update existing tokens with new access token and expiry
            access_token = tokens['access_token']
            expires_at = time.time() + tokens.get('expires_in', 3600)
            save_tokens(tokens, TOKEN_FILE)
            print("Access token refreshed successfully.")

        result = check_subreddit_status(subreddit_id, USER_AGENT, access_token)
        results.append(result)

        status_message = f"Checking /r/{result['subreddit_id']['Name']} ({i + 1}/{len(subreddit_dicts)}): {result['status']} (HTTP: {result['http_code']})"
        if result['http_code'] == 403:
            status_message += " -> Possible User-Agent issue or insufficient permissions. Ensure User-Agent is correct!"
        if result['http_code'] == 401:
            status_message += " -> Token likely expired or invalid. Script will attempt to refresh."

        print(status_message)

        if i > 0 and (i + 1) % REQUESTS_PER_MINUTE == 0:
            print(f'[PARTIAL TIME: {timedelta(seconds=(round(time.time(),4) - round(start_time, 4)))}]\n')

        # Enforce batch-based rate limiting (100 requests per minute)
        if i > 0 and (i + 1) % REQUESTS_PER_MINUTE == 0:
            elapsed_since_last_batch = time.time() - start_time_minute_batch
            time_to_wait = 60 - elapsed_since_last_batch
            if time_to_wait > 0:
                print(f"Pausing for {time_to_wait:.2f} seconds to respect rate limits...\n")
                time.sleep(time_to_wait)
            start_time_minute_batch = time.time() # Reset timer for the next minute's batch

    print('\nDone!')
    
    print("\n" + "-" * 32)
    print(f'Total time: {timedelta(seconds=(round(time.time(),4) - round(start_time, 4)))}')
    banned_list = []
    active_list = []
    banned_count = 0
    for res in results:
        if res["status"] == "Banned/Non-existent":
            banned_count += 1
            banned_list.append(res["subreddit_id"])
        else:
            active_list.append(res["subreddit_id"])

    print(f"\nTotal subreddits checked: {len(subreddit_dicts)}")
    print(f"Total banned/non-existent subreddits found: {banned_count}")

    # You can now manipulate the 'results' list as needed
    # For example, save it to a CSV file:
    with open("active_results.csv", "w") as csvout:
        fnames = ['Subscribers', 'Name', 'Description', 'Special']
        writer = csv.DictWriter(csvout,fieldnames=fnames, delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)

        writer.writeheader()
        writer.writerows(active_list)

    print("\nActive subreddits saved to 'active_results.csv'")


    with open("banned_results.csv", "w") as csvout:
        fnames = ['Subscribers', 'Name', 'Description', 'Special']
        writer = csv.DictWriter(csvout,fieldnames=fnames, delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)

        writer.writeheader()
        writer.writerows(banned_list)

    print("Banned subreddits saved to 'banned_results.csv'")


if __name__ == "__main__":
    main()
