import requests
import time
import random

url = 'https://drogaraia.com.br'

# List of user agents
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 13_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1'
]

working_user_agent = None

for ua in user_agents:
    headers = {'User-Agent': ua}
    
    try:
        response = requests.get(url, headers=headers)
        
        # Check for a successful HTTP status code (200)
        if response.status_code == 200:
            print(f"Success! Found a working User-Agent: {ua}")
            working_user_agent = ua
        else:
            print(f"Failed with status code {response.status_code} for User-Agent: {ua}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed for User-Agent: {ua} with error: {e}")
        
    # Wait for a random period to avoid being blocked
    time.sleep(random.uniform(2, 5))

if not working_user_agent:
    print("Could not find a working User-Agent.")

