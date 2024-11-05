import requests
import random
import time
from bs4 import BeautifulSoup

# URL of the webpage
url = "https://www.betcity.nl/sportsbook#sports-hub/football/spain/la_liga"

# List of User-Agent strings
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
]

# Rotate User-Agent and introduce delay between requests
headers = {
    'User-Agent': random.choice(user_agents)
}

# Optional: Use a proxy (comment this out if you don't have one)
proxies = {
    'http': 'http://your_proxy_ip:your_proxy_port',
    'https': 'http://your_proxy_ip:your_proxy_port'
}

# Send the request
response = requests.get(url, headers=headers)

# Random delay between 2 to 5 seconds
time.sleep(random.uniform(2, 5))

# Check if the request was successful
if response.status_code == 200:
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Get the HTML content
    html_content = soup.prettify()

    # Print or save the HTML content
    # Write the HTML content to a file
    with open("output.html", "w", encoding="utf-8") as file:
        file.write(html_content)
    #print(html_content)
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")
