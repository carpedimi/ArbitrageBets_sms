from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import re
import numpy as np
from selenium.webdriver.chrome.options import Options
# https://opticodds.com/sportsbooks/unibet-api#contact-us for Betcity & Unibet (Kambi) API
# Function to configure the Chrome driver with evasion techniques
def configure_driver():
    # Set Chrome options
    options = Options()
    options.add_argument("--headless")  # Run headless mode (without opening a browser window)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-popup-blocking")
    options.add_argument("disable-infobars")

    # Initialize the Chrome WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Bypass detection
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return driver

# Function to extract data from a URL
def scrape_data(url):
    driver = configure_driver()  # Use the driver with anti-detection setup
    competition = url.split('/')[-1]

    # Open the page
    driver.get(url)
    
    # Wait for the page to load completely
    time.sleep(10)

    # Get the page source and parse it using BeautifulSoup
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all <a> tags and filter by href containing "event"
    event_links = []
    for link in soup.find_all('a', href=True):
        if 'event' in link['href']:
            event_links.append(link['href'])

    # If event links are found, proceed
    if event_links:
        event_links = np.unique(event_links)
        for n, event_link in enumerate(event_links):
            driver.get(event_link)
            time.sleep(10)  # Allow the event page to load

            # Get the HTML of the event page
            second_page_html = driver.page_source
            soup_second = BeautifulSoup(second_page_html, 'html.parser')

            # Initialize lists for texts and decimal values
            texts = []
            decimal_values_class_1 = []  # For sc-fqkvVR cyiQDV
            decimal_values_class_2 = []  # For sc-kAyceB gIMtGL
            decimal_values_class_3 = []  # For sc-dcJsrY gCFiej

            # Extract elements with the specified classes
            for element in soup_second.find_all(class_='sc-fqkvVR cyiQDV'):
                text = element.get_text(strip=True)
                texts.append(text)

            for element in soup_second.find_all(class_='sc-kAyceB gIMtGL'):
                numbers = re.findall(r'\d+\.\d+', element.get_text())
                decimal_values_class_2.extend(numbers)

            for element in soup_second.find_all(class_='sc-dcJsrY gCFiej'):
                numbers = re.findall(r'\d+\.\d+', element.get_text())
                decimal_values_class_3.extend(numbers)

            # Output the results
            print(f"\nTexts found for event {n} in competition {competition}:")
            for text in texts:
                print(text)
            
            print(f"\nDecimal numbers found for event {n} (class 'sc-kAyceB gIMtGL'):")
            for number in decimal_values_class_2:
                print(number)

            print(f"\nDecimal numbers found for event {n} (class 'sc-dcJsrY gCFiej'):")
            for number in decimal_values_class_3:
                print(number)

            # Optionally, save the extracted data to a file
            with open(f"extracted_data_{competition}_event{n}.txt", "w", encoding="utf-8") as file:
                file.write("Texts after 'sc-fqkvVR cyiQDV':\n")
                for text in texts:
                    file.write(f"{text}\n")
                file.write("\nDecimal numbers after 'sc-kAyceB gIMtGL':\n")
                for number in decimal_values_class_2:
                    file.write(f"{number}\n")
                file.write("\nDecimal numbers after 'sc-dcJsrY gCFiej':\n")
                for number in decimal_values_class_3:
                    file.write(f"{number}\n")
    else:
        print(f"No event links found for {url}")

    # Close the driver after finishing the scraping
    driver.quit()

# List of URLs to scrape
urls = [
    "https://www.betcity.nl/sportsbook#sports-hub/football/spain/la_liga_",
    "https://www.betcity.nl/sportsbook#sports-hub/football/spain/la_liga_2",
    "https://www.betcity.nl/sportsbook#sports-hub/football/italy/serie_a",
    "https://www.betcity.nl/sportsbook#sports-hub/football/italy/serie_b"
]

# Loop through each URL and scrape data
for url in urls:
    scrape_data(url)
