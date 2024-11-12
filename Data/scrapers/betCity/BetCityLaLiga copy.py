from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import re
import numpy as np
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

#Betcity en bookmakers Unibet, Jacks Casino en LeoVegas maken gebruik van dezelfde odds.
# # Initialize the headless browser using Firefox
# driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()))

# # Initialize the headless browser using Chrome
# options = webdriver.ChromeOptions()
# options.add_argument("--headless")  # Run in headless mode (without opening a browser window)
# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# URL of the webpage
urls = ["https://www.betcity.nl/sportsbook#sports-hub/football/spain/la_liga_", "https://www.betcity.nl/sportsbook#sports-hub/football/spain/la_liga_2", "https://www.betcity.nl/sportsbook#sports-hub/football/italy/serie_a", "https://www.betcity.nl/sportsbook#sports-hub/football/italy/serie_b"]
for url in urls:
    # # Initialize the headless browser using Chrome
    # options = webdriver.ChromeOptions()
    # options.add_argument("--headless")  # Run in headless mode (without opening a browser window)
    # driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    # Initialize the headless browser using Firefox
    driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()))
    competition = url.split('/')[-1]

    # Open the page in the browser
    driver.get(url)

    # Wait for 5 seconds to allow the page to fully load
    time.sleep(10)

    # Get the page source (fully rendered HTML)
    html_content = driver.page_source

    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all <a> tags and filter by href containing "event"
    event_links = []
    for link in soup.find_all('a', href=True):
        if 'event' in link['href']:
            event_links.append(link['href'])

    # If event links are found, proceed
    if event_links:
        event_links = np.unique(event_links)
        for n in range(len(event_links)):
            event_link = event_links[n]
            print(f"event link: {event_links}")
            
            # Visit the first event link
            driver.get(event_link)
            
            # Wait another 5 seconds to allow the page to fully load
            time.sleep(10)
            
            # Get the HTML of the second page
            second_page_html = driver.page_source
            
            # Parse the second page's HTML
            soup_second = BeautifulSoup(second_page_html, 'html.parser')
            
            # Initialize lists for texts and decimal values
            texts = []
            decimal_values_class_1 = []  # For sc-fqkvVR cyiQDV
            decimal_values_class_2 = []  # For sc-kAyceB gIMtGL
            decimal_values_class_3 = []  # For sc-dcJsrY gCFiej

            # Find all elements with class 'sc-fqkvVR cyiQDV' and extract text <div class="sc-fqkvVR cyiQDV">Meer dan</div>
            for element in soup_second.find_all(class_='sc-fqkvVR cyiQDV'):
                text = element.get_text(strip=True)  # Get the text content
                texts.append(text)

            # Find all elements with class 'sc-kAyceB gIMtGL' and extract decimal numbers
            for element in soup_second.find_all(class_='sc-kAyceB gIMtGL'):
                # Use regex to extract decimal numbers from the text
                numbers = re.findall(r'\d+\.\d+', element.get_text())
                decimal_values_class_2.extend(numbers)

            # Find all elements with class 'sc-dcJsrY gCFiej' and extract decimal numbers
            for element in soup_second.find_all(class_='sc-dcJsrY gCFiej'):
                # Use regex to extract decimal numbers from the text
                numbers = re.findall(r'\d+\.\d+', element.get_text())
                decimal_values_class_3.extend(numbers)
            
            # Output the results
            print("\nTexts found after 'sc-fqkvVR cyiQDV':")
            for text in texts:
                print(text)
            
            print("\nDecimal numbers found after 'sc-kAyceB gIMtGL':")
            for number in decimal_values_class_2:
                print(number)

            print("\nDecimal numbers found after 'sc-dcJsrY gCFiej':")
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
        print("No event links found!")

    # Close the browser
    driver.quit()
