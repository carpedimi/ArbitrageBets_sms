from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd

# Function to configure the Chrome driver
def driver_code():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("disable-infobars")
    options.add_argument("--disable-popup-blocking")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Bypass detection
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    driver.set_window_size(390, 844)
    
    return driver

# Function to open a new tab
def open_tab(driver, link):
    driver.execute_script(f"""window.open('{link}', "_blank");""")
    time.sleep(12)
    driver.switch_to.window(driver.window_handles[-1])

# Function to accept cookies if prompted
def accept_cookies(driver):
    cookies = driver.find_elements(By.CSS_SELECTOR, ".ccm-CookieConsentPopup_Accept")
    if len(cookies) > 0:
        cookies[0].click()

# Function to extract and store all links from the page
def extract_links(driver):
    # Find all <a> tags with href attributes
    links = driver.find_elements(By.TAG_NAME, 'a')
    all_links = []
    
    for link in links:
        href = link.get_attribute('href')
        if href:  # Only add non-empty href attributes
            all_links.append(href)

    # Write all links to a file
    with open('captured_links.txt', 'w') as f:
        for link in all_links:
            f.write(f"{link}\n")

    print(f"Total {len(all_links)} links captured and saved to 'captured_links.txt'.")

# Scraping process
new_driver = driver_code()
open_tab(new_driver, 'https://www.bet365.nl/?_h=Kl-Ibwf5T6ldiUhraSmMCA%3D%3D&btsffd=1#/AC/B1/C1/D1002/E105912932/G40/')
accept_cookies(new_driver)

# Extract and save all links from the current page
extract_links(new_driver)

# Example: Capturing team names, odds, etc.
teams = []
times = []
odds = []

teams_ = new_driver.find_elements(By.CSS_SELECTOR, ".rcl-ParticipantFixtureDetailsTeam_TeamName")
times_ = new_driver.find_elements(By.CSS_SELECTOR, ".rcl-ParticipantFixtureDetails_BookCloses")

for i in teams_:
    teams.append(i.text)
for i in times_:
    times.append(i.text)

odds_ = new_driver.find_elements(By.CSS_SELECTOR, ".sgl-ParticipantOddsOnly80_Odds")

for key in odds_:
    odds.append(key.text)

home_teams = teams[::2]
away_teams = teams[1::2]
home_odds = odds[0:len(times)]
draw_odds = odds[len(times):len(times) * 2]
away_odds = odds[len(times) * 2:len(odds)]

# Columns for DataFrame
columns = ['Home Team', 'Away Team', 'Home Odds', 'Draw Odds', 'Away Odds']

# Initialize a new DataFrame with columns
new_dataframe = pd.DataFrame(columns=columns)

# Add arrays to columns
new_dataframe['Home Team'] = home_teams
new_dataframe['Away Team'] = away_teams
new_dataframe['Home Odds'] = home_odds
new_dataframe['Draw Odds'] = draw_odds
new_dataframe['Away Odds'] = away_odds

# Quit the driver
new_driver.quit()

# Save odds data to a CSV file
new_dataframe.to_csv('Bet365odds.csv', index=False)

print("Odds data saved to 'Bet365odds.csv'.")
