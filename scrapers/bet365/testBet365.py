from supabase import create_client
import json
import pandas as pd
from datetime import date
import statsmodels
import numpy as np
import datetime
from datetime import datetime
import time
import pandas as pd
import numpy as np
import statistics
import requests #The requests library for HTTP requests in Python
import xlsxwriter #The XlsxWriter libarary for 
import math #The Python math module
from scipy import stats #The SciPy stats module
import time
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import time
from selenium.webdriver.common.keys import Keys

def driver_code():
    Capabilities = DesiredCapabilities.CHROME
    Capabilities["pageLoadStrategy"] = "normal"
    options = ChromeOptions()

    useragentarray = [
        "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.76 Mobile Safari/537.36"
    ]

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument(f"--user-data-dir=./profile{driver_num}")

    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("disable-infobars")
    options.add_argument("disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    driver.execute_cdp_cmd(
        "Network.setUserAgentOverride", {"userAgent": useragentarray[0]}
    )

    options.add_argument("--disable-popup-blocking")
    #     driver.execute_script(
    #         """setTimeout(() => window.location.href="https://www.bet365.com.au", 100)"""
    #     )
    driver.get("https://www.bet365.nl/?_h=Kl-Ibwf5T6ldiUhraSmMCA%3D%3D&btsffd=1#/AC/B1/C1/D1002/E105912932/G40/")

    driver.set_window_size(390, 844)
    time.sleep(10)
    return driver

def open_tab(driver,link):
    driver.execute_script(f"""window.open('{link}', "_blank");""")
    time.sleep(12)
    driver.switch_to.window(driver.window_handles[-1])

#Remove Loader
def remove_Loader(driver):
    driver.execute_script("document.querySelector('.bl-Preloader').remove();")

#Accept Cookies
def accept_cookies(driver):
    cookies = driver.find_elements(By.CSS_SELECTOR, ".ccm-CookieConsentPopup_Accept ")
    if(len(cookies) > 0):
        cookies[0].click()
    

#Sorry No Markets Error
def no_markets(driver):
    markets = driver.find_elements(By.CSS_SELECTOR, ".sm-NoAvailableMarkets_Header ")
    if(len(markets) > 0):
        open_tab(driver)

def sort_string(string):
    string = ''.join(e for e in string if e.isalnum())
    string = string.lower()
    return string

new_driver = driver_code()
open_tab(new_driver, 'https://www.bet365.nl/?_h=Kl-Ibwf5T6ldiUhraSmMCA%3D%3D&btsffd=1#/AC/B1/C1/D1002/E105912932/G40/')
accept_cookies(new_driver)
time.sleep(9)
open_tab(new_driver, 'https://www.bet365.nl/?_h=Kl-Ibwf5T6ldiUhraSmMCA%3D%3D&btsffd=1#/AC/B1/C1/D1002/E105912932/G40/')

teams = []
times = []
odds = []
dates = []
teams_ = new_driver.find_elements(
                By.CSS_SELECTOR, ".rcl-ParticipantFixtureDetailsTeam_TeamName "
                 )
times_ = new_driver.find_elements(
                By.CSS_SELECTOR, ".rcl-ParticipantFixtureDetails_BookCloses "
                )
for i in teams_:
    teams.append(i.text)
for i in times_:
    times.append(i.text)

odds_ = new_driver.find_elements(
    By.CSS_SELECTOR, ".sgl-ParticipantOddsOnly80_Odds"
    )

for key in odds_:
    odds.append(key.text)
    

home_teams = teams[::2]
away_teams = teams[1::2]
home_odds = odds[0:len(times)]
draw_odds = odds[len(times):len(times) * 2]
away_odds = odds[len(times)*2:len(odds)]

#Columns for Dataframe
columns = ['Home Team', 'Away Team',"Home Odds","Draw Odds","Away Odds"]

# Initialize a new DataFrame with columns
new_dataframe = pd.DataFrame(columns=columns)

# Add arrays to columns
new_dataframe['Home Team'] = home_teams
new_dataframe['Away Team'] = away_teams
new_dataframe['Home Odds'] = home_odds
new_dataframe['Draw Odds'] = draw_odds
new_dataframe['Away Odds'] = away_odds

new_driver.quit()

new_dataframe.to_csv('Bet365odds.csv')