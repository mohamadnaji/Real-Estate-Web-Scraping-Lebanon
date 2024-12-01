#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
import os
import concurrent.futures
import time
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects, RequestException
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options


# Function to scrape URLs with a specific prefix
def scrape_urls_with_prefix(uri, param):
    
    url = uri + param
    try:
        # Proceed with Selenium if the status is OK
        options = Options()
        options.binary_location = "/usr/bin/firefox"  # Path to Firefox binary
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-gpu')

        service = FirefoxService(executable_path="/usr/bin/geckodriver")
        browser = webdriver.Firefox(service=service, options=options)

        # Navigate to the webpage
        browser.get(url)

        # Wait for the page to fully load
        wait = WebDriverWait(browser, 500)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'wpl_property_listing_list_view_container')))

        time.sleep(1)

        # Scroll down incrementally
        for i in range(10):  # Scroll down 10 times
            browser.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.5)  # Adjust sleep time if needed

        # Once fully loaded, retrieve the page source
        page_source = browser.page_source

        # Parse the HTML content
        soup = BeautifulSoup(page_source, 'html.parser')

        pagination_ul = soup.find('ul', class_='pagination')
        
        # Find all <li> elements within this <ul>
        list_items = pagination_ul.find_all('li')

        # Get the last <li> element using indexing
        last_li = list_items[-2]
        counter = last_li.text

        # Find all 'a' tags
        # Select all elements with an ID that starts with 'prefix'
        elements = soup.select('[id^=prp_link_id_]')


        url_commercial = 'https://ose-properties.com/properties/'
        # Extract the href attribute of each a tag that starts with the specified prefix
        filtered_urls = [a.get('href') for a in elements if a.get('href') and a.get('href').startswith(url_commercial)]
        sett = set(filtered_urls)
        print(f"Scraped {len(sett)} URLs from {url}")
        return sett, counter

    # except ConnectionError as e:
    #     print("Error in url ", url)
    #     print(f"Connection error occurred: {e}")
    # except Timeout as e:
    #     print("Error in url ", url)
    #     print(f"Timeout error occurred: {e}")
    # except TooManyRedirects as e:
    #     print("Error in url ", url)
    #     print(f"Too many redirects: {e}")
    # except RequestException as e:
    #     print("Error in url ", url)
    #     print(f"An error occurred: {e}")
    # except Exception as e:

    #     print("Error in url ", url)
    #     print(f"An error occurred: {e}")
    # except TimeoutException as e:

    #     print("Error in url ", url)
    #     print("An error occurred: Timeout while loading the page or finding elements.")
    finally:
        browser.close()
        browser.quit()
    return None

def scrape_urls(initial_url):
    urls = set()
    idx = 1
    oo = 0
    while True:
        # Scrape URLs with the specified prefix
        print(f'Scraping page {idx}')
        try:
            new_urls, counter = scrape_urls_with_prefix(initial_url, f'?wplpage={idx}')
            urls.update(new_urls)
            print(f"counter {counter}")
            if idx >= int(counter) or idx > 300:
                break
        except Exception as e:
            print(f"Error in page {idx}")
            print(f"An error occurred: {e}")
        idx += 1
    return urls


def save_urls_to_csv(url, csv_file_path):

    url = scrape_urls(url)
    print(f'Number of urls: {len(url)}')

    # Convert set to list
    data_list = list(url)

    # Create a DataFrame from the list
    df = pd.DataFrame(data_list, columns=['URL'])

    # Save the DataFrame to an Excel file
    df.to_csv(csv_file_path, index=False)


url_rent = 'https://www.ose-properties.com/rent/'
url_sale = 'https://www.ose-properties.com/buy/'

current_datetime = datetime.now().strftime('%Y-%m-%d')
csv_file_path_rent = f'ose_urls_to_be_scraped_rent_{current_datetime}.csv'
csv_file_path_sale = f'ose_urls_to_be_scraped_sale_{current_datetime}.csv'

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full file path
csv_file_path_rent = os.path.join(script_dir + "/rent/urls", csv_file_path_rent)
csv_file_path_sale = os.path.join(script_dir + "/sale/urls", csv_file_path_sale)

if os.path.exists(csv_file_path_rent):
    print(f'File {csv_file_path_rent} already scraped')
else:
    save_urls_to_csv(url_rent, csv_file_path_rent)

if os.path.exists(csv_file_path_sale):
    print(f'File {csv_file_path_sale} already scraped')
else:
    save_urls_to_csv(url_sale, csv_file_path_sale)