from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
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
    try:
        # Create a WebDriver instance
        url = uri + param

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
        wait = WebDriverWait(browser, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'main')))

        time.sleep(2)

        # Scroll down incrementally
        for i in range(10):  # Scroll down 10 times
            browser.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.5)  # Adjust sleep time if needed

        # Once fully loaded, retrieve the page source
        page_source = browser.page_source

        # Parse the HTML content
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find all 'a' tags
        a_tags = soup.find_all('a')

        # Extract the href attribute of each a tag that starts with the specified prefix
        filtered_urls = [a.get('href') for a in a_tags if a.get('href') and a.get('href').startswith(uri)]
        print(f"Scraped {len(filtered_urls)} URLs from {url}")
        return set(filtered_urls)

    except ConnectionError as e:
        print("Error in url ", url)
        print(f"Connection error occurred: {e}")
    except Timeout as e:
        print("Error in url ", url)
        print(f"Timeout error occurred: {e}")
    except TooManyRedirects as e:
        print("Error in url ", url)
        print(f"Too many redirects: {e}")
    except RequestException as e:
        print("Error in url ", url)
        print(f"An error occurred: {e}")
    except Exception as e:

        print("Error in url ", url)
        print(f"An error occurred: {e}")
    except TimeoutException as e:

        print("Error in url ", url)
        print("An error occurred: Timeout while loading the page or finding elements.")
    finally:
        browser.close()
        browser.quit()

def scrape_urls(initial_url, types):
    urls = set()
    idx = 1
    while True:
        # Scrape URLs with the specified prefix
        print(f'Scraping page {idx}')
        new_urls = scrape_urls_with_prefix(initial_url, f'?pg={idx}&sort=featured&ct={types}')
        if new_urls is None or len(new_urls) == 0:
            break
        urls.update(new_urls)
        idx += 1
    return urls


def save_urls_to_csv(url, csv_file_path, types):

    urls = scrape_urls(url, types)
    print(f'Number of urls: {len(urls)}')

    # Convert set to list
    data_list = list(urls)

    # Create a DataFrame from the list
    df = pd.DataFrame(data_list, columns=['URL'])

    # Save the DataFrame to an Excel file
    df.to_csv(csv_file_path, index=False)


url_sale = 'https://www.realestate.com.lb/en/buy-apartment-house-lebanon'
url_rent = 'https://www.realestate.com.lb/en/rent-apartment-house-lebanon'
url_commercial = 'https://www.realestate.com.lb/en/commercial-real-estate-property-lebanon'

current_datetime = os.getenv("TODAY_DATE")
#current_datetime = datetime.now().strftime('%Y-%m-%d')
csv_file_path_rent = f'realEstate_urls_to_be_scraped_rent_{current_datetime}.csv'
csv_file_path_sale = f'realEstate_urls_to_be_scraped_sale_{current_datetime}.csv'
csv_file_path_commercial_rent = f'realEstate_urls_to_be_scraped_commercial_rent_{current_datetime}.csv'
csv_file_path_commercial_sale = f'realEstate_urls_to_be_scraped_commercial_sale_{current_datetime}.csv'

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full file path
csv_file_path_rent = os.path.join(script_dir + "/rent/urls", csv_file_path_rent)
csv_file_path_sale = os.path.join(script_dir + "/sale/urls", csv_file_path_sale)
csv_file_path_commercial_rent = os.path.join(script_dir + "/rent/urls", csv_file_path_commercial_rent)
csv_file_path_commercial_sale = os.path.join(script_dir + "/sale/urls", csv_file_path_commercial_sale)


if os.path.exists(csv_file_path_sale):
    print(f'File {csv_file_path_sale} already scraped')
else:
    save_urls_to_csv(url_sale, csv_file_path_sale, 1)

if os.path.exists(csv_file_path_rent):
    print(f'File {csv_file_path_rent} already scraped')
else:
    save_urls_to_csv(url_rent, csv_file_path_rent, 2)

if os.path.exists(csv_file_path_commercial_sale):
    print(f'File {csv_file_path_commercial_sale} already scraped')
else:
    save_urls_to_csv(url_commercial, csv_file_path_commercial_sale, 3)

if os.path.exists(csv_file_path_commercial_rent):
    print(f'File {csv_file_path_commercial_rent} already scraped')
else:
    save_urls_to_csv(url_commercial, csv_file_path_commercial_rent, 4)

