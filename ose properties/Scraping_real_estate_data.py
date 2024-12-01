#!/usr/bin/env python
# coding: utf-8


from bs4 import BeautifulSoup
import pandas as pd
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects, RequestException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import os
from datetime import datetime
import pandas as pd
import concurrent.futures
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options


def scrape_page(url, listing_type, browser, wait):
    data = {}
    try:

        browser.get(url)
        wait.until(EC.presence_of_element_located((By.ID, 'wpl_map_canvas15')))
        # Continue with your scraping logic

        # Scroll down incrementally
        for i in range(10):  # Scroll down 10 times
            browser.execute_script("window.scrollBy(0, 1000);")
            time.sleep(0.5)  # Adjust sleep time if needed

        # time.sleep(0.5)
        # Get the rendered HTML
        html = browser.page_source

        # Initiate data object
        data = {'source': url, 'source_name': 'ose', 'Listing Type': type}

        # Parse the HTML content
        soup = BeautifulSoup(html, 'html.parser')

        body = soup.find(id='page-container')

        price = body.find(class_='price_box')
        price_list = price.text.strip().split(' Per ')
        data['price'] = price_list[0].strip()

        if listing_type == 'Rent' and len(price_list) > 1:
            data['Rental duration'] = price_list[1]

        if listing_type == 'Sell' and len(price_list) > 1:
            return None
            
        desc0 = soup.find(id='desc-0')
        description = desc0.find(id='show_content_1')
        data['description'] = description.text.strip()

        desc1 = soup.find(id='desc-1')
        description = desc1.find(id='show_content_1')
        divs = description.find_all('div')
        for div in divs:
            label = div.find(name='label')
            details = div.find(name='span')
            data[label.text.strip(' :')] = details.text.strip()

        desc2 = soup.find(id='desc-2')
        description = desc2.find(id='show_content_2')
        divs = description.find_all('div')
        location = ''
        for div in divs:
            # label = div.find('label')
            details = div.find('span')
            if location == '':
                location = details.text.strip()
            else:
                location = location + ', ' + details.text.strip()
        data['location'] = location

        # Get the location string
        location = soup.select('a[href^="https://maps.google.com/maps?ll="]')
        if location:
            data['img_src'] = location[0]['href']
            img = data['img_src'].removeprefix("https://maps.google.com/maps?ll=")
            coordinate = img.removesuffix("&z=10&t=m&hl=en-US&gl=US&mapclient=apiv3")
            coordinate = coordinate.split(',')
            x = coordinate[0]
            y = coordinate[1]
            data['X'] = x
            data['Y'] = y

        print(data)
        return data
    except TimeoutException:
        print("Timed out waiting for page to load")
    except ConnectionError as e:
        print(f"Connection error occurred: {e}")
    except Timeout as e:
        print(f"Timeout error occurred: {e}")
    except TooManyRedirects as e:
        print(f"Too many redirects: {e}")
    except RequestException as e:
        print(f"An error occurred RequestException: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        browser.quit()  # Close the browser even if an exception occurs

    return None


def scraping_data(path, file_name, listing_type):
    # Load the CSV file into a DataFrame
    urls = pd.read_csv(path + "/urls/" + file_name)
    print(len(urls))
    all_data = []
    failed_url = []
    index = 1

    def process_url(url, listing_type):
        # Each thread gets its own WebDriver instance
        options = Options()
        options.binary_location = "/usr/bin/firefox"  # Path to Firefox binary
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-gpu')

        service = FirefoxService(executable_path="/usr/bin/geckodriver")
        browser = webdriver.Firefox(service=service, options=options)

        try:
            wait = WebDriverWait(browser, 10)
            return scrape_page(url, listing_type, browser, wait)
        finally:
            browser.quit()  # Ensure the browser is closed after each thread

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Map the process_url function to the list of URLs
        future_to_url = {executor.submit(process_url, url['URL'], listing_type): url for index, url in urls.iterrows()}

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                page_data = future.result()
                all_data.append(page_data)
                index += 1
                print(index)
            except Exception as exc:
                print(f"{url['URL']} generated an exception: {exc}")
                failed_url.append(url['URL'])

    # Filter out None values
    filtered_data = [item for item in all_data if item is not None]

    # Create DataFrame
    df = pd.DataFrame(filtered_data)

    # Rename columns
    df = df.rename(columns={
        'Listing ID': 'Reference Id',
        'Square Meter': 'Size (m²)',
        'Price Type': 'Rental duration'
    })
    
    if 'Price' in df.columns:
        #remove unused column
        df = df.drop('Price', axis=1, errors='ignore')

    csv_file_path = f'{path}/data_{file_name}'
    df.to_csv(csv_file_path, index=False)
    return all_data, failed_url


current_datetime = datetime.now().strftime('%Y-%m-%d')
csv_file_path_rent = f'ose_urls_to_be_scraped_rent_{current_datetime}.csv'
csv_file_path_sale = f'ose_urls_to_be_scraped_sale_{current_datetime}.csv'

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f'Current path {script_dir}')

d,f = scraping_data(script_dir + "/sale", csv_file_path_sale, "Sell")
g,t = scraping_data(script_dir + "/rent", csv_file_path_rent, "Rent")

