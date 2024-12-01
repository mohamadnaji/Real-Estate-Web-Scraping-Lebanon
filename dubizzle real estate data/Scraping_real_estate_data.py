#!/usr/bin/env python
# coding: utf-8

import time
import pandas as pd
import requests
import os
import traceback
import glob
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects, RequestException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options


def scrape_page(url, listing_type, browser, wait):
    try:
        url = 'https://www.dubizzle.com.lb' + url
        browser.get(url)
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[@aria-label='Location']")))

        # Retrieve page source and parse HTML content
        page_source = browser.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        data = {'source_name': 'dubizzle', 'source': url, 'Listing Type': listing_type}
        extraction_datetime = datetime.now().strftime('%Y-%m-%d')
        data['extraction date'] = extraction_datetime

        main = soup.select_one('#body-wrapper')
        if main:
            overview = main.find('div', {'aria-label': 'Overview'})
            if overview:
                price = overview.find('span', {'aria-label': 'Price'})
                creation_date = overview.find('span', {'aria-label': 'Creation date'})
                data['price'] = price.get_text(strip=True) if price else None
                data['creation date'] = creation_date.get_text(strip=True) if creation_date else None

            highlighted = main.find('div', {'class': 'ab0d2d79'})
            if highlighted:
                highlights = highlighted.find_all('span')
                current_label = None
                for child in highlights:
                    text = child.get_text(strip=True)
                    if current_label is None:
                        current_label = text
                    else:
                        data[current_label] = text
                        current_label = None

            details = soup.find('div', {'aria-label': 'Details'})
            if details:
                all_children = details.find_all('span')
                current_label = None
                for child in all_children:
                    text = child.get_text(strip=True)
                    if current_label is None:
                        current_label = text
                    else:
                        data[current_label] = text
                        current_label = None

            feature = main.find('div', {'aria-label': 'Features'})
            if feature:
                features = feature.find_all('span')
                for span in features:
                    text = span.get_text(strip=True)
                    if text:
                        data[text] = 1

            location = soup.find('span', {'aria-label': 'Location'})
            if location:
                data['location_str'] = location.get_text(strip=True)

            # Handle 'See All' button
            see_all = feature.find(attrs={'class': 'a1c1940e'}) if feature else None
            if see_all:
                try:
                    see_all = wait.until(EC.presence_of_element_located((By.XPATH, "//span[text()='See All']")))
                    browser.execute_script("arguments[0].click();", see_all)
                    dialog = wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@aria-label='Dialog']")))
                    all_children = dialog.find_elements(By.CLASS_NAME, '_7fb174a1')
                    for cc in all_children:
                        data[cc.text] = '1'
                except NoSuchElementException:
                    print("Element 'See All' not found, skipping this part.")

            location = browser.find_element(By.XPATH, "//div[@aria-label='Location']")
            button = location.find_element(By.XPATH, ".//div//div//div//div//button")
            browser.execute_script("arguments[0].click();", button)
            dialog = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, '_721fe53c')))
            image_src = dialog.get_attribute('src')
            img_src_arr = image_src.rsplit('/')
            coord = img_src_arr[8].rsplit(',')
            data['X'] = coord[0]
            data['Y'] = coord[1]
            data['img_src'] = image_src

            description = main.find('div', {'aria-label': 'Description'})
            if description:
                span = description.find('span')
                data['description'] = span.text.strip() if span else None

        print(data)
        return data
    except Exception as e:
        print(f"Error occurred for URL {url}: {e}")
        # Additional debugging info
        print(traceback.format_exc())
    return None


def scraping_data(path, file_name, listing_type):
    # Load the CSV file into a DataFrame
    urls = pd.read_csv(path + "/urls/" + file_name)
    print(f"Total URLs to process: {len(urls)}")

 
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


    all_data = []
    batch_size = 1000  # Number of records to save per batch

    with ThreadPoolExecutor(max_workers=5) as executor:  # Reduce max_workers
        future_to_url = {executor.submit(process_url, url['URL'], listing_type): url for index, url in urls.iterrows()}

        for i, future in enumerate(as_completed(future_to_url), start=1):
            print(f"Processed {i} URLs")
            url = future_to_url[future]
            try:
                page_data = future.result()
                if page_data:
                    all_data.append(page_data)
            except Exception as exc:
                print(f"{url['URL']} generated an exception: {exc}")

            # Save data to CSV every 500 records
            if i % batch_size == 0:
                df = pd.DataFrame(all_data)
                csv_file_path = f'{path}/data_{i}_{file_name}'
                df.to_csv(csv_file_path, index=False)
                print(f"Saved {i} records to {csv_file_path}")
                all_data = []  # Clear the list after saving

    # Save remaining data if any
    if all_data:
        df = pd.DataFrame(all_data)
        csv_file_path = f'{path}/data_final_{file_name}'
        df.to_csv(csv_file_path, index=False)
        print(f"Saved final batch of {len(all_data)} records to {csv_file_path}")


def merge_batches(path, file_name, current_date):
    # Find all files matching the pattern
    file_list = glob.glob(os.path.join(path, "*" + current_date + ".csv"))

    print(file_list)  # This will print all the files that match the prefix pattern
    dataframes = []

    for file in file_list:
        try:
            df = pd.read_csv(file)
            dataframes.append(df)
        except pd.errors.ParserError as e:
            print(f"Error reading {file}: {e}")

    # Concatenate all DataFrames into one (if they have the same structure)
    combined_df = pd.concat(dataframes, axis=0, ignore_index=True)

    df3 = combined_df.drop_duplicates(subset=None, keep='first', inplace=False)
    print(df3.size)
    
    if 'See All' in df3.columns:
        df3.drop(columns='See All', inplace=True)
        
    # Save DataFrame to CSV file
    df3.to_csv(f'{path}/data_{file_name}', index=False)

    # Create a new directory with the current date
    new_dir = os.path.join(path, current_date)
    os.makedirs(new_dir, exist_ok=True)

    # Rename each file by adding .old to the filename and move to the new directory
    for file in file_list:
        new_name = os.path.join(new_dir, os.path.basename(file) + '.old')
        os.rename(file, new_name)
        print(f"Renamed and moved {file} to {new_name}")
    
    fill_na_services(df3, path, file_name, current_date)


####################### Step 3 a for dubbizle ##################
def fill_na_services(df, path, file_name, current_date):
    missing_indicators = ["", "N/A", "null"]

    print(df.shape)  # Display the filtered DataFrame

    # Step 2: Remove identical (duplicate) rows
    df = df.drop_duplicates()

    columns_to_check = ['Central A/C & heating', 'Built in Kitchen Appliances']  # Replace with your specific column names

    # Step 3: Filter the DataFrame
    # Keep rows where the specified columns have values 1, None, or NaN
    filtered_df = df[df[columns_to_check].apply(lambda row: all((val in [1, 1.0, None, "1.0", "1"] or pd.isna(val)) for val in row), axis=1)]


    columns_to_fill = ['Balcony', 'Mountain View', 'Central A/C & heating', 'Covered Parking', 'Maids Room', 'Security',
                        'Shared Gym', 'Shared Pool', 'Elevator', 'Storage room', 'Sea view',
                        'Built in Kitchen Appliances', 'Built in Wardrobes', 'Pets Allowed', 'Walk-in Closet',
                        '24/7 Electricity', 'Concierge', 'Accessible', 'Terrace', 'Playroom', 'Private Jacuzzi',
                        'Fireplace', 'Private Garden', 'Study Room', 'Attic/ Loft', 'Shared Spa', 'Private Gym',
                        'Private Pool']

    filtered_df[columns_to_fill] = filtered_df[columns_to_fill].fillna(0)

    # Create a new directory with the current date
    new_dir = os.path.join(path, current_date)
    os.makedirs(new_dir, exist_ok=True)

    new_name = os.path.join(new_dir, os.path.basename(file_name) + 'combined.old')
    os.rename(path + '/data_' +file_name, new_name)

    filtered_df.to_csv(f'{path}/data_{file_name}', index=False)
    print(filtered_df.shape)  # Display the filtered DataFrame


current_datetime = os.getenv("TODAY_DATE")
#current_datetime = datetime.now().strftime('%Y-%m-%d')

csv_file_path_rent = f'dubbizle_urls_to_be_scraped_rent_{current_datetime}.csv'
csv_file_path_sale = f'dubbizle_urls_to_be_scraped_sale_{current_datetime}.csv'

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f'Current path {script_dir}')

# Construct the full file path
# csv_file_path_rent = os.path.join(script_dir, csv_file_path_rent)

# Replace the path with your file's path

def files_with_suffix_exist(directory, suffix):
    return any(file.endswith(suffix) for file in os.listdir(directory))

# if files_with_suffix_exist(script_dir + "/rent", f"{current_datetime}.csv"):
scraping_data(script_dir + "/rent", csv_file_path_rent, 'Rent')
    
# if files_with_suffix_exist(script_dir + "/sale", f"{current_datetime}.csv"):
scraping_data(script_dir + "/sale", csv_file_path_sale, 'Sell')


merge_batches(script_dir + "/rent", csv_file_path_rent, current_datetime)
merge_batches(script_dir + "/sale", csv_file_path_sale, current_datetime)

