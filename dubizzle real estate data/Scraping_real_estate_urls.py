#!/usr/bin/env python
# coding: utf-8


import requests
import pandas as pd
import os
import time
import concurrent.futures
from datetime import datetime
from bs4 import BeautifulSoup


# Function to scrape URLs with a specific prefix
def scrape_urls_with_prefix(url, prefix):
    # Send a GET request to the URL

    sleep_time = 0
    while True:
        response = requests.get(url)
        time.sleep(sleep_time)

        # Check if the request was successful
        if response.status_code == 200 and url == response.url:
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all 'a' tags
            a_tags = soup.find_all('a')

            # Extract the href attribute of each a tag that starts with the specified prefix
            filtered_urls = [a.get('href') for a in a_tags if a.get('href') and a.get('href').startswith(prefix)]
            print(f"Scraped {len(filtered_urls)} URLs from {url}")
            if len(filtered_urls) > 0:
                return set(filtered_urls)
            else:
                if sleep_time < 3:
                    sleep_time += 1
                else:
                    break
        else:
            print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            return None

def scrape_urls(initial_url, prefix):
    urls = set()
    idx = 1
    while True:
        idx += 1
        # Scrape URLs with the specified prefix
        print(f'Scraping page {idx}')
        new_urls = scrape_urls_with_prefix(f'{initial_url}?page={idx}', prefix)

        if idx > 400:
            break
        if new_urls is not None:
                urls.update(new_urls)
    return urls


def save_urls_to_csv(prefix, urls, csv_file_path):

    urls = scrape_urls(urls, prefix)
    print(f'Number of urls: {len(urls)}')

    # Convert set to list
    data_list = list(urls)

    # Create a DataFrame from the list
    df = pd.DataFrame(data_list, columns=['URL'])

    # Save the DataFrame to an Excel file
    df.to_csv(csv_file_path, index=False)
    print(f'File {csv_file_path} saved')



current_datetime = os.getenv("TODAY_DATE")
prefix = '/ad/'
url_rent = f'https://www.dubizzle.com.lb/properties/apartments-villas-for-rent/'
csv_file_path_rent = f'dubbizle_urls_to_be_scraped_rent_{current_datetime}.csv'

url_sale = f'https://www.dubizzle.com.lb/properties/apartments-villas-for-sale/'
csv_file_path_sale = f'dubbizle_urls_to_be_scraped_sale_{current_datetime}.csv'

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full file path
csv_file_path_rent = os.path.join(script_dir + "/rent/urls", csv_file_path_rent)
csv_file_path_sale = os.path.join(script_dir + "/sale/urls", csv_file_path_sale)

if os.path.exists(csv_file_path_rent):
    print(f'File {csv_file_path_rent} already scraped')
else:
    save_urls_to_csv(prefix, url_rent, csv_file_path_rent)

if os.path.exists(csv_file_path_sale):
    print(f'File {csv_file_path_sale} already scraped')
else:
    save_urls_to_csv(prefix, url_sale, csv_file_path_sale)

