#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
import os
import concurrent.futures
from bs4 import BeautifulSoup
from datetime import datetime


# Function to scrape URLs with a specific prefix
def scrape_urls_with_prefix(url):
    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200 and url == response.url:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all 'a' tags
        a_tags = soup.find_all('a', class_="h-100")
        if not a_tags:
            print(f"No 'a' tags found on page {url}")
            return None
        # Extract the href attribute of each a tag that starts with the specified prefix
        filtered_urls = [a.get('href') for a in a_tags if a.get('href') and a.get('href')]
        print(f"Scraped {len(filtered_urls)} URLs from {url}")
        return set(filtered_urls)
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        return None


def scrape_urls(initial_url):
    urls = set()
    idx = 1
    while True:
        # Scrape URLs with the specified prefix
        print(f'Scraping page {idx}')
        new_urls = scrape_urls_with_prefix(f'{initial_url}?page={idx}')
        if new_urls is None:
            break
        urls.update(new_urls)
        idx += 1
    return urls

def save_urls_to_csv(urls, csv_file_path):

    urls = scrape_urls(urls)
    print(f'Number of urls: {len(urls)}')

    # Convert set to list
    data_list = list(urls)

    # Create a DataFrame from the list
    df = pd.DataFrame(data_list, columns=['URL'])

    # Save the DataFrame to an Excel file
    df.to_csv(csv_file_path, index=False)


current_datetime = os.getenv("TODAY_DATE")
#current_datetime = datetime.now().strftime('%Y-%m-%d')
csv_file_path_rent = f'pbm_urls_to_be_scraped_rent_{current_datetime}.csv'
csv_file_path_sale = f'pbm_urls_to_be_scraped_sale_{current_datetime}.csv'

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

url_rent = f'https://pbm-leb.com/en/rent'
url_sale = f'https://pbm-leb.com/en/buy'


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
