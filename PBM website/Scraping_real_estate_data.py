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
import traceback
import concurrent.futures
from requests.structures import CaseInsensitiveDict
from datetime import datetime
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
import math


def extract_tile_coordinates(url_value):
    # Split the URL by '/' to get its components
    parts = url_value.split('/')

    # Extract the zoom level, x, and y values
    zoom_level = parts[-3]
    x1 = parts[-2]
    y1 = parts[-1].split('.')[0]  # Remove the file extension

    return int(x1), int(y1)


def tile_to_lon_lat(xo, yo, zoom):
    n = 2.0 ** zoom
    lon_deg = xo / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * yo / n)))
    lat_deg = math.degrees(lat_rad)
    return lat_deg, lon_deg


def get_coordinates(loc):
    location_split = loc.split(",")
    locationn = location_split[-1]
    url = f"https://api.geoapify.com/v1/geocode/search?text={locationn}&filter=countrycode:lb&apiKey=f543f0f7b9f54b6880d8c341a6692ce6"

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"

    resp = requests.get(url, headers=headers)
    if resp.ok:
        content = resp.json()
        features = content['features']
        if len(features) > 0:
            rank = 0
            final_coordinate = [0, 0]
            for f in features:
                properties = f['properties']
                ranks = properties['rank']
                current_rank = ranks['confidence']
                if current_rank > rank and current_rank > 0.93:
                    final_coordinate[0] = properties['lon']
                    final_coordinate[1] = properties['lat']
            return final_coordinate
    return None


def coordinates(df1, path):
    for index, row in df1.iterrows():
        print(index)
        img = row['img_src']
        x = 0
        y = 0
        if img is not None:
            img = str(img)

            location = str(row['location_str'])
            coordinates = get_coordinates(location)
            if coordinates is not None:
                x = coordinates[0]
                y = coordinates[1]
            else:
                lon, lat = extract_tile_coordinates(img)
                y, x = tile_to_lon_lat(lon, lat, 15)

        df1.at[index, 'X'] = x
        df1.at[index, 'Y'] = y
    df1.to_csv(path, index=False)


def scrape_page(url, listing_type, browser, wait):
    data = {}
    try:

        url = f'https://pbm-leb.com{url}'

        browser.get(url)

        # Wait for the page to load
        element_present = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'leaflet-tile-container')))
        ppp = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'price')))

        # Scroll down incrementally
        # for i in range(10):  # Scroll down 10 times
        #     browser.execute_script("window.scrollBy(0, 800);")
        #     time.sleep(0.2)  # Adjust sleep time if needed

        # Get the rendered HTML
        html = browser.page_source

        data = {'source_name': 'PBM-lebanon', 'source': url, 'Listing Type': listing_type}
        # data['source'] = url

        extraction_datetime = datetime.now().strftime('%Y-%m-%d')
        data['extraction date'] = extraction_datetime

        # Parse the HTML content
        soup = BeautifulSoup(html, 'html.parser')

        # header that contains the price and location string
        header = soup.find('div', attrs={'data-id': '2'})
        locationStr = header.find('h2')
        price = header.find('p', attrs={'class': 'price'})
        if price is None:
            return None
        splitted_price = price.text.strip().split(' / ')
        if listing_type == 'Rent' and len(splitted_price) > 1:
            data['Rental duration'] = splitted_price[1]

        if listing_type == 'Sell' and len(splitted_price) > 1:
            return None

        data['price'] = splitted_price[0]

        ss = locationStr.text.strip().split(" in ")
        if len(ss) > 1:
            data['location_str'] = ss[1]
        else:
            data['location_str'] = ss[0]

        # body content details
        body = soup.find('div', attrs={'data-id': '4'})

        body_details = body.find('div', attrs={'id': 'details'})
        body_details_list = body_details.find_all('p')
        for detail in body_details_list:
            value = detail.find('span').text.strip()
            detail.find('span').decompose()
            key = detail.text.strip()
            if key is not None and key != '':
                data[key.replace(':', '')] = value

        # body content area data
        h3 = body.find('h3')
        ul = h3.find_next_sibling()
        lis = ul.find_all('li')
        for li in lis:
            value = li.text.strip()
            value = value.replace('- ', '')
            jj = value.split(' ')
            if len(jj) > 1:
                kkey = jj[1].strip()
                if kkey is not None and kkey != '':
                    if 'bedroom' not in kkey.lower() and 'bathroom' not in kkey.lower():
                        data[jj[1].strip()] = jj[0].strip()
            else:
                data[jj[0].strip()] = 1

        # map coordinate url
        map = soup.find('div', {'class': 'leafletmap-template-1'})
        img = map.find('img')
        if img is not None:
            data['img_src'] = img.get('src')

        # Proximities
        # proximities = soup.find('div', {'class' : 'template-8'})
        # data['proximities'] = proximities.text.strip()

        # Services
        service = body.find('div', {'class': 'property-info-template-6'})
        services = service.find_all('li')

        servicess = []
        for li in services:
            value = li.text.strip()
            data[value] = 1
            servicess.append(value)

        # info
        # info = body.find('div', {'class' : 'info'})
        # parag = info.find('p')
        # data['info'] = parag.text.strip()

        # description
        description = soup.find(id="description")
        data['description'] = description.text.strip()

        print(data)
        return data, servicess
    finally:
        browser.quit()  # Close the browser even if an exception occurs

    # except TimeoutException:
    #     print("Timed out waiting for page to load")
    # except ConnectionError as e:
    #     print(f"Connection error occurred: {e}")
    # except Timeout as e:
    #     print(f"Timeout error occurred: {e}")
    # except TooManyRedirects as e:
    #     print(f"Too many redirects: {e}")
    # except RequestException as e:
    #     print(f"An error occurred RequestException: {e}")
    # except Exception as e:
    #     print(f"An error occurred: {e}")

    return None


def scraping_data(path, file_name, listing_type):
    # Load the CSV file into a DataFrame
    urls = pd.read_csv(path + "urls/" + file_name)
    print(len(urls))
    all_data = []
    failed_url = []
    all_services = []
    index = 2

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
                print(index)
                page_data, services = future.result()
                if page_data is not None:
                    all_data.append(page_data)
                    all_services.extend(services)
                index += 1
            except Exception as exc:
                print(f"{url['URL']} generated an exception: {exc}")
                print(traceback.format_exc())
                failed_url.append(url['URL'])

    # Filter out None values
    filtered_data = [item for item in all_data if item is not None]

    # Create DataFrame
    df = pd.DataFrame(filtered_data)
    services_set = set(all_services)
    all_services = list(services_set)
    df[all_services] = df[all_services].fillna(0)

    # Rename columns
    df = df.rename(columns={
        'Property ID': 'Reference Id',
        'Area Size': 'Size (m²)',
        'Price Type': 'Rental duration',
        'Bathrooms(s)': 'Bathrooms',
        'Bedrooms(s)': 'Bedrooms'
    })
        
    if 'Well' in df.columns:
        df.drop(columns='Well', inplace=True)
        
    csv_file_path = f'{path}data_{file_name}'
    df.to_csv(csv_file_path, index=False)
    return df, csv_file_path

current_datetime = os.getenv("TODAY_DATE")
#current_datetime = datetime.now().strftime('%Y-%m-%d')
csv_file_path_rent = f'pbm_urls_to_be_scraped_rent_{current_datetime}.csv'
csv_file_path_sale = f'pbm_urls_to_be_scraped_sale_{current_datetime}.csv'

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f'Current path {script_dir}')

# Construct the full file path
# csv_file_path_rent = os.path.join(script_dir, csv_file_path_rent)

# Replace the path with your file's path
s_data, s_path = scraping_data(script_dir + "/sale/", csv_file_path_sale, 'Sell')
r_data, r_path = scraping_data(script_dir + "/rent/", csv_file_path_rent, 'Rent')

coordinates(s_data, s_path)
coordinates(r_data, r_path)
