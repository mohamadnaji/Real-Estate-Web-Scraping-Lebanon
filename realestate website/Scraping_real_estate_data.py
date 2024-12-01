from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import concurrent.futures
import os
import traceback
from datetime import datetime
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options
from concurrent.futures import ThreadPoolExecutor
from requests.structures import CaseInsensitiveDict


def get_coordinates(loc):
    location_split = loc.split(",")
    locationn = location_split[0] + ',' + location_split[-1]
    url = f"https://api.geoapify.com/v1/geocode/search?text={locationn}&filter=countrycode:lb&apiKey=f543f0f7b9f54b6880d8c341a6692ce6"

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    print(url)
    try:

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
    except Exception as exc:
        print(f"{url} generated an exception: {exc}")

    return None


def coordinates(path):
    df1 = pd.read_csv(path)
    for index, row in df1.iterrows():
        print(index)
        # img = row['img_src']
        x = 0
        y = 0
        # if img is not None:
        #     img = str(img)

        location = str(row['location_str'])
        coordinates = get_coordinates(location)
        if coordinates is not None:
            x = coordinates[0]
            y = coordinates[1]

        df1.at[index, 'X'] = x
        df1.at[index, 'Y'] = y
    df1.to_csv(path, index=False)


def scrape_page(url, listing_type, browser, wait):
    data = {}
    try:

        # response = requests.get(url)
        # if response.status_code != 200:
        #     print(f"Failed to retrieve the page. Status code: {response.status_code}")
        #     return None

        browser.get(url)
        element_present = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'cssen-1wavjlf')))
        # Continue with your scraping logic

        # Scroll down incrementally
        # for i in range(10):  # Scroll down 10 times
        #     browser.execute_script("window.scrollBy(0, 800);")
        # time.sleep(0.5)  # Adjust sleep time if needed

        # Get the rendered HTML
        html = browser.page_source

        extraction_datetime = datetime.now().strftime('%Y-%m-%d')
        # Initiate data object
        data = {'source_name': 'realEstate', 'source': url, 'Listing Type': listing_type,
                'extraction date': extraction_datetime}

        # Parse the HTML content
        soup = BeautifulSoup(html, 'html.parser')

        # header that contains the price and location string
        body = soup.find('div', attrs={'class': 'cssen-1wavjlf'})

        # properties data
        general_data = body.find('div', attrs={'class': 'cssen-isbt42'})
        properties = general_data.find_all('div', attrs={'class': 'cssen-1bhwfz0'})
        for prop in properties:
            ps = prop.find_all('p')
            key = ps[0].text.strip().replace(':', '')
            value = ps[1].text.strip()
            data[key] = value

        # price
        price = body.find('p', attrs={'class': 'cssen-j3qa3r'})
        pricee = price.text.strip()
        price_splited = pricee.split(' / ')
        if len(price_splited) > 1:
            data['Rental duration'] = price_splited[1]
        data['price'] = price_splited[0]

        # location
        location = body.find('div', attrs={'class': 'cssen-6lzzp6'})
        location_str = location.find_all('p')
        location_str = f'{location_str[0].text.strip()}, {location_str[1].text.strip()}'
        data['location_str'] = location_str

        amenities = body.find_all('div', attrs={'class': 'cssen-14zw9uq'})

        services = []
        for amenity in amenities:
            data[amenity.text.strip(u'\u200b')] = 1
            services.append(amenity.text.strip(u'\u200b'))
        description = body.find('p', attrs={'class': 'cssen-1ber2fy'})
        data['description'] = description.text.strip()

        reference = body.find('div', attrs={'class': 'cssen-164krv5'})
        reference = reference.find_all('p')
        data[reference[0].text.strip()] = reference[1].text.strip()

        return data, services

    except Exception as e:
        print(f"Error occurred for URL {url}: {e}")
        # Additional debugging info
        print(traceback.format_exc())
    return None


def scraping_data(path, file_name, listing_type):
    # Load the CSV file into a DataFrame
    urls = pd.read_csv(path + 'urls/' + file_name)
    print(len(urls))
    all_data = []
    all_services = []
    failed_url = []
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

    with ThreadPoolExecutor(max_workers=5) as executor:  # Reduce max_workers
        future_to_url = {executor.submit(process_url, url['URL'], listing_type): url for index, url in urls.iterrows()}

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                page_data, services = future.result()
                if page_data:
                    all_data.append(page_data)
                    all_services.extend(services)
                index += 1
                print(index)
            except Exception as exc:
                print(f"{url['URL']} generated an exception: {exc}")
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
        'Reference': 'Reference Id',
        'Property size': 'Size (m²)',
        'Price Type': 'Rental duration'
    })
    csv_file_path = f'{path}data_{file_name}'
    df.to_csv(csv_file_path, index=False)
    return all_data, failed_url

current_datetime = os.getenv("TODAY_DATE")
#current_datetime = datetime.now().strftime('%Y-%m-%d')
csv_file_path_rent = f'realEstate_urls_to_be_scraped_rent_{current_datetime}.csv'
csv_file_path_sale = f'realEstate_urls_to_be_scraped_sale_{current_datetime}.csv'
csv_file_path_commercial_rent = f'realEstate_urls_to_be_scraped_commercial_rent_{current_datetime}.csv'
csv_file_path_commercial_sale = f'realEstate_urls_to_be_scraped_commercial_sale_{current_datetime}.csv'

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f'Current path {script_dir}')

# Construct the full file path
if os.path.exists(script_dir + "/rent/data_" + csv_file_path_rent):
    print(f'File {os.path.join(script_dir + "/rent", "data_" + csv_file_path_rent)} already scraped')
else:
    scraping_data(script_dir + "/rent/", csv_file_path_rent, 'Rent')
coordinates(script_dir + "/rent/data_" + csv_file_path_rent)

if os.path.exists(os.path.join(script_dir + "/sale", 'data_' + csv_file_path_sale)):
    print(f'File {os.path.join(script_dir + "/sale", "data_" + csv_file_path_sale)} already scraped')
else:
    scraping_data(script_dir + "/sale/", csv_file_path_sale, 'Sell')
coordinates(script_dir + "/sale/data_" + csv_file_path_sale)

if os.path.exists(os.path.join(script_dir + "/rent", 'data_' + csv_file_path_commercial_rent)):
    print(f'File {os.path.join(script_dir + "/rent", "data_" + csv_file_path_commercial_rent)} already scraped')
else:
    scraping_data(script_dir + "/rent/", csv_file_path_commercial_rent, 'Rent')
coordinates(script_dir + "/rent/data_" + csv_file_path_commercial_rent)

if os.path.exists(os.path.join(script_dir + "/sale", 'data_' + csv_file_path_commercial_sale)):
    print(f'File {os.path.join(script_dir + "/sale", "data_" + csv_file_path_commercial_sale)} already scraped')
else:
    scraping_data(script_dir + "/sale/", csv_file_path_commercial_sale, 'Sell')
coordinates(script_dir + "/sale/data_" + csv_file_path_commercial_sale)

