from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import requests
from bs4 import BeautifulSoup
import logging
from ratelimit import limits, sleep_and_retry
from fake_useragent import UserAgent
from sqlalchemy import create_engine


logging.basicConfig(filename='scraper.log', level=logging.DEBUG)


@sleep_and_retry
@limits(calls=4, period=60)
def fetch_html(url):
    # Make a GET request to the webpage
    print("getting data from: " + url)

    options = webdriver.ChromeOptions()

    # set user agent header
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
    capabilities = webdriver.DesiredCapabilities.CHROME.copy()
    capabilities['acceptSslCerts'] = True
    capabilities['acceptInsecureCerts'] = True
    capabilities['loggingPrefs'] = {'browser': 'ALL'}

    # add headers to capabilities
    capabilities['chromeOptions'] = {
        'args': ['--disable-extensions', '--start-maximized']}
    headers = {'User-Agent': UserAgent().random}
    capabilities['chromeOptions']['headers'] = headers

    # create webdriver instance with desired capabilities
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')

    driver = webdriver.Chrome(executable_path='/Users/affanmir/Downloads/chromedriver_mac64(1)/chromedriver',
                            desired_capabilities=capabilities, options=options)

    # Set up proxy rotation
    proxies = {
        'http': 'http://localhost:8000',
        'https': 'https://localhost:8000'
    }

    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        html_content = driver.page_source

        return html_content
        driver.quit()

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        driver.quit()
        return None


def scrape_apartments(url):
    html = fetch_html(url)
    update_lst = []

    if html:
        soup = BeautifulSoup(html, 'html.parser')
        apartments = []
        try:
            listings = soup.find('ul', {'class': 'listings'})
            li_tags = listings.find_all(
                'li', {'class': 'listing-details listing-type-standard'})
        except:
            return []
        for result in li_tags:
            Final_Object = {}
            try:
                presentation = result.find(
                    'div', {'class': 'listing-presentation'})
                presentation = presentation.find(
                    'div', {'class': 'listing-photo property'})
                presentation = presentation.find(
                    'img', {'class': 'cursor-pointer photo property'})
                picture_caption = presentation['alt']
                Final_Object['Caption'] = picture_caption
            except:
                pass
            try:
                listing_information = result.find(
                    'div', {'class': 'listing-information'})
                listing_address = listing_information.find(
                    'div', {'class': 'listing-name-address'})
                building_name = listing_address.find(
                    'h2', {'class': 'listing-name building-name'})
                building_name = building_name['title']
                Final_Object['Building Address'] = building_name
            except:
                pass
            try:
                building_name = listing_information.find(
                    'ul', {'class': 'listing-beds'})
                data_rent = building_name.find_all(
                    'li', {'class': 'data-beds'})
                data_rent = list(data_rent)
                data_rent_0 = data_rent[0].text
                data_rent_1 = data_rent[-1].text
                Final_Object['Beds info 0'] = data_rent_0
                Final_Object['Beds info 1'] = data_rent_1
            except:
                pass
            try:
                geo = listing_information.find('div', {'itemprop': 'geo'})
                lat = geo.find('meta', {'itemprop': 'latitude'})
                long = geo.find('meta', {'itemprop': 'longitude'})

                Final_Object['Latitude'] = lat['content']
                Final_Object['Longitude'] = long['content']
            except:
                pass

            update_lst.append(Final_Object)

        return update_lst


additional = '?page='
count = 1
output_lst = []
while True:
    if count < 2:
        output_lst += scrape_apartments(
            'https://www.rentcafe.com/apartments-for-rent/us/il/chicago/')
    elif count < 21:
        time.sleep(25)
        output_lst += scrape_apartments(
            'https://www.rentcafe.com/apartments-for-rent/us/il/chicago/'+additional+str(count))
    else:
        break
    count += 1



my_conn = create_engine("postgresql://postgres@localhost:5432/postgres")

for i in output_lst:
    id = my_conn.execute(f"""INSERT INTO scrapped ( "Caption", "Address", "Beds_info_0", "Beds_info_1", "Latitude", "Longitude", "scraped_at", "updated_at") \
                    VALUES ('{i['Caption']}', '{i['Building Address']}', '{i['Beds info 0']}', '{i['Beds info 1']}', '{i['Latitude']}', '{i['Longitude']}', '{time.time()}','{time.time()}')""")
    print("Row Added  = ", id.rowcount)


