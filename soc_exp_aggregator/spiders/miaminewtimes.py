from pathlib import Path
from datetime import datetime
import threading
import time
import csv
import re
import json
import scrapy
import requests
import psycopg2
import urllib.parse
from bs4 import BeautifulSoup
from ..settings import PGSQL_HOST, PGSQL_DBNAME, PGSQL_USERNAME, PGSQL_PASSWORD, PGSQL_PORT, SCRAPER_API_KEY, SCRAPE_DO_API_KEY

CITY_LIST = ['buenos-aires', 'adelaide', 'brisbane', 'melbourne', 'perth', 'sydney', 'wien', 'antwerpen', 'bruxelles', 'rio-de-janeiro', 's%C3%A3o-paulo', 'calgary', 'edmonton', 'halifax', 'montr%C3%A9al', 'ottawa', 'toronto', 'vancouver', 'santiago', 'praha', 'k%C3%B8benhavn', 'helsinki', 'lyon', 'marseille', 'paris', 'berlin', 'frankfurt-am-main', 'hamburg', 'k%C3%B6ln', 'm%C3%BCnchen', 'hk', 'milano', 'roma', '%E5%A4%A7%E9%98%AA%E5%B8%82', '%E6%9D%B1%E4%BA%AC', 'kl', 'm%C3%A9xico-df', 'auckland', 'oslo', 'manila', 'krak%C3%B3w', 'warszawa', 'lisboa', 'dublin', 'singapore', 'barcelona', 'madrid', 'stockholm', 'z%C3%BCrich', '%E5%8F%B0%E5%8C%97%E5%B8%82', 'amsterdam', 'istanbul', 'belfast', 'brighton', 'bristol', 'cardiff', 'edinburgh', 'glasgow', 'leeds', 'liverpool', 'london', 'manchester', 'phoenix', 'scottsdale-az-us', 'tempe-az-us', 'tucson-az-us', 'alameda-ca-us', 'albany-ca-us', 'alhambra-ca-us', 'anaheim-ca-us', 'belmont-ca-us', 'berkeley', 'beverly-hills-ca-us', 'big-sur-ca-us', 'la-east', 'concord-ca-us', 'costa-mesa-ca-us', 'culver-city-ca-us', 'cupertino-ca-us', 'daly-city-ca-us', 'davis', 'dublin-ca-us', 'emeryville-ca-us', 'foster-city-ca-us', 'fremont-ca-us', 'glendale-ca-us', 'hayward-ca-us', 'healdsburg-ca-us', 'huntington-beach-ca-us', 'irvine-ca-us', 'la-jolla-ca-us', 'livermore-ca-us', 'long-beach-ca-us', 'los-altos-ca-us', 'la', 'los-gatos-ca-us', 'marina-del-rey-ca-us', 'menlo-park-ca-us', 'mill-valley-ca-us', 'millbrae-ca-us', 'milpitas-ca-us', 'monterey-ca-us', 'mountain-view-ca-us', 'napa-ca-us', 'newark-ca-us', 'newport-beach-ca-us', 'oakland', 'oc', 'palo-alto', 'park-la-brea-ca-us', 'pasadena-ca-us', 'pleasanton-ca-us', 'redondo-beach-ca-us', 'redwood-city-ca-us', 'sacramento', 'san-bruno-ca-us', 'san-carlos-ca-us', 'san-diego', 'sf', 'san-jose', 'san-leandro-ca-us', 'san-mateo-ca-us', 'san-rafael-ca-us', 'santa-barbara-ca-us', 'santa-clara-ca-us', 'santa-cruz-ca-us', 'santa-monica-ca-us', 'santa-rosa-ca-us', 'sausalito-ca-us', 'sonoma-ca-us', 'south-lake-tahoe-ca-us', 'stockton-ca-us', 'studio-city-ca-us', 'sunnyvale-ca-us', 'torrance-ca-us', 'union-city-ca-us', 'venice-ca-us', 'walnut-creek-ca-us', 'west-hollywood-ca-us', 'west-los-angeles-ca-us', 'westwood-ca-us', 'yountville-ca-us', 'boulder', 'denver', 'hartford', 'new-haven-ct-us', 'dc', 'fort-lauderdale', 'gainesville', 'miami', 'miami-beach-fl-us', 'orlando-fl-us', 'tampa-bay', 'atlanta', 'savannah', 'honolulu', 'lahaina-hi-us', 'iowa-city', 'boise', 'chicago', 'evanston-il-us', 'naperville-il-us', 'schaumburg-il-us', 'skokie-il-us', 'bloomington-in-us', 'indianapolis-in-us', 'louisville', 'new-orleans', 'allston-ma-us', 'boston', 'brighton-ma-us', 'brookline-ma-us', 'cambridge-ma-us', 'somerville-ma-us', 'baltimore', 'ann-arbor-mi-us', 'detroit', 'minneapolis', 'saint-paul-mn-us', 'kansas-city-mo-us', 'st-louis', 'charlotte-nc-us', 'durham-nc-us', 'raleigh-nc-us', 'newark-nj-us', 'princeton-nj-us', 'albuquerque', 'santa-fe-nm-us', 'las-vegas', 'reno', 'brooklyn', 'long-island-city-ny-us', 'nyc', 'queens', 'cincinnati-oh-us', 'cleveland', 'columbus-oh-us', 'portland', 'salem-or-us', 'philadelphia', 'pittsburgh', 'providence', 'charleston', 'memphis', 'nashville', 'austin', 'dallas', 'houston', 'san_antonio', 'salt-lake-city', 'alexandria-va-us', 'arlington-va-us', 'richmond', 'burlington', 'bellevue-wa-us', 'redmond-wa-us', 'seattle', 'madison', 'milwaukee']
class MiamiNewTimesSpider:
    name = "miaminewtimes"
    LIST_URL = "https://www.miaminewtimes.com/miami/EventSearch?page={}&sortType=date&v=g"
    SCRAPE_DO_PREFIX = f'http://api.scrape.do?token={SCRAPE_DO_API_KEY}&url='
    BASE_URL = "https://www.yelp.com"
    existing_id_list = []
    total_count = 0
    def start_requests(self):
        self.connection = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
        self.cur = self.connection.cursor()
        self.existing_id_list = []
        self.cur.execute("SELECT src_id FROM event_list WHERE source='miaminewtimes'")
        rows = self.cur.fetchall()
        for row in rows:
            self.existing_id_list.append(row[0])

        # for page in range(1, 40):
        self.parse_event_list(self.LIST_URL.format(1), 1)
            
    
    def parse_event_list(self, url, page_index):
        response = requests.request("GET", self.SCRAPE_DO_PREFIX + urllib.parse.quote(url))
        soup = BeautifulSoup(response.text, features="html.parser")
        events = soup.find_all('p', class_='fdn-teaser-headline')
        self.total_count = self.total_count + len(events)
        threads = []
        for event in events:
            
            
            link = event.find('a').attrs['href']
            id = link.split('/')[-1]
            if id in self.existing_id_list:
                print('EXISTING')
                continue
            else:
                self.existing_id_list.append(id)
            t = threading.Thread(target=self.parse_event_detail, args=(link, url, ))
            threads.append(t)
        for thread in threads:
            thread.start()

        # # Wait for all threads to finish
        for thread in threads:
            thread.join()
        threads = []
        if len(events) > 0:
            self.parse_event_list(self.LIST_URL.format(page_index), page_index + 1)
       
    def parse_event_detail(self, url, parent):
        response = requests.request("GET", self.SCRAPE_DO_PREFIX + urllib.parse.quote(url))
        soup = BeautifulSoup(response.text, features="html.parser")
        event_item = {}
        event_item['source'] = self.name
        event_item['src_id'] = url.split('/')[-1]
        
        event_item['src_url'] = url

        event_item['title'] = soup.find('h1', class_='fdn-listing-headline')
        if event_item['title']:
            event_item['title'] = event_item['title'].text.strip()

        event_item['summary'] = soup.find('div', class_='fdn-listing-description')
        if event_item['summary']:
            event_item['summary'] = event_item['summary'].text.strip()

        event_item['timezone'] = None

        event_item['image'] = soup.find('div', class_='fdn-magnum-block')
        if event_item['image']:
            event_item['image'] = event_item['image'].find('img')
            if event_item['image']:
                event_item['image'] = event_item['image'].attrs['src']

        event_item['start_date'] = None
        event_item['start_time'] = None
        event_item['end_date'] = None
        event_item['end_time'] = None
        date_range = soup.find('p', class_='uk-margin-xsmall')
        
        if date_range:
            event_item['start_date'] = date_range.text.strip().replace('When:', '').strip()
            
        event_item['status'] = None
        ticket_available = soup.find('span', class_='fdn-features-icon-tickets-available')
        if ticket_available:
            event_item['is_sold_out'] = False
        else:
            event_item['is_sold_out'] = True

        event_item['is_online_event'] = None
        event_item['is_free'] = None
        event_item['min_price'] = None
        event_item['max_price'] = None
        event_item['currency'] = '$'

        price_info = soup.find('span', class_='fdn-teaser-ticket-link-price')
        if price_info:
            price_info = price_info.text.strip()
            if price_info == 'Free':
                event_item['is_free'] = True
            else:
                if price_info != 'TBA':
                    price_list = price_info.replace(',', '').split('-')
                    event_item['min_price'] = price_list[0].strip()
                    if 'Free' in event_item['min_price'] or event_item['min_price'] == '':
                        event_item['min_price'] = 0
                    else:
                        print(event_item['min_price'])
                        match = re.search(r'\d+(\.\d{1,2})?', event_item['min_price'])
                        event_item['min_price'] = match.group()
                        event_item['currency'] = price_list[0].strip().replace(event_item['min_price'], '')
                    if len(price_list) > 1:
                        event_item['max_price'] = price_list[1].strip()
                        event_item['max_price'] = event_item['max_price'].replace(event_item['currency'], '')
                    
        
        event_item['tags'] = soup.find('div', class_='EventTags')
        if event_item['tags']:
            tags = event_item['tags'].find_all('a')
            tag_arr = []
            for tag in tags:
                tag_arr.append(tag.text.strip())
            event_item['tags'] = ','.join(tag_arr)
        event_item['organizer'] = None
        event_item['organizer_url'] = None
        event_item['published_at'] = None

        event_item['venue_name'] = soup.find('p', class_='fdn-teaser-headline')
        if event_item['venue_name']:
            event_item['venue_name'] = event_item['venue_name'].text.strip()
        
        event_item['venue_address_2'] = None
        event_item['venue_country'] = None
        event_item['venue_region'] = None
        event_item['venue_city'] = None
        event_item['venue_address_1'] = None
        event_item['venue_latitude'] = None
        event_item['venue_longitude'] = None
        event_item['venue_postal_code'] = None
        event_item['venue_display_address'] = soup.find('p', class_='fdn-inline-split-list')
        if event_item['venue_display_address']:
            event_item['venue_display_address'] = event_item['venue_display_address'].find('span').text.strip()

        event_item['scraped_at'] = datetime.now().isoformat()
        event_item['id'] = '{}-{}'.format(self.name, event_item['src_id'])
        event_item['group_id'] = '{}-{}'.format(self.name, event_item['src_id'])
        self.process_item(event_item)
        # print(event_item)

        print('#####################')

    def process_item(self, item):
        try :
            print('#############################')
            print(item)
            self.cur.execute("INSERT INTO event_list(id, source, src_id, title, summary, timezone, start_date, end_date, start_time, end_time, status, is_free, is_sold_out, is_online_event, max_price, min_price, currency, tags, organizer, organizer_url, src_url,  image,  published_at, venue_name, venue_country, venue_region, venue_city, venue_postal_code, venue_address_1, venue_address_2, venue_latitude, venue_longitude, venue_display_address, scraped_at, group_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                item['id'],
                item['source'],
                item['src_id'],
                item['title'],
                item['summary'],
                item['timezone'],
                item['start_date'],
                item['end_date'],
                item['start_time'],
                item['end_time'],
                item['status'],
                item['is_free'],
                item['is_sold_out'],
                item['is_online_event'],
                item['max_price'],
                item['min_price'],
                item['currency'],
                item['tags'],
                item['organizer'],
                item['organizer_url'],
                item['src_url'],
                item['image'],
                item['published_at'],
                item['venue_name'],
                item['venue_country'],
                item['venue_region'],
                item['venue_city'],
                item['venue_postal_code'],
                item['venue_address_1'],
                item['venue_address_2'],
                item['venue_latitude'],
                item['venue_longitude'],
                item['venue_display_address'],
                item['scraped_at'],
                item['group_id'],
            ))
            self.connection.commit()
        except Exception as e:
            self.cur.close()
            self.connection.close()
            self.connection = psycopg2.connect(
            host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
            self.cur = self.connection.cursor()
            print(e)
        return item