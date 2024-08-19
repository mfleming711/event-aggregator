from pathlib import Path
from datetime import datetime
import time
import csv
import re
import json
import scrapy
import psycopg2
from bs4 import BeautifulSoup
from ..items import EventItem, EventItemV2
from ..settings import PGSQL_HOST, PGSQL_DBNAME, PGSQL_USERNAME, PGSQL_PASSWORD, PGSQL_PORT
from scrapy_selenium import SeleniumRequest
CITY_LIST = ['buenos-aires', 'adelaide', 'brisbane', 'melbourne', 'perth', 'sydney', 'wien', 'antwerpen', 'bruxelles', 'rio-de-janeiro', 's%C3%A3o-paulo', 'calgary', 'edmonton', 'halifax', 'montr%C3%A9al', 'ottawa', 'toronto', 'vancouver', 'santiago', 'praha', 'k%C3%B8benhavn', 'helsinki', 'lyon', 'marseille', 'paris', 'berlin', 'frankfurt-am-main', 'hamburg', 'k%C3%B6ln', 'm%C3%BCnchen', 'hk', 'milano', 'roma', '%E5%A4%A7%E9%98%AA%E5%B8%82', '%E6%9D%B1%E4%BA%AC', 'kl', 'm%C3%A9xico-df', 'auckland', 'oslo', 'manila', 'krak%C3%B3w', 'warszawa', 'lisboa', 'dublin', 'singapore', 'barcelona', 'madrid', 'stockholm', 'z%C3%BCrich', '%E5%8F%B0%E5%8C%97%E5%B8%82', 'amsterdam', 'istanbul', 'belfast', 'brighton', 'bristol', 'cardiff', 'edinburgh', 'glasgow', 'leeds', 'liverpool', 'london', 'manchester', 'phoenix', 'scottsdale-az-us', 'tempe-az-us', 'tucson-az-us', 'alameda-ca-us', 'albany-ca-us', 'alhambra-ca-us', 'anaheim-ca-us', 'belmont-ca-us', 'berkeley', 'beverly-hills-ca-us', 'big-sur-ca-us', 'la-east', 'concord-ca-us', 'costa-mesa-ca-us', 'culver-city-ca-us', 'cupertino-ca-us', 'daly-city-ca-us', 'davis', 'dublin-ca-us', 'emeryville-ca-us', 'foster-city-ca-us', 'fremont-ca-us', 'glendale-ca-us', 'hayward-ca-us', 'healdsburg-ca-us', 'huntington-beach-ca-us', 'irvine-ca-us', 'la-jolla-ca-us', 'livermore-ca-us', 'long-beach-ca-us', 'los-altos-ca-us', 'la', 'los-gatos-ca-us', 'marina-del-rey-ca-us', 'menlo-park-ca-us', 'mill-valley-ca-us', 'millbrae-ca-us', 'milpitas-ca-us', 'monterey-ca-us', 'mountain-view-ca-us', 'napa-ca-us', 'newark-ca-us', 'newport-beach-ca-us', 'oakland', 'oc', 'palo-alto', 'park-la-brea-ca-us', 'pasadena-ca-us', 'pleasanton-ca-us', 'redondo-beach-ca-us', 'redwood-city-ca-us', 'sacramento', 'san-bruno-ca-us', 'san-carlos-ca-us', 'san-diego', 'sf', 'san-jose', 'san-leandro-ca-us', 'san-mateo-ca-us', 'san-rafael-ca-us', 'santa-barbara-ca-us', 'santa-clara-ca-us', 'santa-cruz-ca-us', 'santa-monica-ca-us', 'santa-rosa-ca-us', 'sausalito-ca-us', 'sonoma-ca-us', 'south-lake-tahoe-ca-us', 'stockton-ca-us', 'studio-city-ca-us', 'sunnyvale-ca-us', 'torrance-ca-us', 'union-city-ca-us', 'venice-ca-us', 'walnut-creek-ca-us', 'west-hollywood-ca-us', 'west-los-angeles-ca-us', 'westwood-ca-us', 'yountville-ca-us', 'boulder', 'denver', 'hartford', 'new-haven-ct-us', 'dc', 'fort-lauderdale', 'gainesville', 'miami', 'miami-beach-fl-us', 'orlando-fl-us', 'tampa-bay', 'atlanta', 'savannah', 'honolulu', 'lahaina-hi-us', 'iowa-city', 'boise', 'chicago', 'evanston-il-us', 'naperville-il-us', 'schaumburg-il-us', 'skokie-il-us', 'bloomington-in-us', 'indianapolis-in-us', 'louisville', 'new-orleans', 'allston-ma-us', 'boston', 'brighton-ma-us', 'brookline-ma-us', 'cambridge-ma-us', 'somerville-ma-us', 'baltimore', 'ann-arbor-mi-us', 'detroit', 'minneapolis', 'saint-paul-mn-us', 'kansas-city-mo-us', 'st-louis', 'charlotte-nc-us', 'durham-nc-us', 'raleigh-nc-us', 'newark-nj-us', 'princeton-nj-us', 'albuquerque', 'santa-fe-nm-us', 'las-vegas', 'reno', 'brooklyn', 'long-island-city-ny-us', 'nyc', 'queens', 'cincinnati-oh-us', 'cleveland', 'columbus-oh-us', 'portland', 'salem-or-us', 'philadelphia', 'pittsburgh', 'providence', 'charleston', 'memphis', 'nashville', 'austin', 'dallas', 'houston', 'san_antonio', 'salt-lake-city', 'alexandria-va-us', 'arlington-va-us', 'richmond', 'burlington', 'bellevue-wa-us', 'redmond-wa-us', 'seattle', 'madison', 'milwaukee']

class YelpSpider(scrapy.Spider):
    name = "yelp"
    LIST_URL = "https://www.yelp.com/events/{}/browse?start={}&sort_by=added"
    BASE_URL = "https://www.yelp.com"
    SCRAPE_DO_PREFIX = 'http://api.scrape.do?token=scrape_do_token&url='
    existing_id_list = []
    total_count = 0
    def start_requests(self):
        # csv.register_dialect('myDialect1',
        #     quoting=csv.QUOTE_ALL,
        #     skipinitialspace=True)
        # file = open('uscities-geo.csv')
        # reader = csv.reader(file, delimiter=',')
        self.connection = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
        self.cur = self.connection.cursor()
        self.existing_id_list = []
        self.cur.execute("SELECT src_id FROM event_list WHERE source='yelp'")
        rows = self.cur.fetchall()
        for row in rows:
            self.existing_id_list.append(row[0])

        # self.existing_group_id_list = []
        # self.cur.execute("SELECT group_id FROM event_list WHERE source='dice' GROUP BY group_id")
        # rows = self.cur.fetchall()
        # for row in rows:
        #     self.existing_group_id_list.append(row[0])
        for city in CITY_LIST:
            yield scrapy.Request(
                url=self.LIST_URL.format(city, 0),
                callback=self.parse_event_list,
                meta = {
                    "city": city,
                    "start": 0
                }
            )
            # break
                # break
            # break
    
    def parse_event_list(self, response):
        soup = BeautifulSoup(response.body, features="html.parser")
        events = soup.find_all('div', class_='card')
        print('#####################')
        print(response.url)
        self.total_count = self.total_count + len(events)
        print(len(events), self.total_count) 
        for event in events:
            link = event.find('h3', class_='card_content-title')
            if link:
                link = link.find('a').attrs['href']
                print('!!!!!!!!!!!!!!!!!!!!!!!!!!')
                print(self.BASE_URL + link)
                id = link.split('/')[-1]
                if id in self.existing_id_list:
                    print('EXISTING')
                    continue
                else:
                    self.existing_id_list.append(id)
                yield scrapy.Request(
                    url=self.BASE_URL + link,
                    callback=self.parse_event_detail,
                    meta={
                        "city": response.meta['city'],
                        "url": link,
                        "parent": response.url
                    }
                )
                print('!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print('#####################')
        if len(events) > 0:
            yield scrapy.Request(
                url=self.LIST_URL.format(response.meta['city'], response.meta['start']),
                callback=self.parse_event_list,
                meta = {
                    "city": response.meta['city'],
                    "start": int(response.meta['start']) + 15
                }
            )
       
    def parse_event_detail(self, response):
        soup = BeautifulSoup(response.body, features="html.parser")
        print('#####################')
        print(response.url)
        print('Parent:', response.meta['parent'])
        event_item = EventItemV2()
        event_item['source'] = self.name
        event_item['src_id'] = response.meta['url'].split('/')[-1]
        
        event_item['src_url'] = self.BASE_URL + response.meta['url']

        event_item['title'] = soup.find('h1')
        if event_item['title']:
            event_item['title'] = event_item['title'].text.strip()

        event_item['summary'] = soup.find('p', {'itemprop': 'description'})
        if event_item['summary']:
            event_item['summary'] = event_item['summary'].text.strip()

        event_item['timezone'] = None

        event_item['image'] = soup.find('img', class_='photo-box-img')
        if event_item['image']:
            event_item['image'] = event_item['image'].attrs['src']

        start_date = soup.find('meta', {'itemprop': 'startDate'})
        if start_date:
            event_item['start_date'] = start_date.attrs['content'].split('T')[0]
            event_item['start_time'] = start_date.attrs['content'].split('T')[1]
        else:
            event_item['start_date'] = None
            event_item['start_time'] = None
        
        end_date = soup.find('meta', {'itemprop': 'endDate'})
        if end_date:
            event_item['end_date'] = end_date.attrs['content'].split('T')[0]
            event_item['end_time'] = end_date.attrs['content'].split('T')[1]
        else:
            event_item['end_date'] = None
            event_item['end_time'] = None
            
        event_item['status'] = None
        event_item['is_sold_out'] = None
        event_item['is_online_event'] = None
        event_item['is_free'] = None
        event_item['min_price'] = None
        event_item['max_price'] = None
        event_item['currency'] = None

        price_info = soup.find('span', class_='event-details_ticket-info')
        if price_info:
            price_info = price_info.text.strip()
            if price_info == 'Free':
                event_item['is_free'] = True
            else:
                price_list = price_info.replace(',', '').split('-')
                event_item['min_price'] = price_list[0].strip()
                if '¥' in event_item['min_price']:
                    event_item['min_price'] = event_item['min_price'].replace('¥', '')
                    event_item['currency'] = '¥'
                else:
                    match = re.search(r'\d+(\.\d{1,2})?', event_item['min_price'])
                    event_item['min_price'] = match.group()
                    event_item['currency'] = price_list[0].strip().replace(event_item['min_price'], '')
                if len(price_list) > 1:
                    event_item['max_price'] = price_list[1].strip()
                    event_item['max_price'] = event_item['max_price'].replace(event_item['currency'], '')
                    
        
        event_item['tags'] = soup.find('span', class_='category-str-list')
        if event_item['tags']:
            event_item['tags'] = event_item['tags'].text.strip()
        event_item['organizer'] = None
        event_item['organizer_url'] = None
        event_item['published_at'] = None

        event_item['venue_name'] = soup.find('meta', {'itemprop': 'name'})
        if event_item['venue_name']:
            event_item['venue_name'] = event_item['venue_name'].attrs['content']
        
        event_item['venue_address_2'] = soup.find('span', {'itemprop': 'telephone'})
        if event_item['venue_address_2']:
            event_item['venue_address_2'] = event_item['venue_address_2'].text.strip()
        
        event_item['venue_country'] = soup.find('meta', {'itemprop': 'addressCountry'})
        if event_item['venue_country']:
            event_item['venue_country'] = event_item['venue_country'].attrs['content']

        event_item['venue_region'] = soup.find('span', {'itemprop': 'addressRegion'})
        if event_item['venue_region']:
            event_item['venue_region'] = event_item['venue_region'].text.strip()

        event_item['venue_city'] = soup.find('span', {'itemprop': 'addressLocality'})
        if event_item['venue_city']:
            event_item['venue_city'] = event_item['venue_city'].text.strip()

        event_item['venue_address_1'] = soup.find('span', {'itemprop': 'streetAddress'})
        if event_item['venue_address_1']:
            event_item['venue_address_1'] = event_item['venue_address_1'].text.strip()

        # event_item['venue_address_2'] = None
        event_item['venue_latitude'] = None
        event_item['venue_longitude'] = None
        event_item['venue_display_address'] = None

        event_item['venue_postal_code'] = soup.find('span', {'itemprop': 'postalCode'})
        if event_item['venue_postal_code']:
            event_item['venue_postal_code'] = event_item['venue_postal_code'].text.strip()

        event_item['scraped_at'] = datetime.now().isoformat()
        event_item['id'] = '{}-{}'.format(self.name, event_item['src_id'])
        event_item['group_id'] = '{}-{}'.format(self.name, response.meta['city'])
        yield event_item

        print('#####################')