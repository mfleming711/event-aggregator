from pathlib import Path
from datetime import datetime
import time
import csv
import json
import scrapy
import psycopg2
from bs4 import BeautifulSoup
from ..items import EventItem, EventItemV2
from ..settings import PGSQL_HOST, PGSQL_DBNAME, PGSQL_USERNAME, PGSQL_PASSWORD, PGSQL_PORT, SCRAPER_API_KEY, SCRAPE_DO_API_KEY
import urllib.parse


class EventbriteSpider(scrapy.Spider):
    name = "eventbrite"
    MAX_PAGE_PER = 1
    MAX_PAGE_NO = 50
    EVENT_LIST_PAGE_URL = 'https://www.eventbrite.com/d/{}--{}/all-events/?page={}'
    SCRAPER_API_PREFIX = f'http://api.scraperapi.com?keep_headers=true&api_key={SCRAPER_API_KEY}&url='
    SCRAPE_DO_PREFIX = ''
    EVENT_LIST_API_URL = 'https://www.eventbrite.com/api/v3/destination/events/?event_ids={}&page_size={}&expand=event_sales_status,image,primary_venue,saves,ticket_availability,primary_organizer,public_collections'
    headers = {
        'Cookie': 'AN=; AS=847d7338-b6ed-47b6-aaff-38b6c22e439e; G=v%3D2%26i%3D8d6ed876-da30-4508-9793-4a50f78f0278%26a%3D11eb%26s%3D5259e38a7831b5f7e7845352f2f1e1c7f74a8415; SP=AGQgbbk1P9Ziaf1s5V_vRkbX9SGGzA5WkHqFc5JNVw4tZwnT52545kUBNfiblqXQWPJ_UagHodyUAROdDaNMpyoJLBzgDktdhNUmpAF9iKbgF8ssqZIDSGnV_Wg6xmmCEzp7Qnz2CbIZzG0eu2Ytz1jhNDpM6DfSl7bPo_EFEiDVY_B5wHiqW94zDMuoRtA7ph7EyC8gxaSKvr6IvZQwgpT3GND2MKuscyRd-LscRCFo37IOC5T5KPw; SS=AE3DLHQ_e6AHwIhnXbsCPM6-YKiEnPTCBA; csrftoken=fb63124e889f11ee8f0f97beebe5a557; django_timezone=UTC; ebEventToTrack=; eblang=lo%3Den_US%26la%3Den-us; mgref=typeins; mgrefby='
    }
    def start_requests(self):
        self.id_stack = {}
        self.page_no = {}
        self.connection = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
        self.cur = self.connection.cursor()
        
        self.cur.execute("SELECT * FROM cities WHERE status = True")
        rows = self.cur.fetchall()
        self.city_list = []
        for row in rows:
            self.city_list.append([row[1], row[2]])
        
        self.total_id_list = []
        count = 0
        for row in self.city_list:
            count = count + 1
            state = row[1].lower()
            city = row[0].replace(' ', '-').replace('.', '').replace("'", '').lower()
            group_id = '{}-{}-{}'.format(self.name, state, city)
            
            self.id_stack[group_id] = []
            for page in range(1, 51):
                self.page_no[group_id] = page
                url = self.EVENT_LIST_PAGE_URL.format(state.lower(), city.lower().replace(' ', '-'), page)
                yield scrapy.Request(
                    url= url, 
                    callback=self.parse_event_list_page_v2, 
                    errback=self.error_callback,
                    headers = self.headers,
                    meta={
                        "proxy": f'http://scraperapi:{SCRAPER_API_KEY}@proxy-server.scraperapi.com:8001',
                        'group_id': group_id,
                        'url': url,
                    }
                )
            #     break
            # break
        print("END OF SCRAPER")

    def error_callback(self, failure):
        # Error callback function to handle HTTP errors
        if failure.check(scrapy.spidermiddlewares.httperror.HttpError):
            # Accessing the response object for error handling
            response = failure.value.response
            if response.status == 400:
                # Retrieving the response body for 400 error
                for key in response:
                    print(key)
                error_body = response
                self.logger.error(f"Received 400 error. Response body: {error_body}")
        else:
            # Other non-HTTP errors
            self.logger.error(f"Non-HTTP error occurred: {repr(failure)}")
    def parse_event_list_page_v2(self, response):
       
        group_id = response.meta['group_id']
        if response.meta['url'] != response.url:
            print('Redirected or Errored')
            return
        soup = BeautifulSoup(response.body, features="html.parser")
        try:
            events = soup.find('div', class_='search-results-panel-content__events').findAll('ul')[0].find_all('li')
            
        except:
            print('NO EVENTS ON THIS PAGE')
            return

        event_ids = []
        for event in events:
            card_link = event.find('a', class_='event-card-link')
            event_id = '{}-{}'.format(self.name, card_link.attrs['data-event-id'])
            if event_id not in self.total_id_list:
                event_ids.append(card_link.attrs['data-event-id'])
        self.id_stack[group_id] = self.id_stack[group_id] + event_ids
        if len(self.id_stack[group_id]) > self.MAX_PAGE_PER or self.page_no[group_id] == self.MAX_PAGE_NO:
            if len(self.id_stack[group_id]) > 0:
                encoded_url = urllib.parse.quote(self.EVENT_LIST_API_URL.format(','.join(self.id_stack[group_id]), len(self.id_stack[group_id])))
                yield scrapy.Request(
                    # url=self.SCRAPER_API_PREFIX + encoded_url, 
                    url = self.EVENT_LIST_API_URL.format(','.join(self.id_stack[group_id]), len(self.id_stack[group_id])),
                    callback=self.parse_event_list_api, 
                    headers = self.headers,
                    meta = {
                        'group_id': group_id,
                        "proxy": f'http://scraperapi:{SCRAPER_API_KEY}@proxy-server.scraperapi.com:8001',
                    }
                )
                self.id_stack[group_id] = []
        else:
            print('VERY FEW NEW EVENTS ON THIS PAGE')

    def parse_event_list_api(self, response):
        event_list = json.loads(response.body)
        for event in event_list['events']:
            event_item = EventItemV2()
            event_item['source'] = self.name
            event_item['src_id'] = event['id']
            event_item['src_url'] = event['url']
            event_item['title'] = event['name']
            event_item['summary'] = event['summary']
            event_item['timezone'] = event['timezone']
            if 'image' in event:
                event_item['image'] = event['image']['url']
            else:
                event_item['image'] = ''
            event_item['start_date'] = event['start_date']
            event_item['end_date'] = event['end_date']
            event_item['start_time'] = event['start_time']
            event_item['end_time'] = event['end_time']
            event_item['status'] = event['status']
            event_item['is_free'] = event['ticket_availability']['is_free']
            event_item['is_sold_out'] = event['ticket_availability']['is_sold_out']
            event_item['is_online_event'] = event['is_online_event']
            if event['ticket_availability']['maximum_ticket_price']:
                event_item['max_price'] = event['ticket_availability']['maximum_ticket_price']['major_value']
            else:
                event_item['max_price'] = None
            if event['ticket_availability']['minimum_ticket_price']:
                event_item['min_price'] = event['ticket_availability']['minimum_ticket_price']['major_value']
                event_item['currency'] = event['ticket_availability']['minimum_ticket_price']['currency']
            else:
                event_item['min_price'] = None
                event_item['currency'] = None
            tags = event['tags']
            tag_arr = []
            for tag in tags:
                tag_arr.append(tag['display_name'])
            event_item['tags'] = ','.join(tag_arr)
            event_item['organizer'] = event['primary_organizer']['name']
            event_item['organizer_url'] = event['primary_organizer']['url']
            event_item['published_at'] = event['published']
            if 'primary_venue' in event: 
                event_item['venue_name'] = event['primary_venue']['name'] if 'name' in event['primary_venue']['address'] else None
                event_item['venue_country'] = event['primary_venue']['address']['country'] if 'country' in event['primary_venue']['address'] else None
                event_item['venue_region'] = event['primary_venue']['address']['region'] if 'region' in event['primary_venue']['address'] else None
                event_item['venue_city'] = event['primary_venue']['address']['city'] if 'city' in event['primary_venue']['address'] else None
                event_item['venue_postal_code'] = event['primary_venue']['address']['postal_code'] if 'postal_code' in event['primary_venue']['address'] else None
                event_item['venue_address_1'] = event['primary_venue']['address']['address_1'] if 'address_1' in event['primary_venue']['address'] else None
                event_item['venue_address_2'] = event['primary_venue']['address']['address_2']  if 'address_2' in event['primary_venue']['address'] else None
                event_item['venue_latitude'] = event['primary_venue']['address']['latitude'] if 'latitude' in event['primary_venue']['address'] else None
                event_item['venue_longitude'] = event['primary_venue']['address']['longitude'] if 'longitude' in event['primary_venue']['address'] else None
                event_item['venue_display_address'] = event['primary_venue']['address']['localized_address_display']  if 'localized_address_display' in event['primary_venue']['address'] else None
            else:
                event_item['venue_name'] = None
                event_item['venue_country'] = None
                event_item['venue_region'] = None
                event_item['venue_city'] = None
                event_item['venue_postal_code'] = None
                event_item['venue_address_1'] = None
                event_item['venue_address_2'] = None
                event_item['venue_latitude'] = None
                event_item['venue_longitude'] = None
                event_item['venue_display_address'] = None
            event_item['scraped_at'] = datetime.now().isoformat()
            event_item['id'] = '{}-{}'.format(self.name, event_item['src_id'])
            event_item['group_id'] = response.meta['group_id']
            yield event_item
    
    def parse_event_list_page(self, response):
        soup = BeautifulSoup(response.body, features="html.parser")
        scripts = soup.find_all('script')
        events = []
        events = soup.find_all('li', class_='search-main-content__events-list-item')
        for event in events:
            card_link = event.find('a', class_='event-card-link')
            event_item = EventItem()
            event_item['source'] = self.name
            event_item['src_url'] = card_link.attrs['href']
            event_item['src_id'] = card_link.attrs['data-event-id'] if 'data-event-id' in card_link.attrs else None
            event_item['location'] = card_link.attrs['data-event-location'] if 'data-event-location' in card_link.attrs else None
            event_item['category'] = card_link.attrs['data-event-category'] if 'data-event-category' in card_link.attrs else None
            event_item['title'] = event.find('h2', class_='Typography_root__4bejd').text.strip()
            if event.find('img', class_='event-card-image'):
                event_item['image'] = event.find('img', class_='event-card-image').attrs['src']
            start_index = 0
            if event.find('div', class_='EventCardUrgencySignal'):
                event_item['urgency_signal'] = event.find('div', class_='EventCardUrgencySignal').text.strip()
                start_index = 1
            else:
                event_item['urgency_signal'] = None
            info_items = event.find_all('p', class_='Typography_root__4bejd')
            event_item['scraped_at'] = datetime.now().isoformat()
            yield scrapy.Request(
                url=event_item['src_url'], 
                callback=self.parse_event_detail_page,
                headers = self.headers,
                meta={
                    'event_item': event_item,
                }
            )
        
    def parse_event_detail_page(self, response):
        event_item = response.meta['event_item']
        soup = BeautifulSoup(response.body, features="html.parser")
        if soup.find('div', class_='conversion-bar__panel-info'):
            event_item['price'] = soup.find('div', class_='conversion-bar__panel-info').text.strip()
        event_item['datetime'] = soup.find('span', class_='date-info__full-datetime')
        if event_item['datetime']:
            event_item['datetime'] = event_item['datetime'].text.strip()
        event_item['description'] = soup.find('div', class_='has-user-generated-content').text.strip()
        event_item['description_html'] = str(soup.find('div', class_='has-user-generated-content'))
        event_item['exact_location'] = soup.find('p', class_='location-info__address-text')
        if event_item['exact_location']:
            event_item['exact_location'] = event_item['exact_location'].text.strip()
        event_item['exact_address'] = soup.find('div', class_='location-info__address')
        if event_item['exact_address']:
            event_item['exact_address'] = event_item['exact_address'].text.strip().replace(event_item['exact_location'], '').replace('Show map', '')
        event_item['organizer'] = soup.find('a', class_='descriptive-organizer-info__name-link')
        if event_item['organizer']:
            event_item['organizer'] = event_item['organizer'].text.strip()
        yield event_item