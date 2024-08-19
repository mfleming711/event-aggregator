from datetime import datetime
import json
import scrapy
import psycopg2
from bs4 import BeautifulSoup
from ..items import EventItemV2
from ..settings import PGSQL_HOST, PGSQL_DBNAME, PGSQL_USERNAME, PGSQL_PASSWORD, PGSQL_PORT, SCRAPE_DO_API_KEY
from scrapy_selenium import SeleniumRequest
CITY_LIST = [ 'miami']

class DiceSpider(scrapy.Spider):
    name = "dice"
    BASE_URL = "https://dice.fm{}"
    CITY_MAIN_PAGE = "https://dice.fm/browse/{}"
    SCRAPE_DO_URL = f"http://api.scrape.do?token={SCRAPE_DO_API_KEY}&url="
    SCRAPE_DO_HEADER_URL = f"http://api.scrape.do?token={SCRAPE_DO_API_KEY}&url="
    API_URL = "https://api.dice.fm/unified_search"
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'en',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'https://dice.fm',
        'Referer': 'https://dice.fm/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'X-Api-Timestamp': '2021-10-06',
        'X-Client-Timezone': 'America/New_York',
        'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"'
    }
    total_events = []
    
    def start_requests(self):
        self.connection = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
        self.cur = self.connection.cursor()
        self.cur.execute("SELECT * FROM cities WHERE status = True")
        rows = self.cur.fetchall()
        self.city_list = []
        for row in rows:
            self.city_list.append(row[1].lower())
            
        self.existing_id_list = []
        self.existing_group_id_list = []
        for city in self.city_list:
            yield SeleniumRequest(
                url=self.SCRAPE_DO_URL + self.CITY_MAIN_PAGE.format(city), 
                callback=self.parse_city_main_page, 
                dont_filter=True,
                meta = {
                    'city': city,
                    'url': self.CITY_MAIN_PAGE.format(city),
                    'retry': 0
                }
            )
    def parse_city_main_page(self, response):
        soup = BeautifulSoup(response.body, features="html.parser")
        try:
            data = json.loads(soup.find('script', {'id': '__NEXT_DATA__'}).text.strip())
            pageProps = data["props"]["pageProps"]
        except Exception as e:
            if response.meta['retry'] < 3:
                yield SeleniumRequest(
                    url=self.SCRAPE_DO_URL + response.meta['url'], 
                    callback=self.parse_city_main_page, 
                    dont_filter=True,
                    meta={
                        "url": response.meta['url'],
                        "city": response.meta['city'],
                        "retry": response.meta['retry'] + 1
                    }
                )
            print(e)
            return
        
        for tag in pageProps['primaryFilters']:
            group_id = '{}-{}-{}'.format(self.name, response.meta['city'], tag['id'])
            if group_id not in self.existing_group_id_list:
                self.existing_group_id_list.append(group_id)
                yield SeleniumRequest(
                    url=self.SCRAPE_DO_URL + self.BASE_URL.format(tag['activateLink']), 
                    callback=self.parse_tag_page, 
                    dont_filter=True,
                    meta={
                        "tag": tag['id'],
                        "url": self.BASE_URL.format(tag['activateLink']),
                        "is_primary": True,
                        "retry": 0,
                        "city": response.meta['city']
                    }
                )
    def parse_tag_page(self, response):
        
        soup = BeautifulSoup(response.body, features="html.parser")
        try:
            data = json.loads(soup.find('script', {'id': '__NEXT_DATA__'}).text.strip())
            pageProps = data["props"]["pageProps"]
        except Exception as e:
            if response.meta['retry'] < 3:
                yield SeleniumRequest(
                    url=self.SCRAPE_DO_URL + response.meta['url'], 
                    callback=self.parse_tag_page, 
                    dont_filter=True,
                    meta={
                        "tag": response.meta['tag'],
                        "url": response.meta['url'],
                        "is_primary": response.meta['is_primary'],
                        "retry": response.meta['retry'] + 1,
                        "city": response.meta['city'],
                    }
                )
            print(e)
            return
        if response.meta['retry'] > 0:
            print("Retrying:", response.meta['retry'])
        
        event_item_list = self.export_events(pageProps['events'], response.meta['tag'], response.meta['city'])
        for event_item in event_item_list:
            yield event_item
        if pageProps['nextCursor']:
            payload = {
                "count": 24,
                "lat": pageProps['location']['lat'],
                "lng": pageProps['location']['lng'],
                "cursor": pageProps['nextCursor'],
                "tag": response.meta['tag']
            }
            

            yield scrapy.Request(
                url=self.API_URL,
                method='POST',
                headers=self.headers,
                body=json.dumps(payload),
                callback=self.parse_event_api_response,
                meta = {
                    "lat": payload['lat'],
                    "lng": payload['lng'],
                    "tag": payload['tag'],
                    "city": response.meta['city']
                }
            )
        
            
    def parse_event_api_response(self, response):
        data = json.loads(response.body)
        events = []
        for section in data['sections']:
            if 'events' in section:
                events = events + section['events']
                event_item_list = self.export_events(section['events'], response.meta['tag'], response.meta['city'])
                for event_item in event_item_list:
                    yield event_item
        if data['next_page_cursor']:
            payload = {
                "count": 24,
                "lat": response.meta['lat'],
                "lng": response.meta['lng'],
                "cursor": data['next_page_cursor'],
                "tag": response.meta['tag']
            }
            

            yield scrapy.Request(
                url=self.API_URL,
                method='POST',
                headers=self.headers,
                body=json.dumps(payload),
                callback=self.parse_event_api_response,
                meta = {
                    "count": 24,
                    "lat": response.meta['lat'],
                    "lng": response.meta['lng'],
                    "tag": response.meta['tag'],
                    "city": response.meta['city']
                }
            )
    def export_events(self, events, cur_tag, city_id):
        event_item_list = []
        for event in events:
            if event['id'] not in self.existing_id_list:
                event_item = EventItemV2()
                event_item['source'] = self.name
                event_item['src_id'] = event['id']
                event_item['src_url'] = event['social_links']['event_share']
                event_item['title'] = event['name']
                event_item['summary'] = event['about']['description']
                event_item['timezone'] = event['dates']['timezone']
                event_item['image'] = event['images']['landscape']
                event_item['lineup'] = None
                if event['summary_lineup'] and len(event['summary_lineup']['top_artists']) > 0:
                    artists = event['summary_lineup']['top_artists']
                    artists_arr = []
                    for artist in artists:
                        artists_arr.append(artist['name'])
                    event_item['lineup'] = ','.join(artists_arr)
                if event['dates']['event_start_date']:
                    event_item['start_date'] = event['dates']['event_start_date'].split('T')[0]
                    event_item['start_time'] = event['dates']['event_start_date'].split('T')[1]
                else:
                    event_item['start_date'] = None
                    event_item['start_time'] = None
                if event['dates']['event_end_date']:
                    event_item['end_date'] = event['dates']['event_end_date'].split('T')[0]
                    event_item['end_time'] = event['dates']['event_end_date'].split('T')[1]
                else:
                    event_item['end_date'] = None
                    event_item['end_time'] = None
                event_item['status'] = event['status']
                event_item['is_free'] = None
                event_item['is_sold_out'] = None
                event_item['is_online_event'] = None
                event_item['max_price'] = None
                if event['price'] and 'amount' in event['price'] and event['price']['amount']:
                    event_item['min_price'] = event['price']['amount'] / 100
                elif event['price'] and 'amount_from' in event['price'] and event['price']['amount_from']:
                    event_item['min_price'] = event['price']['amount_from'] / 100
                else:
                    event_item['min_price'] = None
                if event['price'] and 'currency' in event['price']:
                    event_item['currency'] = event['price']['currency']
                else:
                    event_item['currency'] = None
                tags = event['tags_types']
                tag_arr = []
                for tag in tags:
                    tag_arr.append(tag['title'])
                event_item['tags'] = ','.join(tag_arr)
                event_item['organizer'] = event['presented_by']
                event_item['organizer_url'] = None
                event_item['published_at'] = event['dates']['announcement_date']
                if len(event['venues']) > 0:
                    venue = event['venues'][0]
                    event_item['venue_name'] = venue['name']
                    event_item['venue_country'] = venue['city']['country_name']
                    event_item['venue_region'] = None
                    event_item['venue_city'] = venue['city']['name']
                    event_item['venue_postal_code'] = None
                    event_item['venue_address_1'] = None
                    event_item['venue_address_2'] = None
                    event_item['venue_latitude'] = venue['location']['lat']
                    event_item['venue_longitude'] = venue['location']['lng']
                    event_item['venue_display_address'] = venue['address']
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
                event_item['group_id'] = '{}-{}-{}'.format(self.name, city_id, cur_tag)
                event_item_list.append(event_item)
        return event_item_list
    