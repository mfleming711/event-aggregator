import requests
import json
import threading
import csv
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2
from soc_exp_aggregator.settings import PGSQL_HOST, PGSQL_DBNAME, PGSQL_USERNAME, PGSQL_PASSWORD, PGSQL_PORT
from soc_exp_aggregator.pipelines import upsert_data_to_supabase

class FeverupSpider:
    # algolia_index = 'Fever-pl'
    name = 'feverup'
    url = 'https://i80y2bqlsl-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.17.2)%3B%20Browser%20(lite)&x-algolia-api-key=e4226055c240f9e38e89794dcfb91766&x-algolia-application-id=I80Y2BQLSL'
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://feverup.com',
        'Referer': 'https://feverup.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded',
        'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"'
    }
    city_list = []
    event_count = 0

    def load_event_detail(self, event):
        event_item = {}
        event_item['source'] = self.name
        event_item['src_id'] = event['id']
        event_item['src_url'] = f'https://feverup.com/m/{event_item["src_id"]}'
        event_item['title'] = event['name']
        event_item['image'] = event['cover_image']
        sessions = event['sessions']
        if len(sessions) > 0:
            start_session = sessions[0]
            event_item['start_date'] = start_session['startDateStr'].split('T')[0]
            event_item['start_time'] = start_session['startDateStr'].split('T')[1]
            end_session = sessions[-1]
            event_item['end_date'] = end_session['startDateStr'].split('T')[0]
            event_item['end_time'] = end_session['startDateStr'].split('T')[1]
        event_item['min_price'] = event['numeric_price']
        event_item['currency'] = event['currency_code']
        event_item['organizer'] = event['partner']
        event_item['venue_city'] = event['city_name']
        if len(event['venues_coordinates']) > 0: 
            venue_info = event['venues_coordinates'][0]
            event_item['venue_name'] = venue_info.get('venue_name')
            event_item['venue_latitude'] = venue_info.get('lat')
            event_item['venue_longitude'] = venue_info.get('lng')
            event_item['venue_display_address'] = venue_info.get('venue_address')
        
        event_item['scraped_at'] = datetime.now().isoformat()
        event_item['id'] = '{}-{}'.format(self.name, event_item['src_id'])
        event_item['group_id'] = '{}-{}'.format(self.name, event['city_id'])
        event_item['summary'] = ''
        if event_item['src_url']:
            response = requests.get(event_item['src_url'])
            soup = BeautifulSoup(response.text, features="html.parser")
            if soup.find('div', class_='plan-description'):
                event_item['summary'] = soup.find('div', class_='plan-description').text.strip()
            else:
                event_item['summary'] = ''
        upsert_data_to_supabase(event_item)

    def send_request_for_city(self, city_index, algolia_index, offset):
        data = {
            'requests': [{
                'indexName': algolia_index,
                'query': ['', ''],
                'params': 'length=100&offset={}&filters=city_id%3D{}'.format(offset, city_index)
            }]
        }
        response = requests.post(self.url, headers=self.headers, json=data)
        results = json.loads(response.text)
        results = results['results'][0]
        nbHits = int(results['nbHits'])
        city = ''
        
        if nbHits > 0:
            print('##########',nbHits, len(results['hits']))
            if len(results['hits']) > 0:
                city = results['hits'][0]['city_name']
                self.city_list.append(city)
        self.event_count = self.event_count + nbHits
        threads = []
        for event in results['hits']:
            print(event)
            if str(event['id']) in self.total_id_list:
                print('#######################')
                print('Duplicated Event:', event['id'])
                print('#######################')
                continue
            t = threading.Thread(target=self.load_event_detail, args=(event, ))
            threads.append(t)
            if len(threads) == 1:
                for thread in threads:
                    thread.start()

                # # Wait for all threads to finish
                for thread in threads:
                    thread.join()
                threads = []
        for thread in threads:
            thread.start()

        # # Wait for all threads to finish
        for thread in threads:
            thread.join()
        threads = []
            
        print(city_index, nbHits, city, len(self.city_list), self.event_count)  # Print or process the response content as needed
        print('###################')
    
    def start_requests(self):
        self.connection = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
        self.cur = self.connection.cursor()

        self.cur.execute("SELECT * FROM cities WHERE status = True")
        rows = self.cur.fetchall()
        self.city_list = []
        for row in rows:
            print(row[6])
            other_info = row[6]['feverup']
            self.city_list.append([other_info['city_index'], row[1], '', other_info['algolia_index']])
        self.total_id_list = []
        
        threads = []
        for row in self.city_list:
            page_count = 10
            print(page_count)
            for page in range(0, page_count):
                t = threading.Thread(target=self.send_request_for_city, args=(row[0], row[3], page * 100, ))
                threads.append(t)
                print(len(threads))
                if len(threads) == 1:
                    for thread in threads:
                        thread.start()

                    # # Wait for all threads to finish
                    for thread in threads:
                        thread.join()
                    threads = []
            # break

        for thread in threads:
            thread.start()

        # # Wait for all threads to finish
        for thread in threads:
            thread.join()
        threads = []
scraper = FeverupSpider()
scraper.start_requests()