import requests
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import psycopg2
from soc_exp_aggregator.settings import PGSQL_HOST, PGSQL_DBNAME, PGSQL_USERNAME, PGSQL_PASSWORD, PGSQL_PORT, TM_API_KEY
from soc_exp_aggregator.pipelines import upsert_data_to_supabase
connection = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
cur = connection.cursor()
name = 'ticketmaster'
# Define the base URL for the Ticketmaster Discovery API
base_url = 'https://app.ticketmaster.com'
next_path = '/discovery/v2/events.json?city=Miami&size=200'
api_key_param = f'&apikey={TM_API_KEY}'

def generate_event_item(event):
    event_item = {}
    event_item['source'] = name
    event_item['src_id'] = event['id']
    event_item['src_url'] = event.get('url')
    event_item['title'] = event['name']
    event_item['summary'] = (event['info'] if 'info' in event else '') + '\n' + (event['pleaseNote'] if 'pleaseNote' in event else '')
    event_item['timezone'] = event['dates'].get('timezone')
    
    max_width = 0
    max_img = None
    for img in event['images']:
        if max_width < img['width']:
            max_width = img['width']
            max_img = img['url']
    event_item['image'] = max_img

    if 'start' in event['dates']:
        event_item['start_date'] = event['dates']['start'].get('localDate')
        event_item['start_time'] = event['dates']['start'].get('localTime')
    else:
        event_item['start_date'] = None
        event_item['start_time'] = None
    if 'end' in event['dates']:
        event_item['end_date'] = event['dates']['end'].get('localDate')
        event_item['end_time'] = event['dates']['end'].get('localTime')
    else:
        event_item['end_date'] = None
        event_item['end_time'] = None
    event_item['status'] = None
    event_item['is_free'] = None
    event_item['is_sold_out'] = None
    event_item['is_online_event'] = None
    event_item['max_price'] = None
    event_item['currency'] = None
    event_item['min_price'] = None
    if 'priceRanges' in event and len(event['priceRanges']) > 0:
        event_item['min_price'] = event['priceRanges'][0].get('min')
        event_item['max_price'] = event['priceRanges'][0].get('max')
        event_item['currency'] = event['priceRanges'][0].get('currency')
    
    tags = []
    if 'classifications' in event:
        for key in ['segment', 'genre', 'subGenre']:
            if 'key' in event['classifications'] and event['classifications'][key]['name'] not in tags:
                tags.append(event['classifications'][key]['name'])
    event_item['tags'] = ','.join(tags)
    event_item['organizer'] = None
    if 'promoter' in event:
        event_item['organizer'] = event['promoter']['name']
    event_item['organizer_url'] = None
    event_item['published_at'] = None
    if len(event['_embedded']['venues']) > 0:
        venue = event['_embedded']['venues'][0]
        event_item['venue_name'] = venue['name'] if 'name' in venue else None
        event_item['venue_country'] = venue['country']['name'] if 'country' in venue else None
        event_item['venue_region'] = venue['state']['name'] if 'state' in venue else None
        event_item['venue_city'] = venue['city']['name'] if 'city' in venue else None
        event_item['venue_postal_code'] = venue['postalCode'] if 'postalCode' in venue else None
        event_item['venue_address_1'] = venue['address']["line1"] if 'address' in venue else None
        event_item['venue_address_2'] = None
        event_item['venue_latitude'] = venue['location']['latitude'] if 'location' in venue else None
        event_item['venue_longitude'] = venue['location']['longitude'] if 'location' in venue else None
        event_item['venue_display_address'] = None
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
    event_item['group_id'] = '{}'.format(name)
    event_item['scraped_at'] = datetime.now().isoformat()
    event_item['id'] = '{}-{}'.format(name, event_item['src_id'])
    return event_item
# Function to fetch events
def fetch_events():
    global next_path
    cur.execute("SELECT * FROM cities WHERE status = True")
    rows = cur.fetchall()
    city_list = []
    for row in rows:
        city_list.append(row[1])
    all_events = []
    for city in city_list:
        next_path = '/discovery/v2/events.json?city={}&size=200'.format(city)
        while True:
            response = requests.get(base_url + next_path + api_key_param)
            if response.status_code == 200:
                # Get rate limit information from response headers
                rate_limit_available = response.headers.get('Rate-Limit-Available')
                print(f"Remaining API Credits: {rate_limit_available}")

                events = response.json()
                if '_embedded' in events and 'events' in events['_embedded']:
                    all_events.extend(events['_embedded']['events'])
                # Check if there is a next page
                if 'next' in events['_links']:
                    next_path = events['_links']['next']['href']
                else:
                    break
            else:
                print(f"Error: {response.status_code}")
                print(response.json())
                break
    return all_events
def export_to_google_sheets(all_events):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = 'google_account.json'

    creds = None
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of the spreadsheet.
    SPREADSHEET_ID = '1HYgeBHik0O4NJcgzcpFZWup_vJId5qFJ0f3ewQjMEYo'
    RANGE_NAME = 'Sheet1!A1'

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    new_rows = []
    # Insert the data into the Google Sheet
    for event in all_events:
        event_item = generate_event_item(event)
        new_rows.append([
            event_item['source'], 
            event_item['title'],
            event_item['summary'],
            event_item['tags'],
            event_item['image'],
            event_item['start_date'],
            event_item['start_time'],
            event_item['end_date'],
            event_item['end_time'],
            event_item['max_price'],
            event_item['min_price'],
            event_item['currency'],
            event_item['organizer'],
            event_item['src_url'],
            event_item['venue_name'],
            event_item['venue_city'],
            event_item['venue_latitude'],
            event_item['venue_longitude'],
            event_item['venue_address_1'] + ', ' + event_item['venue_city'] + ', ' + event_item['venue_region'] + ', ' + event_item['venue_postal_code']
        ])
        print(event_item['venue_name'])
    body = {
        'values': new_rows
    }
    result = sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption='RAW',
        insertDataOption='INSERT_ROWS',
        body=body
    ).execute()

def process_item(item):
    global cur, connection
    try :
        print('#############################')
        cur.execute("INSERT INTO event_list(id, source, src_id, title, summary, start_date, end_date, start_time, end_time, min_price, currency, organizer, src_url, image, venue_name, venue_city, venue_latitude, venue_longitude, venue_display_address, scraped_at, group_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
            item['id'],
            item['source'],
            item['src_id'],
            item['title'],
            item['summary'],
            item['start_date'],
            item['end_date'],
            item['start_time'],
            item['end_time'],
            item['min_price'],
            item['currency'],
            item['organizer'],
            item['src_url'],
            item['image'],
            item['venue_name'],
            item['venue_city'],
            item['venue_latitude'],
            item['venue_longitude'],
            item['venue_display_address'],
            item['scraped_at'],
            item['group_id'],
        ))
        connection.commit()
    except Exception as e:
        cur.close()
        connection.close()
        connection = psycopg2.connect(
        host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
        cur = connection.cursor()
        print(e)
    return item  
# Fetch all events
all_events = fetch_events()
for event in all_events:
    event_item = generate_event_item(event)
    upsert_data_to_supabase(event_item)
