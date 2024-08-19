# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import time
import datetime
import psycopg2
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from .settings import PGSQL_HOST, PGSQL_DBNAME, PGSQL_USERNAME, PGSQL_PASSWORD, PGSQL_PORT, SUPABASE_KEY, SUPABASE_URL
from supabase import create_client, Client

print('######################')
print(SUPABASE_KEY, SUPABASE_URL)
print('######################')
url: str = SUPABASE_URL
key: str = SUPABASE_KEY
supabase: Client = create_client(url, key)

def upsert_data_to_supabase(data):
	if 'start_date' in data:
		response = (supabase
			.table('event_list')
			.select('*')
			.eq('source', data['source'])
			.eq('title', data['title'])
			.eq('venue_name', data['venue_name'])
			.eq('start_date', data['start_date'])
			.neq('id', data['id'])
			.execute())
	else:
		response = (supabase
			.table('event_list')
			.select('*')
			.eq('source', data['source'])
			.eq('title', data['title'])
			.eq('venue_name', data['venue_name'])
			.neq('id', data['id'])
			.execute())
	print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
	print(len(response.data))
	print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
	if len(response.data) > 0:
		print('Data already exists')
		return
	try:
		response = supabase.table('event_list').upsert(data, on_conflict=['id']).execute()
	except Exception as e:
		print('********************')
		print('Error:', e)
		print('********************')
	return response

class SocExpAggregatorPipeline(object):
	def __init__(self):
		self.files = {}
		self.exporter = {}
	@classmethod
	def from_crawler(cls, crawler):

		pipeline = cls()
		crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
		crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
		return pipeline

	def spider_opened(self, spider):
		# file = open('events.csv', 'w+b')
		# self.files[spider] = file
		# self.exporter = CsvItemExporter(file)
		# self.exporter.fields_to_export = ['source', 'src_id', 'title', 'datetime','location','exact_location','paid_status', 'category', 'organizer', 'followers', 'image', 'src_url']
		# self.exporter.fields_to_export = ["source", "src_id", "title", "summary", "timezone", "start_date", "end_date", "start_time", "end_time", "status", "is_free", "is_sold_out", "is_online_event", "max_price", "min_price", "currency", "tags", "organizer", "organizer_url", "src_url",  "image",  "published_at", "venue_name", "venue_country", "venue_region", "venue_city", "venue_postal_code", "venue_address_1", "venue_address_2", "venue_latitude", "venue_longitude", "venue_display_address", "scraped_at"]
		# self.exporter.start_exporting()        
		self.connection = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USERNAME, password=PGSQL_PASSWORD, dbname=PGSQL_DBNAME, port=PGSQL_PORT)
		self.cur = self.connection.cursor()

	def spider_closed(self, spider):
		print('Closed')
	# 	# self.exporter.finish_exporting()
	# 	# file = self.files.pop(spider)
	# 	# file.close()

	def process_item(self, item, spider):
		# self.exporter.export_item(item)
		print('#############################')
		lineup = None
		if 'lineup' in item:
			lineup = item['lineup']
		venue_phone = None
		if 'venue_phone' in item:
			venue_phone = item['venue_phone']
		item['lineup'] = lineup
		upsert_data_to_supabase(dict(item))
		return item