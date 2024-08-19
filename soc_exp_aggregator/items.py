# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

import scrapy


class EventItem(scrapy.Item):
    source = scrapy.Field()
    src_id = scrapy.Field()
    src_url = scrapy.Field()
    title = scrapy.Field()
    datetime = scrapy.Field()
    location = scrapy.Field()
    exact_location = scrapy.Field()
    exact_address = scrapy.Field()
    # paid_status = scrapy.Field()
    price = scrapy.Field()
    category = scrapy.Field()
    organizer = scrapy.Field()
    # followers = scrapy.Field()
    image = scrapy.Field()
    description = scrapy.Field()
    description_html = scrapy.Field()
    urgency_signal = scrapy.Field()
    scraped_at = scrapy.Field()
    pass

class EventItemV2(scrapy.Item):
    id = scrapy.Field()
    source = scrapy.Field()
    src_id = scrapy.Field()
    src_url = scrapy.Field()
    title = scrapy.Field()
    summary = scrapy.Field()
    timezone = scrapy.Field()
    image = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    start_time = scrapy.Field()
    end_time = scrapy.Field()
    status = scrapy.Field()
    is_free = scrapy.Field()
    is_sold_out = scrapy.Field()
    is_online_event = scrapy.Field()
    max_price = scrapy.Field()
    min_price = scrapy.Field()
    currency = scrapy.Field()
    tags = scrapy.Field()
    organizer = scrapy.Field()
    organizer_url = scrapy.Field()
    published_at = scrapy.Field()
    venue_name = scrapy.Field()
    venue_country = scrapy.Field()
    venue_region = scrapy.Field()
    venue_city = scrapy.Field()
    venue_postal_code = scrapy.Field()
    venue_address_1 = scrapy.Field()
    venue_address_2 = scrapy.Field()
    venue_latitude = scrapy.Field()
    venue_longitude = scrapy.Field()
    venue_phone = scrapy.Field()
    venue_display_address = scrapy.Field()
    lineup = scrapy.Field()
    scraped_at = scrapy.Field()
    group_id = scrapy.Field()