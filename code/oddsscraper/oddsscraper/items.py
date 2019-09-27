# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class OddsCheckerEvent(scrapy.Item):

    event_url = scrapy.Field()
    event_scrape_time = scrapy.Field()

    event_venue = scrapy.Field()
    event_time = scrapy.Field()
    event_name = scrapy.Field()
    options = scrapy.Field()

    event_runners = scrapy.Field()
    event_price_breakdown = scrapy.Field()
