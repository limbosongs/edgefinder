# -*- coding: utf-8 -*-

# Scrapy settings for oddsscraper project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'oddsscraper'

SPIDER_MODULES = ['oddsscraper.spiders']
NEWSPIDER_MODULE = 'oddsscraper.spiders'

ITEM_PIPELINES = ['oddsscraper.pipelines.MongoDBPipeline', ]

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'oddsscraper (+http://www.yourdomain.com)'

MONGODB_SERVER = "localhost"
MONGODB_PORT = 27017
MONGODB_DB = "horse-racing"
MONGODB_COLLECTION = "odds-scraper"