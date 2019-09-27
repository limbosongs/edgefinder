# -*- coding: utf-8 -*-

import datetime
import scrapy

import datetime

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor
import re
from scrapy import Request

from oddsscraper.items import OddsCheckerEvent


class HorseSpider(CrawlSpider):
    name = "golf"
    allowed_domains = ["oddschecker.com"]

    start_urls = ['https://m.oddschecker.com/m/golf']

    rules = (
        Rule(LinkExtractor(restrict_xpaths=('//a[@class="cell vert a-link"]'),
                           process_value=lambda x: x + '/winner'
                           ),
             callback='parse_race'),
    )
    def make_requests_from_url(self, url):
        return Request(url, dont_filter=True, cookies={'odds_type': 'decimal'})

    def parse_race(self, response):
        result = OddsCheckerEvent()

        # Scrape specifics
        result['event_url'] = response.url
        result['event_scrape_time'] = datetime.datetime.now()

        result['event_venue'] = response.url.split(
            '/')[-2].replace('-', ' ').lower()

        dttm = datetime.date.today()
        # result['event_venue'] = response.url.split(
        #    '/')[-3].replace('-', ' ').lower()
        result['event_time'] = datetime.datetime.now()

        result['event_runners'] = [item.extract().strip().lower()
                                   for item in response.xpath('//span[@class="left name"]/text()')]

        detailed_price_breakdown = list()
        for horse in response.xpath('//tr[@class="odds-dropdown"]/td/select'):

            bookmaker_options = list()
            breakdown = [bookmaker.extract().strip()
                         for bookmaker in horse.xpath('./option/text()')]
            for bookie in breakdown:
                parse_result = re.search(
                    '([0-9.]+) ([A-Za-z ]*)(?: EW: ([0-9]?) at 1/([0-9]*))', bookie)
                if (parse_result):
                    price, bookmaker, place_count, divisor = parse_result.groups()
                    bookmaker_options.append(
                        [float(price), bookmaker, int(place_count), int(divisor)])

            detailed_price_breakdown.append(bookmaker_options)

        result['event_price_breakdown'] = detailed_price_breakdown

        # result['event_prices'] = [float(item.extract().strip().split(' ')[0]) for item in response.xpath('//tr[@class="odds-dropdown"]/td/select/option[1]/text()')]
        # result['event_bookies'] = [item.extract().strip().split(' ')[1] for item in response.xpath('//tr[@class="odds-dropdown"]/td/select/option[1]/text()')]

        yield result

