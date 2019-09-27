import datetime
import scrapy

import datetime
import pytz

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor
import re
from scrapy import Request

from oddsscraper.items import OddsCheckerEvent

TZ = pytz.timezone('Europe/London')

class HorseSpider(CrawlSpider):
    name = "horse"
    allowed_domains = ["oddschecker.com"]

    # tracks
    tracks = [
        'aintree',
        'bangor',
        'downpatrick',
        'meydan', 
        'market-rasen',
        'ascot',
        'sedgefield',
        'roscommon',
        'pontefract',
        'punchestown',
        'brighton',
        'down-royal',
        'sandown',
        'listowel',
        'naas',
        'bellewstown',
        'yarmouth',
        'chelmsford-city',
        'musselburgh',
        'leopardstown',
        'wetherby',
        'goodwood',
        'curragh',
        'stratford', 
        'Killarney',
        'ffos-las',
        'newcastle',
        'ludlow',
        'york',
        'bath',
        'worcester',
        'perth',
        'kilbeggan',
        'Huntingdon',
        'hamilton',
        'leicester',
        'kempton',
        'fontwell',
        'beverley',
        'tipperary',
        'warwick',
        'wexford',
        'hexham',
        'newbury',
        'lingfield',
        'gowran-park',
        'wolverhampton',
        'haydock',
        'wincanton',
        'fairyhouse',
        'carlisle',
        'doncaster',
        'thurles',
        'exeter',
        'navan',
        'sligo',
        'chepstow',
        'plumpton',
        'ayr',
        'cartmel',
        'catterick',
        'epsom',
        'dundalk',
        'limerick',
        'kelso',
        'cheltenham',
        'clonmel',
        'redcar',
        'windsor',
        'ballinrobe',
        'galway',
        'tramore',
        'southwell',
        'newmarket',
        'uttoxeter',
        'cork',
        'newton-abbot',
        'chester',
        'thirsk',
        'nottingham',
        'ripon',

    ]
    ante_post = [
        'cheltenham-festival']
    ante_post = []
    tracks = ['ludlow']
    # tracks = ['ascot']
    dates = [str(datetime.date.today()), str(
        datetime.date.today() + datetime.timedelta(days=1))]

    start_urls = ['https://m.oddschecker.com/m/horse-racing/{0}-{1}'.format(
        dt, track) for dt in dates for track in tracks]

    #start_urls = start_urls + ['https://m.oddschecker.com/m/{0}'.format(track) for track in ante_post]
    rules = (
        #Rule(LinkExtractor(restrict_xpaths=('//a[@class="header button-row"]'),
        Rule(LinkExtractor(restrict_xpaths=('//a[@class="race-link "]'),
                           # deny='.*horse-racing/201.*'
                           ),
             callback='parse_race'),
        Rule(LinkExtractor(restrict_xpaths=('//a[@class="button-row"]'),
                           # deny='.*horse-racing/201.*'
                           ),
             callback='parse_ante_post'),
        # list-view-link cell vert
        #Rule(LinkExtractor(restrict_xpaths=('//a[@class="list-view-link cell vert"]'),
        #                    ),
        #      callback='parse_race'),
    )

    def make_requests_from_url(self, url):
        return Request(url, dont_filter=True, cookies={'odds_type': 'decimal'})

    def parse_ante_post(self, response):
        result = OddsCheckerEvent()

        # Scrape specifics
        result['event_url'] = response.url
        result['event_scrape_time'] = datetime.datetime.now()
        a = re.search(
            '(?P<course>.*)-.*', response.url.split('/')[-3])

        result['event_venue'] = a.group('course').replace('-', ' ').lower()

        result['event_name'] = response.url.split('/')[-2].replace('-', ' ').lower()
        # result['event_time'] = datetime.datetime.combine(
        #    dttm, datetime.datetime.strptime(response.url.split('/')[-2], '%H:%M').time())

        result['event_runners'] = [item.extract().strip().lower()
                                   for item in response.xpath('//span[@class="left name"]/text()')]

        detailed_price_breakdown = list()
        for horse in response.xpath('//tr[@class="odds-dropdown"]/td/select'):

            bookmaker_options = list()
            breakdown = [bookmaker.extract().strip()
                         for bookmaker in horse.xpath('./option/text()')]
            for bookie in breakdown:
                parse_result = re.search(
                    '(?P<price>[0-9.]+) (?P<bookmaker>[A-Za-z0-9 ]*)( EW: )?'
                    '(?P<place_count>\d)?( at 1/)?(?P<divisor>[0-9]+)?$',
                    bookie)

#                parse_result = re.search(
#                    '([0-9.]+) ([A-Za-z0-9 ]*)(?: EW: ([0-9]?) at 1/([0-9]*))', bookie)


                logging.info(bookie)
                if (parse_result and parse_result.group('bookmaker') != 'Coral'):
#                    price, bookmaker, place_count, divisor = parse_result.groups()

                    result['price'] = float(parse_result.group('price'))
                    # result['bookmaker'] = bookmaker
                    result['bookmaker'] = parse_result.group('bookmaker')

                    try:
                        result['place_count'] = int(
                            parse_result.group('place_count'))
                    except TypeError:
                        result['place_count'] = -1

                    try:
                        result['divisor'] = int(parse_result.group('divisor'))
                    except TypeError:
                        result['divisor'] = -1


                    if result['bookmaker'] not in  ['Stan James', 'Coral']:
                        logging.info('addings bookmaker {0}'.format(bookmaker))
                        print result['bookmaker']
                        bookmaker_options.append(
                            [float(price), bookmaker, int(place_count), int(divisor)])
                    else:
                        logging.info('Ignore excluded bookmaker. {0}'.format(result['bookmaker']))

            detailed_price_breakdown.append(bookmaker_options)

        result['event_price_breakdown'] = detailed_price_breakdown

        # result['event_prices'] = [float(item.extract().strip().split(' ')[0]) for item in response.xpath('//tr[@class="odds-dropdown"]/td/select/option[1]/text()')]
        # result['event_bookies'] = [item.extract().strip().split(' ')[1] for item in response.xpath('//tr[@class="odds-dropdown"]/td/select/option[1]/text()')]

        yield result

    def parse_race(self, response):
        result = OddsCheckerEvent()

        # Scrape specifics
        result['event_url'] = response.url
        result['event_scrape_time'] = datetime.datetime.now()

        a = re.search(
            '((?P<date>\d{4}(-\d\d){2})-)?(?P<course>.*)', response.url.split('/')[-3])

        result['event_venue'] = a.group('course').replace('-', ' ').lower()

        if a.group('date'):
            dttm = datetime.datetime.strptime(a.group('date'), "%Y-%m-%d")
        else:
            dttm = datetime.date.today()
        # result['event_venue'] = response.url.split(
        #    '/')[-3].replace('-', ' ').lower()
        try:
            local_time = datetime.datetime.combine(
                dttm, datetime.datetime.strptime(response.url.split('/')[-2], '%H:%M').time())
            result['event_time'] = TZ.localize(local_time).astimezone(pytz.utc)
        except ValueError:
            result['event_time'] = datetime.datetime.strptime('Jul 15 2016 6:35AM', '%b %d %Y %I:%M%p')

        result['event_runners'] = [item.extract().strip().lower()
                                   for item in response.xpath('//span[@class="left name"]/text()')]

        detailed_price_breakdown = list()
        for horse in response.xpath('//tr[@class="odds-dropdown"]/td/select'):

            bookmaker_options = list()
            breakdown = [bookmaker.extract().strip()
                         for bookmaker in horse.xpath('./option/text()')]
            for bookie in breakdown:
                parse_result = re.search(
                    '([0-9.]+) ([A-Za-z0-9 ]*)(?: EW: ([0-9]?) at 1/([0-9]*))', bookie)
                
                parse_result = re.search(
                    '(?P<price>[0-9.]+) (?P<bookmaker>[A-Za-z0-9 ]*)( EW: )?'
                    '(?P<place_count>\d)?( at 1/)?(?P<divisor>[0-9]+)?$',
                    bookie)

                if (parse_result):
                    # price, bookmaker, place_count, divisor = parse_result.groups()

                    price = float(parse_result.group('price'))
                    # result['bookmaker'] = bookmaker
                    bookmaker = parse_result.group('bookmaker')

                    if parse_result.group('place_count'):
                    
                        place_count = int(
                            parse_result.group('place_count'))

                        divisor = int(parse_result.group('divisor'))

                    else:
                        place_count = -1
                        divisor = -1

                    if bookmaker not in ['Stan James', 'Betfair', 'Coral']:
                        bookmaker_options.append(
                            [float(price), bookmaker, int(place_count), int(divisor)])
                
            detailed_price_breakdown.append(bookmaker_options)

        result['event_price_breakdown'] = detailed_price_breakdown

        # result['event_prices'] = [float(item.extract().strip().split(' ')[0]) for item in response.xpath('//tr[@class="odds-dropdown"]/td/select/option[1]/text()')]
        # result['event_bookies'] = [item.extract().strip().split(' ')[1] for item in response.xpath('//tr[@class="odds-dropdown"]/td/select/option[1]/text()')]

        yield result
