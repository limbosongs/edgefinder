# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import datetime
import pymongo
import re
import multiprocessing
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - '
                              '%(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


tracks = [
        'aintree',
        'kempton',
        'bangor',
        'haydock',
        'downpatrick',
        'huntingdon',
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


MONGODB_SERVER = "localhost"
MONGODB_PORT = 27017
MONGODB_DB = "horse-racing"
MONGODB_COLLECTION = "odds-scraper"

EXTRA_PLACES = {
                'Paddy Power:haydock:16:35':4,
                }

def _extract_mid(soup):
    try:
        return soup.find('ul', {'class': ['odds-tabs']})['data-mid']
    except TypeError:
        return None


def query_oddschecker(venue, date, time):

    result = {
        'options': None,
        'event_runners': None,
        'event_price_breakdown': None
    }

    event_runners = []
    bookmaker_options = []

    runner_to_id = {}

    dyn_url = '{date}-{venue}/{time}'.format(**locals())

    url = 'https://m.oddschecker.com/m/horse-racing/{0}/winner'.format(dyn_url)

    resp = requests.get(url)

    result['event_url'] = url
    result['event_scrape_time'] = datetime.datetime.now()
    #if venue.lower() == 'ascot':
    #    venue = 'royal ascot'
    result['event_venue'] = venue.replace('-', ' ').lower()
    dttm = datetime.datetime.strptime(date, "%Y-%m-%d")

    try:
        result['event_time'] = datetime.datetime.combine(
            dttm, datetime.datetime.strptime(resp.url.split('/')[-2],
                                             '%H:%M').time())  + datetime.timedelta(hours=-1)
    except ValueError:
        result['event_time'] = datetime.datetime.now()
    result['event_name'] = None

    soup = BeautifulSoup(resp.content, 'html.parser')

    mid = _extract_mid(soup)

    for li in soup.find_all('li', {'class': ['best-odds-row']}):
        runner_to_id[li['data-bid']] = li['data-bname'].lower().strip()
        event_runners.append(li['data-bname'].lower().strip())

    cookies = {'odds_type': 'decimal'}
    resp = requests.post(
        'https://m.oddschecker.com/m/ajax/all-odds?mid={0}&urlPath=horse-racing/{1}/winner'.format(mid, dyn_url), cookies=cookies)

    soup = BeautifulSoup(resp.content, 'html.parser')

    for a in soup.find_all('a', {'class': ['bc']}):
        print 'Processing an a {0}'.format(a)
        if a['title'] == 'Sky Bet':
            continue
        try:
            div = int(a['data-ew-div'][-1:])
        except:
            div = -1
        try:
            place = int(a['data-ew-places'])
        except:
            place = -1 
        
        print 'price: {0}, bookie: {1}, place: {2} divisor: {3}'.format(a['data-odig'], a['title'], place,div) 

        print 'runner: {0}'.format(runner_to_id[a['data-bid']])

        bookmaker_options.append([float(
            a['data-odig']), a['title'], place, div, runner_to_id[a['data-bid']]])

        extra_place_key = a['title'] +':'+venue+':'+time
        if extra_place_key in EXTRA_PLACES.keys():
            bookmaker_options.append([float(
            a['data-odig']), a['title'], EXTRA_PLACES[extra_place_key], div, runner_to_id[a['data-bid']]])
            print 'Added extra place race.{0} - {1}'.format(a['title'], runner_to_id[a['data-bid']])

    result['event_runners'] = event_runners
    result['event_price_breakdown'] = bookmaker_options
    return result

def get_races(meeting_url):
    "Return list of races for a given meeting url"
    full_url = 'https://m.oddschecker.com/m/{0}'.format(meeting_url)
    logger.info('Getting races from %s', full_url)
    response = requests.get(full_url)
    races = BeautifulSoup(response.content, 'html.parser')
    list_of_races = set()
    for race in races.find_all('a', class_="race-link"):
        list_of_races.add(race['href'])
    return list(list_of_races)


def get_meetings():
    """Get list of meetings from oddschecker"""
    resp = requests.get(
        'https://m.oddschecker.com/m/horse-racing/')
    soup = BeautifulSoup(resp.content, 'html.parser')
    list_of_meetings = set()
    regex = re.compile(r'^/horse-racing/(?!.*-coupon$)[^/]+$')
    for meeting in soup.find_all('a', href=regex):
        for course in tracks:
            if course in meeting['href']:
                list_of_meetings.add(meeting['href'])
                break
    return list(list_of_meetings)


def process_race(race):
    time = race.split('/')[-2]
    #print time
    #time = time.split(':')
    #print time
    #time = str(int(time[0])-2)+':'+time[1]
    #print time
    a = re.search('((?P<date>\d{4}(-\d\d){2})-)?(?P<course>.*)', race.split('/')[-3])
    if a.group('date'):
        date = a.group('date')
    else:
        date = datetime.date.today().strftime('%Y-%m-%d')

    venue = a.group('course')

    logger.info("Querying: %s - %s - %s", venue, date, time)

    result = query_oddschecker(venue, date, time)
    return result

if __name__ == '__main__':
    connection = pymongo.MongoClient(
        MONGODB_SERVER,
        MONGODB_PORT
    )

    db = connection[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]

    all_races = []
    meetings = get_meetings()
    pool = multiprocessing.Pool(processes=5)
    all_races = pool.map(get_races, meetings)
    print all_races 
    all_races = [race for meeting in all_races for race in meeting]

    pool.close()
    pool.join()
    pool = multiprocessing.Pool(processes=5)
    all_details = pool.map(process_race, all_races)
    collection.drop()
    for item in all_details:
        print item
        collection.insert_one(item)
    pool.close()
    pool.join()
