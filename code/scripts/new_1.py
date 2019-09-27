# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import datetime
import pymongo
import re
import multiprocessing

MONGODB_SERVER = "localhost"
MONGODB_PORT = 27017
MONGODB_DB = "horse-racing"
MONGODB_COLLECTION = "odds-scraper"

EXTRA_PLACES = {'Paddy Power:fairyhouse:16:00':4,
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
    result['event_venue'] = venue.replace('-', ' ').lower()
    #result['event_time'] = time
    dttm = datetime.datetime.strptime(date, "%Y-%m-%d")

    try:
        result['event_time'] = datetime.datetime.combine(
            dttm, datetime.datetime.strptime(resp.url.split('/')[-2],
                                             '%H:%M').time())  #+ datetime.timedelta(hours=-1)
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
        try:
            div = int(a['data-ew-div'][-1:])
        except:
            div = -1
        try:
            place = int(a['data-ew-places'])
        except:
            place = -1 
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

def races(url):
    response = requests.get(
    'https://m.oddschecker.com/m/{}'.format(url))
    races = BeautifulSoup(response.content, 'html.parser') 
    listofraces = []
    for race in races.find_all('a', class_="race-link"):
        listofraces.append(race['href'])
    return listofraces


def returnraces():
    print 'getting races'
    resp = requests.get(
        'https://m.oddschecker.com/m/horse-racing/')
    print 'got response'
    soup = BeautifulSoup(resp.content, 'html.parser')
    listofraces = []

    for meeting in soup.find_all('a', href=re.compile(r'^/horse-racing/(?!.*-coupon$)[^/]+$')):
        listofraces.append(races(meeting['href']))
        #yield races(meeting['href'])
    print listofraces
    return listofraces

def do_crawl(meeting):
    print meeting
    for meet in meeting:
      time = meet.split('/')[-2]
      a = re.search(
          '((?P<date>\d{4}(-\d\d){2})-)?(?P<course>.*)',
        meet.split('/')[-3])
      if a.group('date'):
        date = a.group('date')
      else:
        date = datetime.date.today().strftime('%Y-%m-%d')    

    venue = a.group('course')
    print("Querying: {0} - {1} - {2}".format(venue, date, time))    
    result = query_oddschecker(venue, date, time)
    collection.insert_one(result)


if __name__ == '__main__':
    resp = requests.get(
        'https://m.oddschecker.com/m/horse-racing/')
    soup = BeautifulSoup(resp.content, 'html.parser')
    connection = pymongo.MongoClient(
        MONGODB_SERVER,
        MONGODB_PORT
    )

    db = connection[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]
    gen = returnraces()
    # gen = ['/horse-racing/2017-01-07-wolverhampton/19:15/winner']
    pool = multiprocessing.Pool(processes=50)
    pool.map(do_crawl, gen)


