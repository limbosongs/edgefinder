import dateutil.tz
import datetime
from time import gmtime, strftime
import collections
import dateutil.parser
import pymongo
import requests
import settings
import pandas as pd
import numpy as np
import boto3
from itertools import product

BookmakerPrice = collections.namedtuple('BookmakerPrice', ['name', 'price', 'place_count', 'divisor'])
BetfairMarketCatalogue = collections.namedtuple('BetfairMarketCatalogue', [
                                                'race_keys', 'market_to_id', 'id_to_market', 'horse_to_id', 'id_to_horse'])
BetDataPoint = collections.namedtuple('BetDataPoint', [
                                      'horse', 'bookmaker', 'bookmaker_price', 'fair_price', 'probability', 'expectancy', 'betfair_runner_count', 'betfair_matched_amount', 'bookmaker_place_count', 'bookmaker_divisor'])
BetSuggestion = collections.namedtuple(
    'BetSuggestion', ['event', 'time', 'type', 'stake', 'bet_data', 'bookmaker_win_price', 'bookmaker_place_count', 'betfair_runner_count', 'betfair_matched_amount', 'other_bet_data'])


class KeyUtils(object):

    @staticmethod
    def generate_race_key(event_time, venue):
        return '%s/%s' % (venue.lower(), event_time.strftime('%Y-%m-%dT%H:%M'))

    @classmethod
    def generate_market_key(cls, event_time, venue, market_code):
        return '%s/%s' % (cls.generate_race_key(event_time, venue), market_code)


class OddsCheckerInterface(object):

    def __init__(self, db_collection):
        self.db_collection = db_collection

    def retrieve_odds(self):

        bookmaker_odds = collections.defaultdict(
            lambda: collections.defaultdict(list))
        for doc in self.db_collection.find({"event_scrape_time": {'$gte': datetime.datetime.utcnow() + datetime.timedelta(hours=-3)}}):
            venue = doc['event_venue']
            start_time = doc.get('event_time', datetime.datetime.utcnow())
            try:
                default_divisor = max([divisor for _, _, _, divisor, _ in doc['event_price_breakdown']])
                default_place_count = min([divisor for _, _, place_count, _, _ in doc['event_price_breakdown']])
            except (IndexError, ValueError):
                pass

            # for horse, price in zip(doc['event_runners'], doc['event_price_breakdown']):
  #          for price in doc['event_price_breakdown']:
  #              print price
            for win_price, bookmaker_name, place_count, divisor, horse in doc['event_price_breakdown']:
                if place_count == -1:
                    continue 
                place_count = default_place_count if place_count == -1 else place_count
                divisor = default_divisor if divisor == -1 else divisor

                win_race_key = KeyUtils.generate_market_key(
                    start_time, venue, 'WIN/1')
                place_race_key = KeyUtils.generate_market_key(
                    start_time, venue, 'PLACE/%s' % place_count)

                place_price = ((win_price - 1) / divisor) + 1
                bookmaker_odds[win_race_key][horse].append(
                    BookmakerPrice(bookmaker_name, win_price, place_count, divisor))
                bookmaker_odds[place_race_key][horse].append(
                    BookmakerPrice(bookmaker_name, place_price, place_count, divisor))
        return bookmaker_odds


class BetfairInterface(object):
    API_URL = 'https://api.betfair.com/exchange/betting/json-rpc/v1'
    BETFAIR_FORMAT_DATETIME = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, app_key, liquidity_threshold=0.0):

        self.app_key = app_key
        self.liquidity_threshold = liquidity_threshold

    def execute_request(self, request, session_token=None):
        headers = {'X-Application': self.app_key, 'X-Authentication':
                   session_token, 'content-type': 'application/json'}
        resp = requests.get(self.API_URL, data=request, headers=headers)
        return resp

    def login(self, username=None, password=None, crt_path=None, key_path=None):

        payload = 'username=%s&password=%s' % (username, password)

        headers = {'X-Application': self.app_key,
                   'Content-Type': 'application/x-www-form-urlencoded'}

        resp = requests.post('https://identitysso.betfair.com/api/certlogin',
                             verify=False, data=payload, cert=(crt_path, key_path), headers=headers)

        if resp.status_code == 200:
            session_token = resp.json()['sessionToken']
        else:
            raise Exception('Unable to login to the betfair api.')
        return session_token

    def retrieve_order_book(self, market_code, session_token=None, country_code="GB", start=0, end=0):
        catalogue = self.retrieve_catalogue(market_code, session_token, country_code, start, end)
        return self._retrieve_order_book(catalogue, session_token) if catalogue else {}

    def _retrieve_order_book(self, catalogue, session_token=None):
        market_list = catalogue.id_to_market.keys()
        markets = ','.join(map(lambda x: '"%s"' % x, market_list))
        request = '[{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketBook", "params": {"currencyCode":"EUR","marketIds":[%(market_list)s],"priceProjection":{"priceData":["EX_BEST_OFFERS"]}}, "id": 1}]'
        request = request % {'market_list': markets}
        order_book = self.execute_request(
            request, session_token).json()[0]
        order_book = order_book['result']
        result = collections.defaultdict(dict)
        for book in order_book:
            if bool(book['inplay']):
                continue
            # Liquidity check to prevent small markets
            Liquidity = book['totalMatched']
            should_ignore = self.liquidity_threshold and 'WIN' in market_list and liquidity < self.liquidity_threshold
            if should_ignore:
                continue

            market_name = catalogue.id_to_market[book['marketId']]
            winner_count = book['numberOfWinners']
            runner_count = book['numberOfActiveRunners']
            full_market_name = '/'.join((market_name, str(winner_count)))
            for runner in book['runners']:
                horse_name = catalogue.id_to_horse[runner['selectionId']]
                prices = runner['ex']['availableToLay']
                if (prices):
                    top_of_book = prices[0]['price']
                    result[full_market_name][horse_name] = top_of_book
            result[full_market_name]['_meta'] = {}
            result[full_market_name]['_meta']['_runner_count'] = runner_count
            result[full_market_name]['_meta']['_total_matched'] = book['totalMatched']
            # result[full_market_name]['total_matched'] = 
        return result

    def retrieve_catalogue(self, market_code='WIN', session_token=None, country_code="GB", start=0, end=1):
        # market_types = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketCatalogue", "params": {"filter":{"eventTypeIds":["3"] }}, "id": 1}'
        # print self.execute_request(market_types, session_token=session_token).json()
        request = '[{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketCatalogue", "params": {"filter":{"eventTypeIds":["7"],"marketCountries":["%(country_code)s"],"marketTypeCodes":["%(market_code)s"],"marketStartTime":{"from":"%(start_time)s","to":"%(end_time)s"}},"maxResults":"999","marketProjection":["EVENT","RUNNER_DESCRIPTION","MARKET_DESCRIPTION"]}, "id": 1}]'
        #request = '[{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketCatalogue", "params": {"filter":{"eventTypeIds":["3"]}, "maxResults":"999", "marketProjection":["EVENT","RUNNER_DESCRIPTION","MARKET_DESCRIPTION"]}, "id": 1}]'
        request = request % {'country_code': country_code, 'market_code': market_code, 'start_time': (datetime.datetime.utcnow() + datetime.timedelta(days=start) ).strftime(
            self.BETFAIR_FORMAT_DATETIME), 'end_time': (datetime.datetime.utcnow() + datetime.timedelta(days=end)).strftime(self.BETFAIR_FORMAT_DATETIME), }
        
        
        catalogue = self.execute_request(
            request, session_token=session_token).json()[0]['result']
        races = set()
        market_to_market_id = {}
        market_id_to_market = {}
        horse_to_selection_id = {}
        selection_id_to_horse = {}
        for market in catalogue:
            event_time = dateutil.parser.parse(
                market['description']['marketTime'])
            race_key = KeyUtils.generate_race_key(
                event_time, market['event']['venue'])
            market_key = '%s/%s' % (race_key, market_code if market_code == 'WIN' else 'PLACE')
            market_to_market_id[market_key] = market['marketId']
            market_id_to_market[market['marketId']] = market_key
            races.add(race_key)
            horse_to_selection_id[race_key] = {}

            for runner in market['runners']:
                horse_name = runner['runnerName'].strip().lower()
                selectionId = runner['selectionId']
                horse_to_selection_id[race_key][horse_name] = selectionId
                selection_id_to_horse[selectionId] = horse_name
        return BetfairMarketCatalogue(race_keys=race_key, market_to_id=market_to_market_id, id_to_market=market_id_to_market, horse_to_id=horse_to_selection_id, id_to_horse=selection_id_to_horse) if catalogue else None


class MarketComparator(object):

    def __init__(self, fair_value=None, bookmaker_market=None, exclusion_filters=set()):
        self.fair_value = fair_value
        self.bookmaker_market = bookmaker_market
        self.exclusion_filters = exclusion_filters

    def compare(self):
        result = dict()

        all_horses = set(self.fair_value.keys()).intersection(
            self.bookmaker_market.keys())
        for horse in all_horses:
            fair_price = self.fair_value[horse]

            all_bookmakers = self.bookmaker_market[horse]
            all_bookmakers = filter(
                lambda x: x.name not in self.exclusion_filters, all_bookmakers)

            if all_bookmakers:
                best = all_bookmakers[0]
                best_bookmaker_name, best_bookmaker_price = best.name, best.price
                probability = 1. / fair_price
                expectancy = probability * \
                    (best_bookmaker_price - 1) - (1 - probability)

                bookies = '/'.join(set([bookmaker.name for bookmaker in all_bookmakers if bookmaker.price == best.price]))

                point = BetDataPoint(horse=horse, bookmaker=bookies, bookmaker_price=best_bookmaker_price,
                                     fair_price=fair_price, probability=probability, expectancy=expectancy, betfair_runner_count=self.fair_value['_meta']['_runner_count'], betfair_matched_amount=self.fair_value['_meta']['_total_matched'], bookmaker_place_count=best.place_count, bookmaker_divisor=best.divisor)



                result[horse] = point
        # result['_meta'] = self.fair_value['_meta']
        return result


class BetSuggestor(object):
    PUSSY_FACTOR = .5
    BANKROLL = 100.0

    def __init__(self, expectancy_threshold=-.01, market_odds={}, bookmaker_odds={}):

        self.market_odds = market_odds
        self.bookmaker_odds = bookmaker_odds
        self.expectancy_threshold = expectancy_threshold

    def _divide_markets(self):
        print 'Bookmaker'
        print sorted(self.bookmaker_odds.keys())
        print 'Betfair'
        print sorted(self.market_odds.keys())
        win_markets, place_markets = {}, {}
        common_markets = set(self.bookmaker_odds.keys()).intersection(
            self.market_odds.keys())
        for interesting_market in common_markets:
            venue, time, market_type, _ = interesting_market.split('/')
            to_populate = win_markets if market_type == 'WIN' else place_markets

            fair_value = betfair_order_book[interesting_market]
            bookmaker_market = odds[interesting_market]
            comparator = MarketComparator(fair_value, bookmaker_market)
            comparison = comparator.compare()
            if comparison:
                # TODO revisit this... maybe we want to keep the place count
                to_populate['%s/%s' % (venue, time)] = comparison
        return win_markets, place_markets

    def suggest(self):

        win_markets, place_markets = self._divide_markets()
        suggestions = list()
        unique_events = set(win_markets.keys()).intersection(
            place_markets.keys())
        for event in unique_events:
            all_horses = set(win_markets[event].keys()).intersection(
                place_markets[event].keys())
            for horse in all_horses:

                win = win_markets[event][horse]
                place = place_markets[event][horse]
                each_way_expectancy = (place.expectancy + win.expectancy) / 2.0

                win_stake = (self.PUSSY_FACTOR * self.BANKROLL * (win.probability *
                                                                  win.bookmaker_price - 1.0)) / (win.bookmaker_price - 1.0)
                place_stake = (self.PUSSY_FACTOR * self.BANKROLL * (
                    place.probability * place.bookmaker_price - 1.0)) / (place.bookmaker_price - 1.0)
                each_way_stake = (win_stake + place_stake) / 2.0

                if win.expectancy >= self.expectancy_threshold:
                    if win.expectancy >= place.expectancy:
                        suggestions.append(BetSuggestion(
                            event=event, type='WIN', stake=win_stake, time='', bet_data=win, bookmaker_win_price=win.bookmaker_price, bookmaker_place_count=0, betfair_runner_count=0, betfair_matched_amount=0, other_bet_data=place))
                    else:
                        suggestions.append(BetSuggestion(
                            event=event, type='EACH_WAY', stake=each_way_stake, time='', bet_data=place, bookmaker_win_price=win.bookmaker_price, bookmaker_place_count=0, betfair_runner_count=0, betfair_matched_amount=0, other_bet_data=win))

                elif place.expectancy >= self.expectancy_threshold:
                    if each_way_expectancy >= self.expectancy_threshold:
                        suggestions.append(BetSuggestion(
                            event=event, type='EACH_WAY', stake=each_way_stake, time='', bet_data=place, bookmaker_win_price=win.bookmaker_price, bookmaker_place_count=0, betfair_runner_count=0, betfair_matched_amount=0, other_bet_data=win))

        return suggestions


if __name__ == '__main__':

    client = boto3.client('s3')

    odds_checker = OddsCheckerInterface(
        pymongo.MongoClient('localhost', 27017)['horse-racing']['odds-scraper'])
    odds = odds_checker.retrieve_odds()

    api = BetfairInterface(
        app_key=settings.BETFAIR_APP_KEY, liquidity_threshold=10000)

    token = api.login(settings.BETFAIR_USERNAME,
                      settings.BETFAIR_PASSWORD,
                      settings.BETFAIR_CRT_LOC,
                      settings.BETFAIR_KEY_LOC)

    betfair_order_book = {}
    for country, market in product(['GB', 'IE', 'AE'], ['WIN', 'PLACE', 'OTHER_PLACE']):
        for time in [-0.1, 0, 0.1,  0.2,  0.3,  0.4,  0.5,  0.6,  0.7,  0.8,  0.9]:

            betfair_order_book.update(api.retrieve_order_book(market, token, country, start=time, end=time+0.1))
    
    bet_suggestor = BetSuggestor(
        market_odds=betfair_order_book, bookmaker_odds=odds)
    suggestions = bet_suggestor.suggest()
    
    suggestions.sort(
        lambda x, y: x.bet_data.expectancy > y.bet_data.expectancy)

    output = ''
    out_list = [['Venue', 'Time', 'Selection', 'Type', 'Bookmaker', 'Betfair Runner Count', 'Betfair Matched Amount', 'Bookmaker Pace Count', 'Bookmaker Place Divisor','Bookmaker Win Price', 'Betfair Win Price', 'Bookmaker Place Price', 'Betfair Place Price', 'Stake']]
    for suggestion in suggestions:
        venue, time = suggestion.event.split('/')
        dttm = dateutil.parser.parse(time)
        dttm = dttm.replace(tzinfo=dateutil.tz.tzutc())
        time = dttm.astimezone(dateutil.tz.gettz('Europe/London'))
        bet_data = suggestion.bet_data
        other_bet_data = suggestion.other_bet_data

        out_list.append([venue, time,

  bet_data.horse,

  suggestion.type,

  bet_data.bookmaker,

  bet_data.betfair_runner_count,
  bet_data.betfair_matched_amount,
  bet_data.bookmaker_place_count,bet_data.bookmaker_divisor,
  suggestion.bookmaker_win_price,

  bet_data.fair_price if suggestion.type=='WIN' else other_bet_data.fair_price,
  (((suggestion.bookmaker_win_price-1)/bet_data.bookmaker_divisor)+1) ,
  other_bet_data.fair_price if suggestion.type=='WIN' else bet_data.fair_price,
  



  suggestion.stake,
])
    response = client.put_object(ACL='public-read',Body=output,Bucket='edgefest',Key='index_old.html', ContentType='text/html')
    pd.set_option('display.max_colwidth', -1)
    df = pd.DataFrame(out_list[1:], columns=out_list[0])
    df = df[df['Betfair Runner Count'] > 4]
    df = df.sort(['Stake'], ascending=False)
    if len(df):
        df = df.round({'Stake': 2, 'Bookmaker Place Price': 2 })
        html = df.to_html() + strftime("%Y-%m-%d %H:%M:%S", gmtime())
        a = df.to_html
        response = client.put_object(ACL='public-read',Body=html,Bucket='edgefest',Key='index.html', ContentType='text/html')
        df.to_csv('/tmp/output.csv')
