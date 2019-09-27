from bs4 import BeautifulSoup
import pandas as pd
import requests
import re


class OddsChecker(object):

    def __init__(self, url):
        self.url = url
        self.soup = self._get_page()
        self.bookies, self.cols = self._get_bookies()
        self.runners = None
        self.ew_terms = self._get_ew_terms()
        self.df_win = self._get_matrix()
        self.df_ew = None

    def _get_page(self):
        r = requests.get(self.url)
        return BeautifulSoup(r.text)

    def _get_ew_terms(self):
        ew_terms = {}
        ew_row = self.soup.find_all('tr', {'id': 'etfEW'})[0]
        for ew in ew_row.find_all('td'):
            if ew.has_attr('data-bk'):
                ew_terms[
                    ew['data-bk']] = {'ew_div': ew['data-ew-div'],
                                      'ew_places': ew['data-ew-places']}
            # if "ew_column_header" not in ew['class']:
            #    if re.match('\w+', ew.text):
            #        for a in ew.strings:
            #            ew_terms.append(a)
            #    else:
            #        ew_terms.append('None')
        return ew_terms

    def _get_bookies(self):
        bookie_mapping = {}
        # Extract list of bookies
        result = ['runner']
        header = self.soup.find_all('tr', {'class': 'eventTableHeader'})[0]
        for bookie in header.find_all('td'):
            if bookie.has_attr('data-bk'):
                for a in bookie.find_all('a'):
                    result.append(a['data-bk'])
                    bookie_mapping[a['title']] = a['data-bk']
        return bookie_mapping, result

    def _get_matrix(self):
        result_set = []
        cols = self.cols
        # diff-row eventTableRow bc
        for runner in self.soup.find_all('tr',
                                         {'class': 'diff-row eventTableRow bc'}):
            temp_dict = {}
            try:
                temp_dict[cols[0]] = runner['data-bname']
                for i, bookie in enumerate(
                        runner.find_all(
                            'td', {'class': re.compile('(bc|np).*')})):
                    temp_dict[cols[i + 1]] = bookie['data-odig']
            except KeyError:
                next
            result_set.append(temp_dict)
        return result_set


if __name__ == "__main__":

    url = "http://www.oddschecker.com/golf/northern-trust-open/winner"
    #url = "http://www.oddschecker.com/horse-racing/2016-01-31-punchestown/14:00/winner"

    ad = OddsChecker(url)
    win = pd.DataFrame(ad.df_win, columns=ad.cols)

    a = win.to_csv()

    print a

    print win.head(40)

    print ad.ew_terms
