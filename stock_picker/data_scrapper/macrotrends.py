from bs4 import BeautifulSoup
import requests
import logging
import re
import json
from typing import Dict


class FetchingError(Exception):
    pass


class ParsingError(Exception):
    pass


class RegexError(Exception):
    pass


class MacrotrendsScrapper:
    def __init__(self):
        self.prefix_url = 'https://www.macrotrends.net'
        self.data_frequencies = {
            'annual': 'A',
            'quarter': 'Q'
        }
        self.financial_aspects = [
            'income-statement',
            'balance-sheet',
            'cash-flow-statement',
        ]
        self.stock_research_page_url = self.prefix_url + '/stocks/research'
        self.logger = logging.getLogger('MacrotrendsScrapper')

    def fetch(self, url):
        """Raise error if cannot fetch data"""
        self.logger.debug(f"Fetching {url}")
        try:
            res = requests.get(url)
        except Exception as e:
            raise FetchingError(f'Failed to fetch {url}: {e}')
        return res

    def soupify(self, content, parser):
        """Raise error if cannot soupify content"""
        self.logger.debug(f'Soupifying the following content with parser {parser}: {content[:100]}...')
        try:
            soup = BeautifulSoup(content, parser)
        except Exception as e:
            self.logger.debug(f'Failed to soupify content {content}')
            raise ParsingError(f'Failed to parse content with beautiful soup: {e}')
        return soup

    def regex_search_must_exist(self, pattern, string) -> str:
        """Raise error if no match found"""
        self.logger.debug(f'Regex search the following string with pattern {pattern}: {string[:100]}...')
        re_match = re.search(pattern, string)
        if not re_match:
            self.logger.debug(f'Failed to regex search string: {string}')
            raise RegexError(f'No result when searching the following string with pattern {pattern}: {string}')
        return string[re_match.start():re_match.end()]

    @staticmethod
    def beautify_field(field_name: str):
        return re.sub('[^A-Za-z0-9]+', '_', field_name.lower())

    def fetch_industry_listing_urls(self) -> Dict:
        """Find listing of stocks by industry urls urls by scrapping the research page
        :return: Dictionary of industries and their corresponding urls
        """
        _l = self.logger
        _l.info('Fetching content from the research page')
        res = self.fetch(self.stock_research_page_url)
        soup = self.soupify(res.content, 'html.parser')
        tables = soup.find_all('table')
        if not tables:
            raise ParsingError("Could not find any table from the research page")
        list_by_industry_table = [tbl for tbl in tables if tbl.thead.tr.th.strong.string == 'Stocks by Industry']
        if not list_by_industry_table:
            raise ParsingError("Could not find table `Stocks by Industry` in the research page")
        list_by_industry_table = list_by_industry_table[0]
        industry_cells = list_by_industry_table.find_all('td', attrs = {'style': 'text-align:left'})
        if not industry_cells:
            raise ParsingError("Could not find any industry cell in the `Stock by Industry` table")
        industry_listing_urls = {}
        for cell in industry_cells:
            try:
                industry_listing_urls[self.beautify_field(cell.a.string)] = self.prefix_url + cell.a['href']
            except Exception as e:
                _l.error(f'Failed to parse industry cell {cell}: {e}')
        _l.info(f'Found {len(industry_listing_urls)} industry listing url')
        _l.debug(f"Found following industry listing: {', '.join(industry_listing_urls.keys())}")
        return industry_listing_urls

    def fetch_stocks_url_from_industry_listing(self, industry_listing_page_url: str) -> Dict:
        """Find stocks in an industry urls by scrapping listing of stock by industry page

        :return: Dictionary of stocks ticker and their corresponding urls
        """
        _l = self.logger
        _l.info(f'Fetching content from industry listing {industry_listing_page_url}')
        res = self.fetch(industry_listing_page_url)
        soup = self.soupify(res.content, 'html.parser')
        data_script = soup.find('script', string=re.compile('var data'))
        if not data_script:
            raise ParsingError("Failed to find script containing `var data` in the page content")
        data_script_string = data_script.string
        stocks_data_table_string = self.regex_search_must_exist(r'(?<=var data = )(.*)', data_script_string)
        try:
            stocks_data_table = json.loads(stocks_data_table_string)
        except Exception as e:
            _l.debug(f'Table content: {stocks_data_table_string}')
            raise ParsingError(f'Failed to parse stocks data table: {e}')
        industry_stocks_url = {}
        for stock_datum in stocks_data_table:
            try:
                stock_ticker = stock_datum['ticker']
            except Exception as e:
                _l.error(f'Stock ticker not found {e} in {stock_datum}')
                continue
            try:
                stock_url = self.soupify(stock_datum['link'], 'html.parser').a['href'].replace('stock-price-history', '')
            except Exception as e:
                _l.error(f'Failed to parse stock link: {e} in {stock_datum}')
                continue
            _l.debug(f'Found ticker `{stock_ticker}` with url: {stock_url}')
            industry_stocks_url[stock_ticker] = stock_url
        _l.info(f'Found {len(industry_stocks_url)} stocks url in this industry')
        return industry_stocks_url

    def fetch_stock_financial_data(self, financial_data_page_url: str):
        """Fetch a stock financial data from url.
        This has to be done separately for each financial aspect under Financials tab in the website

        :param financial_data_page_url:
        :return:
        """
        _l = self.logger
        _l.info(f"Start fetching financial data from {financial_data_page_url}")
        res = self.fetch(financial_data_page_url)
        soup = self.soupify(res.content, 'html.parser')
        data_script = soup.find('script', string=re.compile('var originalData'))
        if not data_script:
            raise ParsingError('Failed to find script containing `var originalData` in the page content')
        data_script_string = data_script.string
        raw_data_string = self.regex_search_must_exist(r'(?<=originalData = )(.*)', data_script_string)
        try:
            raw_data = json.loads(raw_data_string.strip()[:-1])  # strip space and remove last semi colon
        except Exception as e:
            raise ParsingError(f'Failed to parse stock data string {e}: {raw_data_string}')
        years = [key for key in raw_data[0].keys() if key not in ['field_name', 'popup_icon']]
        stock_data = {
            'years': years
        }
        for raw_field_data in raw_data:
            try:
                field_name = self.beautify_field(self.soupify(raw_field_data['field_name'], 'html.parser').string)
            except Exception as e:
                _l.debug(f'Failed to parse field name from field data: {raw_field_data}')
                raise ParsingError(f'Failed to parse field name from field data: {e}')
            try:
                stock_data[field_name] = [float(raw_field_data[yr]) if raw_field_data[yr] else None for yr in years]
            except Exception as e:
                _l.debug(f'Failed to parse field data: {raw_field_data}')
                raise ParsingError(f'Failed to parse field {field_name} data: {e}')
        return stock_data

    def fetch_stock_price_data_and_profile_data(self, stock_price_page_url: str) -> (Dict, Dict):
        """Fetch stock historical price data

        :param stock_price_page_url:
        :return: stock price data and stock profile data
        """
        _l = self.logger
        _l.info(f'Start fetching price data from {stock_price_page_url}')
        res = self.fetch(stock_price_page_url)
        soup = self.soupify(res.content, 'html.parser')
        try:
            price_table_element, profile_table_element, _ = soup.find_all('table', class_='historical_data_table')
        except Exception as e:
            raise ParsingError(f'Could not parse page into price table and info table: {e}')
        try:
            price_table_headers = [self.beautify_field(th.string)
                                   for th in price_table_element.find_all('th') if 'colspan' not in th.attrs]
        except Exception as e:
            raise ParsingError(f'Failed to parse price table header: {e}')
        price_data = {tbl_header: [] for tbl_header in price_table_headers}
        price_table_row_elements = price_table_element.tbody.find_all('tr')
        for tbl_row_element in price_table_row_elements:
            for idx, cell_element in enumerate(tbl_row_element.find_all('td')):
                try:
                    cell_data = float(cell_element.string)
                except ValueError:
                    cell_data = cell_element.string
                price_data[price_table_headers[idx]].append(cell_data)

        try:
            profile_table_headers = [th.string for th in profile_table_element.find_all('th')]
        except Exception as e:
            raise ParsingError(f'Failed to parse info table header: {e}')
        profile_Data = {}
        profile_table_data_elements = [td for td in profile_table_element.find_all('td') if 'colspan' not in td.attrs]
        try:
            profile_Data['sector'] = profile_table_data_elements[profile_table_headers.index('Sector')].string
        except Exception as e:
            raise ParsingError(f'Failed to parse Sector: {e}')
        try:
            profile_Data['industry'] = profile_table_data_elements[profile_table_headers.index('Industry')].string
        except Exception as e:
            raise ParsingError(f'Failed to parse Industry: {e}')
        try:
            profile_Data['info'] = profile_table_element.find('td', attrs={'colspan': 4}).span.string
        except Exception as e:
            raise ParsingError(f'Failed to parse generic info: {e}')

        return price_data, profile_Data

    def fetch_stock_data(self, stock_url: str):
        """Fetch stock data from its url

        :param stock_url: macrotrends stock url
        :return:
        """
        _l = self.logger
        stock_data = {}
        for fin_aspect in self.financial_aspects:
            stock_data[self.beautify_field(fin_aspect)] = self.fetch_stock_financial_data(
                stock_url + fin_aspect)
        stock_data['price'], stock_data['profile'] = self.fetch_stock_price_data_and_profile_data(
            stock_url + 'stock-price-history')
        return stock_data
