from bs4 import BeautifulSoup
import requests
import logging
import re
import json
from typing import Dict


LOG = logging.getLogger('MacrotrendsScrapper')


class BaseError(Exception):
    def __init__(self, message: str):
        LOG.exception(self.message)
        self.message = message

    def __str__(self):
        return f'MacrotrendsScrapperError: {self.message}'


class FetchError(BaseError):
    def __init__(self, url, error):
        super().__init__(f'failed to fetch {url}: {error}')


class ParseError(BaseError):
    def __init__(self, message, parsing_content):
        LOG.debug(f'fail to parse: {parsing_content}')
        super().__init__(message)


class MacrotrendsScrapper:
    def __init__(self):
        self.main_page_url = 'https://www.macrotrends.net'
        self.stock_research_page_url = self.main_page_url + '/stocks/research'

    @staticmethod
    def _fetch(url):
        """Raise error if cannot fetch data"""
        try:
            res = requests.get(url)
        except Exception as e:
            raise FetchError(url, e)
        return res.content

    @staticmethod
    def _soupify(content, parser):
        """Raise error if cannot soupify content"""
        try:
            soup = BeautifulSoup(content, parser)
        except Exception as e:
            raise ParseError(f'fail to soupify content with parser {parser}: {e}', content)
        return soup

    @staticmethod
    def _regex_search_must_exist(pattern, string) -> str:
        """Raise error if find no match when regex"""
        re_match = re.search(pattern, string)
        if not re_match:
            raise ParseError(f'regex found no match with pattern {pattern}', string)
        return string[re_match.start():re_match.end()]

    @staticmethod
    def _beautify_field(field_name: str):
        return re.sub('[^A-Za-z0-9]+', '_', field_name.lower())

    def scrap_industry_listing_urls(self) -> Dict:
        """Find listing of stocks by industry urls urls by scrapping the research page
        :return: Dictionary of industries and their corresponding urls
        """
        res = self._fetch(self.stock_research_page_url)
        soup = self._soupify(res, 'html.parser')
        tables = soup.find_all('table')
        if not tables:
            raise ParseError('tables not found in industry listing page', soup)
        list_by_industry_table = [tbl for tbl in tables if tbl.thead.tr.th.strong.string == 'Stocks by Industry']
        if not list_by_industry_table:
            raise ParseError('table `Stocks by Industry` not found in industry listing page', tables)
        list_by_industry_table = list_by_industry_table[0]
        industry_cells = list_by_industry_table.find_all('td', attrs={'style': 'text-align:left'})
        if not industry_cells:
            raise ParseError('no industry cells in table `Stock by Industry`', list_by_industry_table)
        industry_listing_urls = {}
        parse_succeed = 0
        for cell in industry_cells:
            try:
                industry_listing_urls[self._beautify_field(cell.a.string)] = self.main_page_url + cell.a['href']
                parse_succeed += 1
            except Exception as e:
                LOG.exception(f'fail to parse industry cell {cell}: {e}')
                continue
        LOG.info(f'successfully parsed {parse_succeed}/{len(industry_listing_urls)} industry listing url')
        LOG.debug(f"found the following industry listing: {', '.join(industry_listing_urls.keys())}")
        return industry_listing_urls

    def scrap_stocks_data_from_industry_listing(self, industry_listing_page_url: str) -> Dict:
        """Find stocks url and profile information by scrapping listing of stock by industry page

        :return: Dictionary of stocks ticker and its data
        """
        res = self._fetch(industry_listing_page_url)
        soup = self._soupify(res, 'html.parser')
        data_script = soup.find('script', string=re.compile('var data'))
        if not data_script:
            raise ParseError('no script containing `var data`', soup)
        stocks_data_table_string = self._regex_search_must_exist(r'(?<=var data = )(.*)', data_script.string)
        try:
            scrapped_stocks_data_table = json.loads(stocks_data_table_string)
        except Exception as e:
            raise ParseError(f'fail to JSON parse stocks data table: {e}', stocks_data_table_string)
        LOG.debug(f'scrapped stocks data table: {scrapped_stocks_data_table}')
        stocks_data = {}
        parse_succeed = 0
        for scrapped_stock_datum in scrapped_stocks_data_table:
            try:
                stock_ticker = scrapped_stock_datum['ticker']
                stock_datum = {
                    'country': self._beautify_field(scrapped_stock_datum['country_code']),
                    'company_name': scrapped_stock_datum['comp_name'],
                    'url': f"{self.main_page_url}/stocks/charts/{stock_ticker}/{scrapped_stock_datum['comp_name']}"
                }
                try:
                    stock_datum['market_cap'] = float(scrapped_stock_datum['market_val'])
                except Exception as e:
                    LOG.exception(f'failed to parse market_val field: {e}')
                    stock_datum['market_cap'] = None
                try:
                    stock_datum['backup_url'] = self._soupify(
                        scrapped_stock_datum['link'], 'html.parser').a['href'].replace('/stock-price-history', '')
                except Exception as e:
                    LOG.exception(f'failed to parse backup url: {e}')
                    stock_datum['backup_url'] = None
            except Exception as e:
                LOG.exception(f'failed to parse stock datum: {e}')
                continue
            stocks_data[stock_ticker] = stock_datum
            parse_succeed += 1
        LOG.info(f'successfully parsed {parse_succeed}/{len(stocks_data)} stocks data in this industry listing')
        return stocks_data

    def scrap_stock_financial_aspect_data(self, financial_aspect: str, stock_url: str):
        """Fetch a finanicla aspect data of a stock

        :param financial_aspect:
        :param stock_url:
        :return:
        """
        financial_aspect_page_url = f'{stock_url}/{financial_aspect}'
        LOG.debug(f'fetching {financial_aspect} data from {financial_aspect_page_url}...')
        res = self._fetch(financial_aspect_page_url)
        soup = self._soupify(res, 'html.parser')
        data_script = soup.find('script', string=re.compile('var originalData'))
        if not data_script:
            raise ParseError('no script containing `var originalData` in the page content', soup)
        raw_data_string = self._regex_search_must_exist(r'(?<=originalData = )(.*)', data_script.string)
        try:
            raw_data = json.loads(raw_data_string.strip()[:-1])  # strip space and remove last semi colon
        except Exception as e:
            raise ParseError(f'failed to JSON parse stock data: {e}', {raw_data_string})
        years = [key for key in raw_data[0].keys() if key not in ['field_name', 'popup_icon']]
        stock_data = {
            'years': years
        }
        for raw_field_data in raw_data:
            try:
                field_name = self._beautify_field(self._soupify(raw_field_data['field_name'], 'html.parser').string)
            except Exception as e:
                raise ParseError(f'failed to parse field name: {e}', raw_field_data)
            try:
                stock_data[field_name] = [float(raw_field_data[yr]) if raw_field_data[yr] else None for yr in years]
            except Exception as e:
                raise ParseError(f'failed to parse `{field_name}` data: {e}', raw_field_data)
        LOG.info(f"fetched {financial_aspect} data from {financial_aspect_page_url}")
        return stock_data

    def scrap_stock_price_data_and_profile_data(self, stock_url: str) -> Dict:
        """Fetch stock historical price data and industry, sector, and description info from its price page

        :param stock_url:
        :return: Dictionary of price and other info
        """
        stock_price_page_url = f'{stock_url}/stock-price-history'
        LOG.debug(f'fetching price data from {stock_price_page_url}...')
        res = self._fetch(stock_price_page_url)
        soup = self._soupify(res, 'html.parser')
        try:
            price_table_element, profile_table_element, _ = soup.find_all('table', class_='historical_data_table')
        except Exception as e:
            raise ParseError(f'fail to parse page into price table and info table: {e}', soup)
        try:
            price_table_headers = [self._beautify_field(th.string)
                                   for th in price_table_element.find_all('th') if 'colspan' not in th.attrs]
        except Exception as e:
            raise ParseError(f'fail to parse price table header: {e}', price_table_element)
        price_table_row_elements = price_table_element.tbody.find_all('tr')
        price_data = {tbl_header: [] for tbl_header in price_table_headers}
        for tbl_row_element in price_table_row_elements:
            for idx, cell_element in enumerate(tbl_row_element.find_all('td')):
                try:
                    cell_data = float(cell_element.string)
                except ValueError:
                    cell_data = cell_element.string
                price_data[price_table_headers[idx]].append(cell_data)
        price_data['years'] = price_data['year']
        price_data.pop('year', None)
        stock_data = {'price': price_data}
        try:
            profile_table_headers = [th.string for th in profile_table_element.find_all('th')]
        except Exception as e:
            LOG.error(f'fail to parse info table header: {e}')
            return stock_data
        profile_table_data_elements = [td for td in profile_table_element.find_all('td') if 'colspan' not in td.attrs]
        try:
            stock_data['sector'] = self._beautify_field(
                profile_table_data_elements[profile_table_headers.index('Sector')].string)
        except Exception as e:
            raise ParseError(f'fail to parse sector: {e}', profile_table_data_elements)
        try:
            stock_data['industry'] = self._beautify_field(
                profile_table_data_elements[profile_table_headers.index('Industry')].string)
        except Exception as e:
            raise ParseError(f'fail to parse industry: {e}', profile_table_data_elements)
        try:
            stock_data['description'] = profile_table_element.find('td', attrs={'colspan': 4}).span.string
        except Exception as e:
            raise ParseError(f'fail to parse description: {e}', profile_table_element)
        LOG.info(f'fetched price data from {stock_price_page_url}')
        return stock_data

    def scrap_stock_data(self, stock_url: str):
        """Scrap stock data from its url

        :param stock_url: macrotrends stock url
        :return:
        """
        LOG.debug(f'fetching stock data of {stock_url}...')
        stock_data = {}
        for fin_aspect in ('income-statement', 'balance-sheet', 'cash-flow-statement'):
            stock_data[self._beautify_field(fin_aspect)] = self.scrap_stock_financial_aspect_data(fin_aspect, stock_url)
        stock_data.update(self.scrap_stock_price_data_and_profile_data(stock_url))
        LOG.debug(f'fetched stock data of {stock_url}')
        return stock_data
