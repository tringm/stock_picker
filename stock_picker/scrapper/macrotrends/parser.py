import json
import logging
import re
from typing import Dict

from bs4 import BeautifulSoup

LOG = logging.getLogger('scrapper.macrotrends.Parser')


class ParserError(Exception):
    def __init__(self, error, parsing_content):
        self.message = f'failed to parse: {error}'
        LOG.exception(self.message)
        LOG.debug(f'failed content: {parsing_content}')

    def __str__(self):
        return f'MacrotrendsParserError: {self.message}'


class Parser:
    def __init__(self, main_page_url: str = 'https://www.macrotrends.net'):
        self.main_page_url = main_page_url

    @staticmethod
    def _soupify(content, parser):
        """Raise error if cannot soupify content"""
        try:
            soup = BeautifulSoup(content, parser)
        except Exception as e:
            raise ParserError(e, content)
        return soup

    @staticmethod
    def _regex_search_must_exist(pattern, string) -> str:
        """Raise error if find no match when regex"""
        re_match = re.search(pattern, string)
        if not re_match:
            raise ParserError(f'regex found no match with pattern {pattern}', string)
        return string[re_match.start():re_match.end()]

    @staticmethod
    def beautify_field(field_name: str):
        return re.sub('[^A-Za-z0-9]+', '_', field_name.lower())

    def parse_industry_listing_urls(self, research_page_content: str) -> Dict:
        """Find listing of stocks by industry and its page url from the research page
        :param research_page_content: content of the research page at https://www.macrotrends.net/stocks/research
        :return: Dict of industry name and its page url
        """
        soup = self._soupify(research_page_content, 'html.parser')
        tables = soup.find_all('table')
        if not tables:
            raise ParserError('tables not found in industry listing page', soup)
        list_by_industry_table = [tbl for tbl in tables if tbl.thead.tr.th.strong.string == 'Stocks by Industry']
        if not list_by_industry_table:
            raise ParserError('table `Stocks by Industry` not found in industry listing page', tables)
        list_by_industry_table = list_by_industry_table[0]
        industry_cells = list_by_industry_table.find_all('td', attrs={'style': 'text-align:left'})
        if not industry_cells:
            raise ParserError('no industry cells in table `Stock by Industry`', list_by_industry_table)
        industry_listing_urls = {}
        parse_succeed = 0
        for cell in industry_cells:
            try:
                industry_listing_urls[self.beautify_field(cell.a.string)] = self.main_page_url + cell.a['href']
                parse_succeed += 1
            except Exception as e:
                LOG.exception(f'fail to parse industry cell {cell}: {e}')
                continue
        LOG.info(f'successfully parsed {parse_succeed}/{len(industry_listing_urls)} industry listing url')
        LOG.debug(f"found the following industry listing: {', '.join(industry_listing_urls.keys())}")
        return industry_listing_urls

    def parse_stocks_data_from_industry_listing_page(self, industry_listing_page_content: str) -> Dict:
        """Find stocks url and profile information by parsing listing of stock by industry page

        :param industry_listing_page_content: content of the industry listing page
        :return:
        """
        soup = self._soupify(industry_listing_page_content, 'html.parser')
        data_script = soup.find('script', string=re.compile('var data'))
        if not data_script:
            raise ParserError('no script containing `var data`', soup)
        stocks_data_table_string = self._regex_search_must_exist(r'(?<=var data = )(.*)', data_script.string)
        try:
            parsed_stocks_data_table = json.loads(stocks_data_table_string)
        except Exception as e:
            raise ParserError(f'fail to JSON parse stocks data table: {e}', stocks_data_table_string)
        LOG.debug(f'parsed stocks data table')
        stocks_data = {}
        parse_succeed = 0
        for parsed_stock_datum in parsed_stocks_data_table:
            try:
                stock_ticker = parsed_stock_datum['ticker']
                stock_datum = {
                    'country': self.beautify_field(parsed_stock_datum['country_code']),
                    'company_name': parsed_stock_datum['comp_name'],
                    'url': f"{self.main_page_url}/stocks/charts/{stock_ticker}/{parsed_stock_datum['comp_name']}"
                }
                try:
                    stock_datum['market_cap'] = float(parsed_stock_datum['market_val'])
                except Exception as e:
                    LOG.exception(f'failed to parse market_val field: {e}')
                    stock_datum['market_cap'] = None
                try:
                    stock_datum['backup_url'] = self._soupify(
                        parsed_stock_datum['link'], 'html.parser').a['href'].replace('/stock-price-history', '')
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

    def parse_stock_financial_aspect_page(self, financial_aspect_page_content: str):
        """parse financial aspect data from page

        :param financial_aspect_page_content:
        :return:
        """
        soup = self._soupify(financial_aspect_page_content, 'html.parser')
        data_script = soup.find('script', string=re.compile('var originalData'))
        if not data_script:
            raise ParserError('no script containing `var originalData` in the page content', soup)
        raw_data_string = self._regex_search_must_exist(r'(?<=originalData = )(.*)', data_script.string)
        try:
            raw_data = json.loads(raw_data_string.strip()[:-1])  # strip space and remove last semi colon
        except Exception as e:
            raise ParserError(f'failed to JSON parse stock data: {e}', {raw_data_string})
        years = [key for key in raw_data[0].keys() if key not in ['field_name', 'popup_icon']]
        parsed_data = {
            'years': years
        }
        for raw_field_data in raw_data:
            try:
                field_name = self.beautify_field(self._soupify(raw_field_data['field_name'], 'html.parser').string)
            except Exception as e:
                raise ParserError(f'table field name: {e}', raw_field_data)
            try:
                parsed_data[field_name] = [float(raw_field_data[yr]) if raw_field_data[yr] else None for yr in years]
            except Exception as e:
                raise ParserError(f'table `{field_name}` data: {e}', raw_field_data)
        LOG.debug(f'parsed financial aspect data')
        return parsed_data

    def parse_stock_price_data_and_profile_page(self, price_page_content: str) -> Dict:
        """parse stock historical price data and industry, sector, and description info from its price page

        :param price_page_content:
        :return: Dictionary of price and other info
        """
        soup = self._soupify(price_page_content, 'html.parser')
        try:
            price_table_element, profile_table_element, _ = soup.find_all('table', class_='historical_data_table')
        except Exception as e:
            raise ParserError(f'price table and info table: {e}', soup)
        try:
            price_table_headers = [self.beautify_field(th.string)
                                   for th in price_table_element.find_all('th') if 'colspan' not in th.attrs]
        except Exception as e:
            raise ParserError(f'price table header: {e}', price_table_element)
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
        parsed_data = {'price': price_data}
        try:
            profile_table_headers = [th.string for th in profile_table_element.find_all('th')]
        except Exception as e:
            LOG.error(f'fail to parse info table header: {e}')
            return parsed_data
        profile_table_data_elements = [td for td in profile_table_element.find_all('td') if 'colspan' not in td.attrs]
        try:
            parsed_data['sector'] = self.beautify_field(
                profile_table_data_elements[profile_table_headers.index('Sector')].string)
        except Exception as e:
            raise ParserError(f'sector: {e}', profile_table_data_elements)
        try:
            parsed_data['industry'] = self.beautify_field(
                profile_table_data_elements[profile_table_headers.index('Industry')].string)
        except Exception as e:
            raise ParserError(f'industry: {e}', profile_table_data_elements)
        try:
            parsed_data['description'] = profile_table_element.find('td', attrs={'colspan': 4}).span.string
        except Exception as e:
            raise ParserError(f'description: {e}', profile_table_element)
        LOG.debug(f'parsed price data')
        return parsed_data
