from bs4 import BeautifulSoup
import requests
import logging
import re
import json
from typing import Dict, List, Optional, Awaitable
from aiohttp import ClientSession
import asyncio

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


class ScrapError(BaseError):
    def __init__(self, error, parsing_content):
        super().__init__(f'failed to scrap: {error}')
        LOG.debug(f'failed scrap content: {parsing_content}')


class MacrotrendsScrapper:
    def __init__(self):
        self.main_page_url = 'https://www.macrotrends.net'
        self.stock_research_page_url = self.main_page_url + '/stocks/research'
        self.financial_aspects = ('income-statement', 'balance-sheet', 'cash-flow-statement')
        self.beautified_financial_aspects = ('income_statement', 'balance_sheet', 'cash_flow_statement')

    @staticmethod
    def _fetch(url):
        """Raise error if cannot fetch data"""
        try:
            res = requests.get(url)
        except Exception as e:
            raise FetchError(url, e)
        return res.content

    @staticmethod
    async def _async_fetch(client_session: ClientSession, url: str) -> Optional[str]:
        """Asynchronously perform a get request

        :param client_session: aiohttp Client Session
        :param url: fetching url
        :return: None if response status is not 200
        """
        LOG.debug(f'fetching {url}')
        async with client_session.get(url) as resp:
            if resp.status != 200:
                return None
            return await resp.text()

    @staticmethod
    def _soupify(content, parser):
        """Raise error if cannot soupify content"""
        try:
            soup = BeautifulSoup(content, parser)
        except Exception as e:
            LOG.debug(f'failed soupify content: {content}')
            raise BaseError(f'fail to soupify content with parser {parser}: {e}')
        return soup

    @staticmethod
    def _regex_search_must_exist(pattern, string) -> str:
        """Raise error if find no match when regex"""
        re_match = re.search(pattern, string)
        if not re_match:
            raise ScrapError(f'regex found no match with pattern {pattern}', string)
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
            raise ScrapError('tables not found in industry listing page', soup)
        list_by_industry_table = [tbl for tbl in tables if tbl.thead.tr.th.strong.string == 'Stocks by Industry']
        if not list_by_industry_table:
            raise ScrapError('table `Stocks by Industry` not found in industry listing page', tables)
        list_by_industry_table = list_by_industry_table[0]
        industry_cells = list_by_industry_table.find_all('td', attrs={'style': 'text-align:left'})
        if not industry_cells:
            raise ScrapError('no industry cells in table `Stock by Industry`', list_by_industry_table)
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
            raise ScrapError('no script containing `var data`', soup)
        stocks_data_table_string = self._regex_search_must_exist(r'(?<=var data = )(.*)', data_script.string)
        try:
            scrapped_stocks_data_table = json.loads(stocks_data_table_string)
        except Exception as e:
            raise ScrapError(f'fail to JSON parse stocks data table: {e}', stocks_data_table_string)
        LOG.debug(f'scrapped stocks data table')
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

    def scrap_stock_financial_aspect_page(self, financial_aspect_page_content: str):
        """ scrap financial aspect data from page

        :param financial_aspect_page_content:
        :return:
        """
        soup = self._soupify(financial_aspect_page_content, 'html.parser')
        data_script = soup.find('script', string=re.compile('var originalData'))
        if not data_script:
            raise ScrapError('no script containing `var originalData` in the page content', soup)
        raw_data_string = self._regex_search_must_exist(r'(?<=originalData = )(.*)', data_script.string)
        try:
            raw_data = json.loads(raw_data_string.strip()[:-1])  # strip space and remove last semi colon
        except Exception as e:
            raise ScrapError(f'failed to JSON parse stock data: {e}', {raw_data_string})
        years = [key for key in raw_data[0].keys() if key not in ['field_name', 'popup_icon']]
        scrapped_data = {
            'years': years
        }
        for raw_field_data in raw_data:
            try:
                field_name = self._beautify_field(self._soupify(raw_field_data['field_name'], 'html.parser').string)
            except Exception as e:
                raise ScrapError(f'table field name: {e}', raw_field_data)
            try:
                scrapped_data[field_name] = [float(raw_field_data[yr]) if raw_field_data[yr] else None for yr in years]
            except Exception as e:
                raise ScrapError(f'table `{field_name}` data: {e}', raw_field_data)
        LOG.debug(f'scrapped financial aspect data')
        return scrapped_data

    def scrap_stock_price_data_and_profile_page(self, price_page_content: str) -> Dict:
        """scrap stock historical price data and industry, sector, and description info from its price page

        :param price_page_content:
        :return: Dictionary of price and other info
        """
        soup = self._soupify(price_page_content, 'html.parser')
        try:
            price_table_element, profile_table_element, _ = soup.find_all('table', class_='historical_data_table')
        except Exception as e:
            raise ScrapError(f'price table and info table: {e}', soup)
        try:
            price_table_headers = [self._beautify_field(th.string)
                                   for th in price_table_element.find_all('th') if 'colspan' not in th.attrs]
        except Exception as e:
            raise ScrapError(f'price table header: {e}', price_table_element)
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
        scrapped_data = {'price': price_data}
        try:
            profile_table_headers = [th.string for th in profile_table_element.find_all('th')]
        except Exception as e:
            LOG.error(f'fail to parse info table header: {e}')
            return scrapped_data
        profile_table_data_elements = [td for td in profile_table_element.find_all('td') if 'colspan' not in td.attrs]
        try:
            scrapped_data['sector'] = self._beautify_field(
                profile_table_data_elements[profile_table_headers.index('Sector')].string)
        except Exception as e:
            raise ScrapError(f'sector: {e}', profile_table_data_elements)
        try:
            scrapped_data['industry'] = self._beautify_field(
                profile_table_data_elements[profile_table_headers.index('Industry')].string)
        except Exception as e:
            raise ScrapError(f'industry: {e}', profile_table_data_elements)
        try:
            scrapped_data['description'] = profile_table_element.find('td', attrs={'colspan': 4}).span.string
        except Exception as e:
            raise ScrapError(f'description: {e}', profile_table_element)
        LOG.debug(f'scrapped price data')
        return scrapped_data

    async def async_scrap_stock_pages(
            self,
            client_session: ClientSession,
            stock_main_page_url: str
    ):
        """

        :param client_session: aiohttp client session
        :param stock_main_page_url: the main page of a stock
        :return: scrapped page
        """
        async def fetch_and_scrap_fin_asp_page(aspect: str) -> Optional[Dict]:
            LOG.debug(f'fetching {stock_main_page_url}/{aspect}')
            async with client_session.get(f'{stock_main_page_url}/{aspect}') as resp:
                if resp.status != 200:
                    return None
                resp_text = await resp.text()
                return self.scrap_stock_financial_aspect_page(resp_text)

        async def fetch_and_scrap_price_page() -> Optional[Dict]:
            LOG.debug(f'fetching {stock_main_page_url}/stock-price-history')
            async with client_session.get(f'{stock_main_page_url}/stock-price-history') as resp:
                if resp.status != 200:
                    return None
                resp_text = await resp.text()
                return self.scrap_stock_price_data_and_profile_page(resp_text)
        tasks = [fetch_and_scrap_fin_asp_page(asp) for asp in self.financial_aspects] + [fetch_and_scrap_price_page()]
        return await asyncio.gather(*tasks)

    def get_stock_data_from_scrapped_pages(self, scrapped_pages: List[Dict]):
        """

        :param scrapped_pages: scrapped page in order of fin aspects + price
        :return:
        """
        stock_data = {field: scrapped_pages[idx] for idx, field in enumerate(self.beautified_financial_aspects)}
        scrapped_price_data = scrapped_pages[3]
        for field in ['sector', 'industry', 'description']:
            stock_data[field] = scrapped_price_data.pop(field)
        stock_data['price'] = scrapped_price_data['price']
        return stock_data

    async def async_scrap_stock_data(self, client_session: ClientSession, stock_main_page_url: str) -> Dict:
        LOG.debug(f'scrapping {stock_main_page_url}')
        scrapped_pages = await self.async_scrap_stock_pages(client_session, stock_main_page_url)
        LOG.debug(f'fetched all pages')
        return self.get_stock_data_from_scrapped_pages(scrapped_pages)

    def scrap_multiple_stocks(self, main_page_urls: List[str]) -> List[Dict]:
        async def run():
            async with ClientSession(raise_for_status=True) as session:
                tasks = [self.async_scrap_stock_data(session, url) for url in main_page_urls]
                return await asyncio.gather(*tasks)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        scrapped_data = loop.run_until_complete(run())
        LOG.info(f'scrapped {len(main_page_urls)} stocks')
        return scrapped_data

    def scrap_stock_data(self, stock_main_page_url: str):
        """Scrap stock data from its url

        :param stock_main_page_url: macrotrends stock url
        :return:
        """
        async def run():
            async with ClientSession(raise_for_status=True) as session:
                tasks = [self.async_scrap_stock_data(session, stock_main_page_url)]
                return await asyncio.gather(*tasks)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        scrapped_data = loop.run_until_complete(run())[0]
        LOG.info(f'scrapped {stock_main_page_url} stocks')
        return scrapped_data
