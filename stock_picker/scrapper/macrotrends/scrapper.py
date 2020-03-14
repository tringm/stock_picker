import asyncio
import logging
from typing import Dict, List, Tuple

import requests
from aiohttp import ClientSession

from .parser import Parser

LOG = logging.getLogger('scrapper.macrotrends')


class BaseError(Exception):
    def __init__(self, message: str):
        LOG.exception(message)
        self.message = message

    def __str__(self):
        return f'MacrotrendsScrapperError: {self.message}'


class FetchError(BaseError):
    def __init__(self, url: str, error):
        super().__init__(f'failed to fetch {url}: {error}')


class Scrapper:
    def __init__(
            self,
            main_page_url: str = 'https://www.macrotrends.net',
            research_page_postfix: str = '/stocks/research',
            financial_aspect_endpoints: Tuple[str] = ('income-statement', 'balance-sheet', 'cash-flow-statement'),
            price_endpoint: str = 'stock-price-history'
    ):
        self.main_page_url = main_page_url
        self.research_page_postfix = research_page_postfix
        self.financial_aspect_endpoints = financial_aspect_endpoints
        self.price_endpoint = price_endpoint
        self.all_endpoints = financial_aspect_endpoints + (price_endpoint,)
        self.parser = Parser(main_page_url)
        self.beautified_financial_aspects = [self.parser.beautify_field(field) for field in financial_aspect_endpoints]

    @staticmethod
    def fetch(url):
        """Raise error if cannot fetch data"""
        try:
            res = requests.get(url)
        except Exception as e:
            raise FetchError(url, e)
        return res.content

    @staticmethod
    async def async_fetch(client_session: ClientSession, url: str, raise_for_status: bool = True) -> str:
        """Asynchronously perform a get request

        :param client_session: aiohttp Client Session
        :param url: fetching url
        :param raise_for_status: raise Error if resposne status is 400 or highger
        :return: response for content
        """
        LOG.debug(f'fetching {url}')
        async with client_session.get(url, raise_for_status=raise_for_status) as resp:
            return await resp.text()

    def scrap_industry_listing_urls(self) -> Dict:
        """Find listing of stocks by industry urls urls by scrapping the research page
        :return: Dictionary of industries and their corresponding urls
        """
        research_page_content = self.fetch(self.main_page_url + self.research_page_postfix)
        return self.parser.parse_industry_listing_urls(research_page_content)

    def scrap_stocks_data_from_industry_listing(self, industry_listing_page_url: str) -> Dict:
        """Find stocks url and profile information by scrapping listing of stock by industry page

        :return: Dictionary of stocks ticker and its data
        """
        industry_listing_page_content = self.fetch(industry_listing_page_url)
        return self.parser.parse_stocks_data_from_industry_listing_page(industry_listing_page_content)

    def create_stock_data_from_parsed_pages(self, parsed_pages: List[Dict]):
        """create stock data from parsed financial aspect pages and price page

        :param parsed_pages: scrapped page in order of fin aspects + price
        :return:
        """
        stock_data = {field: parsed_pages[idx] for idx, field in enumerate(self.beautified_financial_aspects)}
        scrapped_price_data = parsed_pages[-1]
        for field in ['sector', 'industry', 'description']:
            stock_data[field] = scrapped_price_data.pop(field)
        stock_data['price'] = scrapped_price_data['price']
        return stock_data

    async def async_scrap_stock_pages(self, client_session: ClientSession, stock_main_page_url: str):
        """async fetch and parse stock financial aspect pages and price pages

        :param client_session: aiohttp client session
        :param stock_main_page_url: the main page of a stock
        :return: scrapped page
        """
        async def scrap_fin_asp_page(aspect: str) -> Dict:
            content = await self.async_fetch(client_session, f'{stock_main_page_url}/{aspect}')
            if isinstance(content, Exception):
                raise FetchError(f'{stock_main_page_url}/{aspect}', content)
            return self.parser.parse_stock_financial_aspect_page(content)

        async def scrap_price_page() -> Dict:
            content = await self.async_fetch(client_session, f'{stock_main_page_url}/{self.price_endpoint}')
            if isinstance(content, Exception):
                raise FetchError(f'{stock_main_page_url}/{self.price_endpoint}', content)
            return self.parser.parse_stock_price_data_and_profile_page(content)
        tasks = [scrap_fin_asp_page(asp) for asp in self.financial_aspect_endpoints] + [scrap_price_page()]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def async_scrap_stock_data(self, client_session: ClientSession, stock_main_page_url: str) -> Dict:
        LOG.debug(f'scrapping {stock_main_page_url}')
        scrapped_pages = await self.async_scrap_stock_pages(client_session, stock_main_page_url)
        for page in scrapped_pages:
            if isinstance(page, Exception):
                raise BaseError(f'fail to scrap {stock_main_page_url}: {page}')
        stock_data = self.create_stock_data_from_parsed_pages(scrapped_pages)
        LOG.debug(f'scrapped {stock_main_page_url}')
        return stock_data

    def scrap_multiple_stocks(self, stock_main_page_urls: List[str]) -> List[Dict]:
        async def run():
            async with ClientSession() as session:
                tasks = [self.async_scrap_stock_data(session, url) for url in stock_main_page_urls]
                return await asyncio.gather(*tasks, return_exceptions=True)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        scrapped_data = loop.run_until_complete(run())
        LOG.info(f'scrapped {len(stock_main_page_urls)} stock data')
        return scrapped_data

    def scrap_stock_data(self, stock_main_page_url: str):
        """Scrap stock data from its url

        :param stock_main_page_url: macrotrends stock url
        :return:
        """

        async def run():
            async with ClientSession(raise_for_status=True) as session:
                return await self.async_scrap_stock_data(session, stock_main_page_url)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        scrapped_data = loop.run_until_complete(run())
        LOG.info(f'scrapped {stock_main_page_url} stock data')
        return scrapped_data
