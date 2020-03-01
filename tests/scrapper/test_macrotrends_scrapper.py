from stock_picker.scrapper.macrotrends import MacrotrendsScrapper
from tests.test_case import TestCaseCompare
import json


class TestMacrotrendsScrapper(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.scrapper = MacrotrendsScrapper()

    def test_scrap_industry_listing_urls(self):
        industry_listing = self.scrapper.scrap_industry_listing_urls()
        with self.exp_file_path.open() as f:
            exp_industry_listing = json.load(f)
        self.assertDictSubSet(industry_listing, exp_industry_listing)

    def test_scrap_stocks_data_from_industry_listing(self):
        industry_listing_url = 'https://www.macrotrends.net/stocks/industry/89/'
        stocks_data = self.scrapper.scrap_stocks_data_from_industry_listing(industry_listing_url)
        with self.exp_file_path.open() as f:
            exp_stocks_data = json.load(f)
        self.assertDictSubSet(stocks_data, exp_stocks_data)

    def test_scrap_stock_data(self):
        stock_url = 'https://www.macrotrends.net/stocks/charts/BRK.B/berkshire-hathaway'
        stock_data = self.scrapper.scrap_stock_data(stock_url)
        with self.exp_file_path.open() as f:
            exp_stock_data = json.load(f)
        self.assertDictSubSet(stock_data, exp_stock_data)
