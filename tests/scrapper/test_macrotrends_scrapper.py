from stock_picker.scrapper.macrotrends import MacrotrendsScrapper
from tests.test_case import TestCaseCompare
import json


class TestMacrotrendsScrapper(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.scrapper = MacrotrendsScrapper()

    def test_fetch_industry_listing_urls(self):
        industry_listing = self.scrapper.fetch_industry_listing_urls()
        with self.exp_file_path.open() as f:
            exp_industry_listing = json.load(f)
        self.assertDictSubSet(industry_listing, exp_industry_listing)

    def test_fetch_stocks_url_and_profile_in_industry(self):
        industry_listing_url = 'https://www.macrotrends.net/stocks/industry/89/'
        stocks_url_and_profile = self.scrapper.fetch_stocks_url_and_profile_in_industry(industry_listing_url)
        with self.exp_file_path.open() as f:
            exp_stocks_url_and_profile = json.load(f)
        self.assertDictSubSet(stocks_url_and_profile, exp_stocks_url_and_profile)

    def test_fetch_stock_data(self):
        stock_url = 'https://www.macrotrends.net/stocks/charts/BRK.B/berkshire-hathaway'
        stock_data = self.scrapper.fetch_stock_data(stock_url)
        with self.exp_file_path.open() as f:
            exp_stock_data = json.load(f)
        self.assertDictSubSet(stock_data, exp_stock_data)
