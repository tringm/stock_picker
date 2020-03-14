from stock_picker.scrapper.macrotrends.scrapper import Scrapper
from tests.cases import TestCaseCompare
import json


class TestMacrotrendsScrapper(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.scrapper = Scrapper()

    def write_json_obj_to_out_file(self, obj):
        with self.out_file_path.open('w') as f:
            json.dump(obj, f, indent=2, sort_keys=True)

    def test_scrap_industry_listing_urls(self):
        industry_listing = self.scrapper.scrap_industry_listing_urls()
        self.write_json_obj_to_out_file(industry_listing)
        with self.exp_file_path.open() as f:
            exp_industry_listing = json.load(f)
        self.assertDictSubSet(industry_listing, exp_industry_listing)

    def test_scrap_stocks_data_from_industry_listing(self):
        industry_listing_url = 'https://www.macrotrends.net/stocks/industry/89/'
        stocks_data = self.scrapper.scrap_stocks_data_from_industry_listing(industry_listing_url)
        self.write_json_obj_to_out_file(stocks_data)
        with self.exp_file_path.open() as f:
            exp_stocks_data = json.load(f)
        # remove market_cap since it can be updated
        for ticker in stocks_data:
            stocks_data[ticker].pop('market_cap')
        for ticker in exp_stocks_data:
            exp_stocks_data[ticker].pop('market_cap')
        self.assertDictEqual(stocks_data, exp_stocks_data)

    def test_scrap_stock_data(self):
        stock_url = 'https://www.macrotrends.net/stocks/charts/BRK.B/berkshire-hathaway'
        stock_data = self.scrapper.scrap_stock_data(stock_url)
        self.write_json_obj_to_out_file(stock_data)
        with self.out_file_path.open('w') as f:
            json.dump(stock_data, f, indent=2, sort_keys=True)
        with self.exp_file_path.open() as f:
            exp_stock_data = json.load(f)
        for field in ('balance_sheet', 'cash_flow_statement', 'income_statement', 'price'):
            self.assertSetEqual(set(stock_data[field].keys()), set(exp_stock_data[field].keys()))

    def test_scrap_multiple_stocks_data(self):
        stock_urls = ['https://www.macrotrends.net/stocks/charts/BRK.A/berkshire-hathaway',
                      'https://www.macrotrends.net/stocks/charts/BRK.B/berkshire-hathaway']
        stocks_data = self.scrapper.scrap_multiple_stocks(stock_urls)
        self.write_json_obj_to_out_file(stocks_data)
        with self.exp_file_path.open() as f:
            exp_stock_data = json.load(f)
        for idx, datum in enumerate(stocks_data):
            for field in ('balance_sheet', 'cash_flow_statement', 'income_statement', 'price'):
                self.assertSetEqual(set(datum[field].keys()), set(exp_stock_data[idx][field].keys()))
