import json
import logging

from stock_picker.scrapper.macrotrends.scrapper import Scrapper
from stock_picker.utils.generic_utils import ROOT_PATH, logging_config

LOG = logging.getLogger('ScrapRunner')


def set_up_logging():
    logging_config(filename=str(ROOT_PATH / 'scrap.log'), filemode='w', level=logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    LOG.addHandler(console)
    logging.getLogger('scrapper.macrotrends').addHandler(console)



set_up_logging()
data_folder = ROOT_PATH / 'data'
scrapper = Scrapper()

# # create meta file
# industry_listings = scrapper.scrap_industry_listing_urls()
# LOG.info(f'scrapped {len(industry_listings)} industry listings')
# stocks_data = {}
# failed_industries = {}
# for industry in industry_listings:
#     try:
#         stocks_data.update(scrapper.scrap_stocks_data_from_industry_listing(industry_listings[industry]))
#     except Exception as e:
#         LOG.error(f'fail to scrap industry {industry} listing: {e}')
#         failed_industries[industry] = str(e)
#         continue
#     LOG.info(f'scrapped industry {industry}')
#
# with (data_folder / 'meta.json').open('w') as f:
#     json.dump({'industries': scrapper.scrap_industry_listing_urls(), 'stocks': stocks_data}, f)
#
# if failed_industries:
#     with (data_folder / 'failed_industries.json').open('w') as f:
#         json.dump(failed_industries, f)

with (data_folder / 'meta.json').open() as f:
    meta_data = json.load(f)
stocks_data = meta_data['stocks']

stocks_data_folder = (data_folder / 'stocks_data')
stocks_data_folder.mkdir(parents=True, exist_ok=True)
count_success = 0

# TODO: Rerun failed ticker
failed_tickers = {}
stocks_ticker = list(stocks_data.keys())
stocks_url = [stocks_data[ticker]['url'] for ticker in stocks_ticker]
scrapped_stocks_data = scrapper.scrap_multiple_stocks(stocks_url)
for idx, ticker in enumerate(stocks_ticker):
    stock_datum = stocks_data[ticker]
    scrapped_stock_datum = scrapped_stocks_data[idx]
    if isinstance(scrapped_stock_datum, Exception):
        LOG.error(f'fail to scrap {ticker}: {scrapped_stock_datum}')
        failed_tickers[ticker] = str(scrapped_stock_datum)
    else:
        stock_datum.update(scrapped_stock_datum)
        count_success += 1
        with (stocks_data_folder / f'{ticker}.json').open('w') as f:
            json.dump(stock_datum, f)

if failed_tickers:
    with (data_folder / 'failed_tickers.json').open('w') as f:
        json.dump(failed_tickers, f)
LOG.info(f'successfully scrapped {count_success}/{len(stocks_ticker)}')
