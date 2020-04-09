import logging

from stock_picker.picker import Picker
from stock_picker.utils.generic_utils import ROOT_PATH, logging_config

stocks_data_folder = ROOT_PATH / 'data' / 'stocks_data'
report_folder = ROOT_PATH / 'data' / 'reports'

logging_config(level=logging.DEBUG, filename=str(ROOT_PATH / '.logs' / 'pick.log'), filemode='w')
picker = Picker()
picker.discover_stocks_data_from_folder(stocks_data_folder)

sector = 'oils_energy'
sector_tickers = picker.get_sector_tickers(sector)
picker.create_reports_from_multiple_tickers(
    sector_tickers,
    unfiltered_period_report_out_file_path=report_folder / f'{sector}_period_unfiltered.csv',
    unfiltered_metrics_report_out_file_path=report_folder / f'{sector}_metrics_unfiltered.csv',
    filtered_metrics_report_out_file_path=report_folder / f'{sector}_metrics_filtered.csv'
)
