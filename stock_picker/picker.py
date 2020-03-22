from pathlib import Path
import json
import logging
from typing import Dict, List, Tuple, Iterable
import numpy as np
from collections import OrderedDict
import csv


LOG = logging.getLogger('Picker')


def apply_f_excl_none(func, array):
    array_excl_none = [item for item in array if item is not None]
    if not array_excl_none:
        return None
    else:
        return func(array_excl_none)


def div(item1, item2):
    try:
        return item1 / item2
    except Exception:
        return None


def sub(item1, item2):
    try:
        return item1 - item2
    except Exception:
        return None


def add(item1, item2):
    try:
        return item1 + item2
    except Exception:
        return None


def reverse_sign(number):
    if number is not None:
        return -number
    else:
        return None


def coef_var(arr):
    array_excl_none = [item for item in arr if item is not None]
    try:
        return np.std(array_excl_none) / np.average(array_excl_none)
    except Exception:
        return None


def change_rate(item1, item2):
    try:
        return (item1 - item2)/item2
    except Exception:
        return None


def check_lt(num1, num2):
    if num1 is not None and num2 is not None and num1 < num2:
        return True
    return False


def flipped_linear_corrcoef(arr, n_data_points):
    arr_exc_none = [item for item in arr if item is not None]
    flipped_arr = np.flip(arr_exc_none[:n_data_points])
    if len(flipped_arr) < n_data_points:
        return None
    return np.corrcoef(flipped_arr, list(range(len(flipped_arr))))[0, 1]


class Picker:
    def __init__(self):
        self._all_stocks_data = {}
        self._stocks_by_industries = {}
        self._stocks_by_sectors = {}
        self.required_info = ('cash_flow_statement', 'income_statement', 'balance_sheet', 'price')

        self.default_period = (0, 4)
        self.default_function = np.average
        # If not specified, calculate average for period 0_4 by default
        self.report_by_period_schema = {
            'income_statement': OrderedDict({
                'revenue': {
                    'periods': [(0, 4), (5, 9)],
                    'functions': [np.average]
                },
                'gross_profit': None,
                'net_income': None,
                'total_non_operating_income_expense': None,
                'research_and_development_expenses': None,
                'shares_outstanding': None,
                'operating_expenses': {
                    'periods': [(0, 4), (5, 9)],
                    'functions': [np.average]
                },
                'eps_earnings_per_share': {
                    'periods': [(0, 4), (5, 9), (10, 14)],
                    'functions': [np.average, np.std]
                }
            }),
            # TODO: rising shares outstanding
            'balance_sheet': OrderedDict({
                'cash_on_hand': None,
                'share_holder_equity': None,
                'goodwill_and_intangible_assets': None,
                'total_current_assets': {
                    'periods': [(0, 4), (5, 9)],
                    'functions': [np.average]
                },
                'total_current_liabilities': {
                    'periods': [(0, 4), (5, 9)],
                    'functions': [np.average]
                },
                'total_assets': {
                    'periods': [(0, 4), (5, 9)],
                    'functions': [np.average]
                },
                'total_liabilities': {
                    'periods': [(0, 4), (5, 9)],
                    'functions': [np.average]
                }
            }),
            'cash_flow_statement': {
                'net_income_loss': None,
                'cash_flow_from_operating_activities': None,
                'total_common_and_preferred_stock_dividends_paid': None,
                'net_cash_flow': {
                    'periods': [(0, 4)],
                    'functions': [np.average, coef_var]
                }
            },
            'price': {
                'average_stock_price': {
                    'periods': [(0, 4), (5, 9)],
                    'functions': [np.average]
                }
            }
            # TODO: rising accounts receivable, rising inventory
        }

    @property
    def all_stocks_data(self) -> Dict:
        return self._all_stocks_data

    @property
    def industries(self) -> List:
        return list(self._stocks_by_industries.keys())

    @property
    def sectors(self) -> List:
        return list(self._stocks_by_sectors.keys())

    def add_ticker_to_industry(self, industry, ticker):
        if industry not in self._stocks_by_industries:
            self._stocks_by_industries[industry] = [ticker]
        else:
            self._stocks_by_industries[industry].append(ticker)

    def add_ticker_to_sector(self, sector, ticker):
        if sector not in self._stocks_by_sectors:
            self._stocks_by_sectors[sector] = [ticker]
        else:
            self._stocks_by_sectors[sector].append(ticker)

    def get_industry_tickers(self, industry) -> List:
        return self._stocks_by_industries[industry]

    def get_sector_tickers(self, sector) -> List:
        return self._stocks_by_sectors[sector]

    def get_stock_data(self, ticker) -> Dict:
        return self._all_stocks_data[ticker]

    def add_stock_data(self, stock_ticker, stock_data):
        """ Add stock data

        :param stock_ticker:
        :param stock_data:
        :return:
        """
        for req_field in self.required_info:
            if req_field not in stock_data:
                raise ValueError(f'{req_field} not in {stock_ticker} data')
            try:
                req_field_data = stock_data[req_field]
                ordered_fields = ('years',) + tuple(field for field in req_field_data if field != 'years')
                zipped = zip(*[req_field_data[field] for field in ordered_fields])
                for idx, sorted_data in enumerate(zip(*sorted(zipped, reverse=True))):
                    req_field_data[ordered_fields[idx]] = sorted_data
            except Exception as e:
                raise ValueError(f'fail to sort {req_field} data by year: {e}')

        self._all_stocks_data[stock_ticker] = stock_data
        if 'sector' in stock_data:
            self.add_ticker_to_sector(stock_data['sector'], stock_ticker)
        else:
            LOG.warning(f'sector not found in {stock_ticker} data')
        if 'industry' in stock_data:
            self.add_ticker_to_industry(stock_data['industry'], stock_ticker)
        else:
            LOG.warning(f'industry not found in {stock_ticker} data')
        LOG.debug(f'added {stock_ticker}')

    def discover_stocks_data_from_folder(self, stocks_folder_path: Path):
        loaded, file_count = 0, 0
        for file in stocks_folder_path.glob('*.json'):
            file_count += 1
            try:
                with file.open() as f:
                    self.add_stock_data(file.stem, json.load(f))
                loaded += 1
            except Exception as e:
                LOG.exception(f'fail to parse file {file}: {e}')
        LOG.info(f'loaded {loaded}/{file_count} stock data file in {stocks_folder_path}')

    def create_report_by_period(self, data, schema) -> OrderedDict:
        def report_field_name(field, function, period):
            return f'{function.__name__}_{field}_prev_{period[0]}_{period[1]}_y'

        report = OrderedDict()
        for fld in schema:
            field_schema = schema[fld]
            if not field_schema:
                per = self.default_period
                func = self.default_function
                report[report_field_name(fld, func, per)] = apply_f_excl_none(func, data[fld][per[0]:per[1]])
            else:
                for per in field_schema['periods']:
                    for func in field_schema['functions']:
                        report[report_field_name(fld, func, per)] = apply_f_excl_none(func, data[fld][per[0]:per[1]])
        return report

    def generate_income_statement_reports(self, i_s_data: Dict) -> Tuple[OrderedDict, OrderedDict]:
        """create report by periods and create metrics report from income statement

        :param i_s_data: income statement data
        :return: (report by periods, metrics report)
        """
        period_rep = self.create_report_by_period(i_s_data, self.report_by_period_schema['income_statement'])

        metrics_rep = OrderedDict()
        metrics_rep['linear_corrcoef_revenue_last_5y'] = flipped_linear_corrcoef(i_s_data['revenue'], 5)
        metrics_rep['op_expenses_ratio_prev_0_4_y'] = div(
            period_rep['average_operating_expenses_prev_0_4_y'], period_rep['average_revenue_prev_0_4_y'])
        metrics_rep['op_expenses_ratio_prev_5_9_y'] = div(
            period_rep['average_operating_expenses_prev_5_9_y'], period_rep['average_revenue_prev_5_9_y'])
        metrics_rep['eps_growth_prev_0_4_vs_5_9_y'] = change_rate(
            period_rep['average_eps_earnings_per_share_prev_0_4_y'],
            period_rep['average_eps_earnings_per_share_prev_5_9_y'])
        metrics_rep['eps_growth_prev_0_4_vs_10_14_y'] = change_rate(
            period_rep['average_eps_earnings_per_share_prev_0_4_y'],
            period_rep['average_eps_earnings_per_share_prev_10_14_y'])
        metrics_rep['linear_corrcoef_eps_last_5y'] = flipped_linear_corrcoef(i_s_data['eps_earnings_per_share'], 5)
        metrics_rep['average_r_and_d_expenses_ratio_prev_0_4_y'] = div(
            period_rep['average_research_and_development_expenses_prev_0_4_y'],
            period_rep['average_operating_expenses_prev_0_4_y']
        )
        metrics_rep['average_non_op_income_expense_per_net_income_ratio_prev_0_4_y'] = div(
            period_rep['average_total_non_operating_income_expense_prev_0_4_y'],
            period_rep['average_net_income_prev_0_4_y']
        )
        metrics_rep['linear_corrcoef_shares_outstanding_last_5y'] = flipped_linear_corrcoef(
            i_s_data['shares_outstanding'], 5)
        metrics_rep['average_gross_profit_margin_prev_0_4_y'] = div(
            period_rep['average_gross_profit_prev_0_4_y'], period_rep['average_revenue_prev_0_4_y']
        )
        return period_rep, metrics_rep

    def generate_balance_sheet_reports(self, b_s_data: Dict) -> Tuple[OrderedDict, OrderedDict]:
        """create report by periods and create metrics report from balance sheet

        :param b_s_data: balance sheet data
        :return: (report by periods, metrics report)
        """
        period_rep = self.create_report_by_period(b_s_data, self.report_by_period_schema['balance_sheet'])
        metrics_rep = OrderedDict()
        metrics_rep['latest_current_assets_liabilities_ratio'] = div(
            b_s_data['total_current_assets'][0], b_s_data['total_current_liabilities'][0])
        metrics_rep['average_current_assets_liabilities_ratio_prev_0_4_y'] = div(
            period_rep['average_total_current_assets_prev_0_4_y'],
            period_rep['average_total_current_liabilities_prev_0_4_y']
        )
        metrics_rep['average_current_assets_liabilities_ratio_prev_5_9_y'] = div(
            period_rep['average_total_current_assets_prev_5_9_y'],
            period_rep['average_total_current_liabilities_prev_5_9_y']
        )
        metrics_rep['latest_total_assets_liabilities_ratio'] = div(
            b_s_data['total_assets'][0], b_s_data['total_liabilities'][0])
        metrics_rep['average_total_assets_liabilities_ratio_prev_0_4_y'] = div(
            period_rep['average_total_assets_prev_0_4_y'], period_rep['average_total_liabilities_prev_0_4_y']
        )
        metrics_rep['average_total_assets_liabilities_ratio_prev_5_9_y'] = div(
            period_rep['average_total_assets_prev_5_9_y'], period_rep['average_total_liabilities_prev_5_9_y']
        )
        metrics_rep['average_cash_current_assets_ratio_prev_0_4_y'] = div(
            period_rep['average_cash_on_hand_prev_0_4_y'], period_rep['average_total_current_assets_prev_0_4_y']
        )
        return period_rep, metrics_rep

    def generate_cash_flow_reports(self, c_f_data: Dict) -> Tuple[OrderedDict, OrderedDict]:
        """create report by periods and create metrics report from cash flow statement

        :param c_f_data: cash flow statement data
        :return: (report by periods, metrics report)
        """
        period_rep = self.create_report_by_period(c_f_data, self.report_by_period_schema['cash_flow_statement'])
        metrics_rep = OrderedDict()
        metrics_rep['linear_corrcoef_accounts_receivable_last_5y'] = flipped_linear_corrcoef(
            c_f_data['change_in_accounts_receivable'], 5)
        metrics_rep['linear_corrcoef_accounts_payable_last_5y'] = flipped_linear_corrcoef(
            c_f_data['change_in_accounts_payable'], 5)
        metrics_rep['linear_corrcoef_accounts_payable_last_5y'] = flipped_linear_corrcoef(
            c_f_data['change_in_accounts_payable'], 5)
        metrics_rep['linear_corrcoef_long_term_debt_last_5y'] = flipped_linear_corrcoef(
            c_f_data['net_long_term_debt'], 5)
        metrics_rep['linear_corrcoef_current_debt_last_5y'] = flipped_linear_corrcoef(
            c_f_data['net_current_debt'], 5)
        metrics_rep['linear_corrcoef_debt_issuance_last_5y'] = flipped_linear_corrcoef(
            c_f_data['debt_issuance_retirement_net_total'], 5)
        metrics_rep['linear_corrcoef_equity_issued_last_5y'] = flipped_linear_corrcoef(
            c_f_data['net_total_equity_issued_repurchased'], 5)
        metrics_rep['latest_dividend_net_income_ratio'] = div(
            reverse_sign(c_f_data['total_common_and_preferred_stock_dividends_paid'][0]),
            c_f_data['net_income_loss'][0])
        metrics_rep['average_dividend_net_income_ratio_prev_0_4_y'] = div(
            reverse_sign(period_rep['average_total_common_and_preferred_stock_dividends_paid_prev_0_4_y']),
            period_rep['average_net_income_loss_prev_0_4_y'])
        return period_rep, metrics_rep

    def generate_period_and_metric_report(self, stock_ticker) -> Tuple[OrderedDict, OrderedDict]:
        LOG.debug(f'generating {stock_ticker} report...')
        stock_data = self.get_stock_data(stock_ticker)

        LOG.debug('generating income statement reports...')
        i_s_data = stock_data['income_statement']
        i_s_period_rep, i_s_metrics_rep = self.generate_income_statement_reports(i_s_data)

        LOG.debug('generating balance sheet reports...')
        b_s_data = stock_data['balance_sheet']
        b_s_period_rep, b_s_metrics_rep = self.generate_balance_sheet_reports(b_s_data)

        LOG.debug('generating cash flow statement reports...')
        c_f_data = stock_data['cash_flow_statement']
        c_f_period_rep, c_f_metrics_rep = self.generate_cash_flow_reports(c_f_data)

        LOG.debug('generating price reports...')
        price_data = stock_data['price']
        price_period_rep = self.create_report_by_period(price_data, self.report_by_period_schema['price'])
        price_metrics_rep = OrderedDict()
        price_metrics_rep['latest_price'] = price_data['year_close'][0] if price_data['year_close'] else None
        price_metrics_rep['latest_eps'] = i_s_data['eps_earnings_per_share'][0]
        price_metrics_rep['latest_pe'] = div(price_metrics_rep['latest_price'], price_metrics_rep['latest_eps'])
        price_metrics_rep['average_pe_prev_0_4_y'] = div(
            price_period_rep['average_average_stock_price_prev_0_4_y'],
            i_s_period_rep['average_eps_earnings_per_share_prev_0_4_y']
        )
        price_metrics_rep['average_pe_prev_5_9_y'] = div(
            price_period_rep['average_average_stock_price_prev_5_9_y'],
            i_s_period_rep['average_eps_earnings_per_share_prev_5_9_y']
        )
        latest_bv = sub(b_s_data['share_holder_equity'][0], b_s_data['goodwill_and_intangible_assets'][0]) if \
            b_s_data['goodwill_and_intangible_assets'][0] else b_s_data['share_holder_equity'][0]
        price_metrics_rep['latest_bv_per_share'] = div(latest_bv, i_s_data['shares_outstanding'][0])
        price_metrics_rep['latest_pb'] = div(price_metrics_rep['latest_price'], price_metrics_rep['latest_bv_per_share'])

        avg_good_will_and_intangible = b_s_period_rep['average_goodwill_and_intangible_assets_prev_0_4_y'] if \
            b_s_period_rep['average_goodwill_and_intangible_assets_prev_0_4_y'] else 0
        average_bv_per_share_prev_0_4_y = div(
            sub(
                b_s_period_rep['average_share_holder_equity_prev_0_4_y'],
                avg_good_will_and_intangible
            ),
            i_s_period_rep['average_shares_outstanding_prev_0_4_y']
        )
        price_metrics_rep['average_pb_prev_0_4_y'] = div(
            price_period_rep['average_average_stock_price_prev_0_4_y'],
            average_bv_per_share_prev_0_4_y
        )
        price_metrics_rep['average_return_on_equity_prev_0_4_y'] = div(
            i_s_period_rep['average_net_income_prev_0_4_y'],
            b_s_period_rep['average_share_holder_equity_prev_0_4_y']
        )
        price_metrics_rep['average_return_on_equity_prev_0_4_y'] = div(
            i_s_period_rep['average_net_income_prev_0_4_y'], b_s_period_rep['average_share_holder_equity_prev_0_4_y']
        )
        price_metrics_rep['latest_dividend_per_share'] = div(
            reverse_sign(c_f_data['total_common_and_preferred_stock_dividends_paid'][0]),
            i_s_data['shares_outstanding'][0])
        price_metrics_rep['latest_dividend_yield'] = div(
            price_metrics_rep['latest_dividend_per_share'], price_metrics_rep['latest_price'])

        metrics_rep = OrderedDict(**{
            'ticker': stock_ticker,
            'country': stock_data['country'],
            'market_cap': stock_data['market_cap'],
            'industry': stock_data['industry']
        }, **i_s_metrics_rep, **b_s_metrics_rep, **c_f_metrics_rep, **price_metrics_rep)
        return metrics_rep

    @staticmethod
    def default_filter(
            report: Dict,
            market_cap_limit: float = 50,
            positive_avg_earning: bool = True,
            solid_liquidity: bool = True,
            positive_net_cash_flow: bool = True,
            compare_current_pe_vs_avg_last_5y: bool = True,
            group_avg: Dict = None,
    ):

        ticker = report['ticker']
        if check_lt(report['market_cap'], market_cap_limit):
            LOG.info(f'filtered {ticker}: Market cap < {market_cap_limit}B')
            return False
        if positive_avg_earning:
            if check_lt(report['avg_eps_prev_0_4_y'], 0) or check_lt(report['avg_eps_prev_5_9_y'], 0):
                LOG.info(f'filtered {ticker}: Either avg EPS prev 0-4y or 5-9y < 0')
                return False
        if solid_liquidity:
            if check_lt(report['latest_current_assets_liabilities_ratio'], 1) \
                    and check_lt(report['latest_total_assets_liabilities_ratio'], 1):
                LOG.info(f'filtered {ticker}: Both current and total assets/liabilities < 1')
                return False
            if check_lt(report['latest_price_book_value_ratio'], 0):
                LOG.info(f'filtered {ticker}: Negative book value')
                return False
            if check_lt(report['avg_price_book_value_ratio_prev_0_4_y'], 0):
                LOG.info(f'filtered {ticker}: Negative avg book value last 5 yrs')
                return False
        if positive_net_cash_flow:
            if check_lt(report['avg_net_cash_flow_prev_0_4_y'], 0):
                LOG.info(f'filtered {ticker}: Negative avg net cash flow last 5 years')
                return False

        if compare_current_pe_vs_avg_last_5y:
            if check_lt(report['avg_pe_prev_0_4_y'], report['latest_pe']):
                LOG.info(f'filtered {ticker}: Current P/E > avg. P/E last 5 years')
                return False

        if group_avg:
            if check_lt(group_avg['avg_pe_prev_0_4_y'], report['avg_pe_prev_0_4_y']) and \
                    check_lt(group_avg['avg_pe_prev_5_9_y'], report['avg_pe_prev_5_9_y']):
                LOG.info(f'filtered {ticker}: Avg P/E prev 0-4y and 5-9y > group avg')
                return False
            if check_lt(report['eps_growth_prev_0_4_vs_5_9'], group_avg['eps_growth_prev_0_4_vs_5_9']) and \
                    check_lt(report['eps_growth_prev_0_4_vs_10_14'], group_avg['eps_growth_prev_0_4_vs_10_14']):
                LOG.info(f'filtered {ticker}: Both EPS growth compared to prev 5-9y or prev 10-14y < group avg')
                return False
        return True

    @staticmethod
    def write_reports_to_csv(output_file_path: Path, fields: List[str], reports: List[OrderedDict]):
        LOG.debug(f'Writing {len(reports)} reports to {output_file_path}')
        with output_file_path.open('w') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for rep in reports:
                writer.writerow(rep)

    @staticmethod
    def create_group_average_report(reports: List[OrderedDict]):
        fields_name = list(reports[0].keys())
        avg_rep = OrderedDict({'ticker': 'average'})
        for field in fields_name:
            if field != 'ticker':
                try:
                    avg_rep[field] = apply_f_excl_none(np.average, [rep[field] for rep in reports])
                except Exception as e:
                    LOG.exception(f'fail to find average of `{field}`: {e}')
                    avg_rep[field] = None
        return avg_rep

    def create_reports_from_multiple_tickers(
            self,
            tickers: List,
            condensed_report: bool = True,
            auto_filter: bool = True
    ):
        """ create report from ticker and create an average of the grouped

        :param tickers:
        :param condensed_report:
        :param auto_filter:
        :return: list of report for each ticker and avg_report
        """
        reports = [self.generate_metrics_report(tkr) for tkr in tickers]
        avg_report = self.create_group_average_report(reports)
        if auto_filter:
            reports = [rep for rep in reports if self.default_filter(rep, group_avg=avg_report)]
        return reports, avg_report

    def write_sector_condensed_reports_to_csv(
            self,
            output_file_path: Path,
            sector: str,
            auto_filter: bool = True
    ):
        LOG.debug(f'writing {sector} report to {output_file_path}')
        sector_tickers = self.get_sector_tickers(sector)
        reports, avg_report = self.create_reports_from_multiple_tickers(sector_tickers, True, auto_filter)
        if reports:
            fields_name = list(reports[0].keys())
            self.write_reports_to_csv(output_file_path, fields_name, reports + [avg_report])
        else:
            LOG.info('No report remaining after filtering')

    def write_industry_condensed_reports_to_csv(
            self,
            output_file_path: Path,
            industry: str,
            auto_filter: bool = True
    ):
        LOG.debug(f'writing {industry} report to {output_file_path}')
        industry_tickers = self.get_industry_tickers(industry)
        reports, avg_report = self.create_reports_from_multiple_tickers(industry_tickers, True, auto_filter)
        if reports:
            fields_name = list(reports[0].keys())
            self.write_reports_to_csv(output_file_path, fields_name, reports + [avg_report])
        else:
            LOG.info('No report remaining after filtering')
