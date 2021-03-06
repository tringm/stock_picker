from pathlib import Path
import json
import logging
from typing import Dict, List, Tuple, Iterable, Optional
import numpy as np
from collections import OrderedDict
import csv

LOG = logging.getLogger('Picker')


def apply_f_excl_none(func, array):
    array_excl_none = [item for item in array if item is not None and not np.isnan(item)]
    if not array_excl_none:
        return None
    else:
        return func(array_excl_none)


def div(item1, item2):
    if item1 is None or item2 is None:
        return None
    if item2 == 0 or (item1 < 0 and item2 < 0):
        return None
    return item1 / item2


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
    array_excl_none = [item for item in arr if item is not None and not np.isnan(item)]
    avg = np.average(array_excl_none)
    if avg == 0:
        return None
    try:
        return np.std(array_excl_none) / avg
    except Exception:
        return None


def change_rate(item1, item2):
    if item2 == 0:
        return None
    try:
        return (item1 - item2)/item2
    except Exception:
        return None


def check_lt(num1, num2):
    if num1 is not None and not np.isnan(num1) and num2 is not None and num1 < num2:
        return True
    return False


def check_lte(num1, num2):
    if num1 is not None and not np.isnan(num1) and num2 is not None and num1 <= num2:
        return True
    return False


def flipped_linear_corrcoef(arr, n_data_points):
    arr_excl_none = [item for item in arr if item is not None]
    if len(arr_excl_none) < n_data_points:
        return None
    flipped_arr = np.flip(arr_excl_none[:n_data_points])
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
                'operating_income': None,
                'pre_tax_income': None,
                'net_income': None,
                'total_non_operating_income_expense': None,
                'research_and_development_expenses': None,
                'shares_outstanding': None,
                'operating_expenses': None,
                'eps_earnings_per_share': {
                    'periods': [(0, 4), (5, 9), (10, 14)],
                    'functions': [np.average]
                }
            }),
            'balance_sheet': OrderedDict({
                'cash_on_hand': None,
                'share_holder_equity': None,
                'goodwill_and_intangible_assets': None,
                'inventory': None,
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
                'net_cash_flow': None,
                'debt_issuance_retirement_net_total': None,
                'net_total_equity_issued_repurchased': None,
            },
            'price': {
                'average_stock_price': {
                    'periods': [(0, 4), (5, 9)],
                    'functions': [np.average]
                }
            }
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
            req_field_data = stock_data[req_field]
            if len(req_field_data['years']) < 10:
                raise ValueError(f'{req_field} has less than 5 data points')
            try:
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
        period_rep['corr_coef_revenue_last_5y'] = flipped_linear_corrcoef(i_s_data['revenue'], 5)
        period_rep['corr_coef_eps_last_5y'] = flipped_linear_corrcoef(i_s_data['eps_earnings_per_share'], 5)
        period_rep['corr_coef_shares_outstanding_last_5y'] = flipped_linear_corrcoef(
            i_s_data['shares_outstanding'], 5)
        period_rep['corr_coef_op_expenses_margin_last_5y'] = flipped_linear_corrcoef(
            [div(g_p, i_s_data['revenue'][idx]) for idx, g_p in enumerate(i_s_data['operating_expenses'])], 5)
        metrics_rep['average_op_expenses_margin_prev_0_4_y'] = div(
            period_rep['average_operating_expenses_prev_0_4_y'], period_rep['average_revenue_prev_0_4_y']
        )
        period_rep['corr_coef_gross_profit_margin_last_5y'] = flipped_linear_corrcoef(
            [div(g_p, i_s_data['revenue'][idx]) for idx, g_p in enumerate(i_s_data['gross_profit'])], 5)
        metrics_rep['average_gross_profit_margin_prev_0_4_y'] = div(
            period_rep['average_gross_profit_prev_0_4_y'], period_rep['average_revenue_prev_0_4_y']
        )
        period_rep['corr_coef_net_income_margin_last_5y'] = flipped_linear_corrcoef(
            [div(inc, i_s_data['revenue'][idx]) for idx, inc in enumerate(i_s_data['net_income'])], 5)
        metrics_rep['average_net_income_margin_prev_0_4_y'] = div(
            period_rep['average_net_income_prev_0_4_y'], period_rep['average_revenue_prev_0_4_y']
        )
        period_rep['corr_coef_pre_tax_net_income_margin_last_5y'] = flipped_linear_corrcoef(
            [div(inc, i_s_data['revenue'][idx]) for idx, inc in enumerate(i_s_data['pre_tax_income'])], 5)
        metrics_rep['average_pre_tax_net_income_margin_prev_0_4_y'] = div(
            period_rep['average_pre_tax_income_prev_0_4_y'], period_rep['average_revenue_prev_0_4_y']
        )
        metrics_rep['eps_growth_prev_0_4_vs_5_9_y'] = change_rate(
            period_rep['average_eps_earnings_per_share_prev_0_4_y'],
            period_rep['average_eps_earnings_per_share_prev_5_9_y'])
        if period_rep['average_eps_earnings_per_share_prev_0_4_y'] < 0 and \
                period_rep['average_eps_earnings_per_share_prev_5_9_y']:
            metrics_rep['eps_growth_prev_0_4_vs_5_9_y'] = -metrics_rep['eps_growth_prev_0_4_vs_5_9_y']
        metrics_rep['eps_growth_prev_0_4_vs_10_14_y'] = change_rate(
            period_rep['average_eps_earnings_per_share_prev_0_4_y'],
            period_rep['average_eps_earnings_per_share_prev_10_14_y'])
        if period_rep['average_eps_earnings_per_share_prev_0_4_y'] < 0 \
                and period_rep['average_eps_earnings_per_share_prev_5_9_y']:
            metrics_rep['eps_growth_prev_0_4_vs_10_14_y'] = metrics_rep['eps_growth_prev_0_4_vs_10_14_y']
        metrics_rep['average_r_and_d_expenses_ratio_prev_0_4_y'] = div(
            period_rep['average_research_and_development_expenses_prev_0_4_y'],
            period_rep['average_operating_expenses_prev_0_4_y']
        )
        metrics_rep['average_non_op_per_op_expense_prev_0_4_y'] = div(
            abs(period_rep['average_total_non_operating_income_expense_prev_0_4_y']),
            period_rep['average_net_income_prev_0_4_y']
        ) if (
            period_rep['average_total_non_operating_income_expense_prev_0_4_y'] and
            period_rep['average_total_non_operating_income_expense_prev_0_4_y'] < 0
        ) else None
        return period_rep, metrics_rep

    def generate_balance_sheet_reports(self, b_s_data: Dict) -> Tuple[OrderedDict, OrderedDict]:
        """create report by periods and create metrics report from balance sheet

        :param b_s_data: balance sheet data
        :return: (report by periods, metrics report)
        """
        period_rep = self.create_report_by_period(b_s_data, self.report_by_period_schema['balance_sheet'])
        metrics_rep = OrderedDict({
            'latest_current_assets_liabilities_ratio': div(
                b_s_data['total_current_assets'][0], b_s_data['total_current_liabilities'][0]
            ),
            'average_current_assets_liabilities_ratio_prev_0_4_y': div(
                period_rep['average_total_current_assets_prev_0_4_y'],
                period_rep['average_total_current_liabilities_prev_0_4_y']
            ),
            'average_current_assets_liabilities_ratio_prev_5_9_y': div(
                period_rep['average_total_current_assets_prev_5_9_y'],
                period_rep['average_total_current_liabilities_prev_5_9_y']
            ),
            'latest_total_assets_liabilities_ratio': div(
                b_s_data['total_assets'][0], b_s_data['total_liabilities'][0]
            ),
            'average_total_assets_liabilities_ratio_prev_0_4_y': div(
                period_rep['average_total_assets_prev_0_4_y'], period_rep['average_total_liabilities_prev_0_4_y']
            ),
            'average_total_assets_liabilities_ratio_prev_5_9_y': div(
                period_rep['average_total_assets_prev_5_9_y'], period_rep['average_total_liabilities_prev_5_9_y']
            ),
            'average_cash_total_liabilities_prev_0_4_y': div(
                period_rep['average_cash_on_hand_prev_0_4_y'],
                period_rep['average_total_current_liabilities_prev_0_4_y']
            ),
            'average_inventory_current_assets_ratio_prev_0_4_y': div(
                period_rep['average_inventory_prev_0_4_y'],
                period_rep['average_total_current_assets_prev_0_4_y']
            )
        })
        period_rep['corr_coef_current_assets_liabilities_last_5y'] = flipped_linear_corrcoef([
            div(b_s_data['total_current_assets'][idx], equity)
            for idx, equity in enumerate(b_s_data['total_current_liabilities'])], 5
        )
        period_rep['corr_coef_total_assets_liabilities_last_5y'] = flipped_linear_corrcoef([
            div(b_s_data['total_assets'][idx], equity)
            for idx, equity in enumerate(b_s_data['total_liabilities'])], 5
        )
        return period_rep, metrics_rep

    def generate_cash_flow_reports(self, c_f_data: Dict) -> Tuple[OrderedDict, OrderedDict]:
        """create report by periods and create metrics report from cash flow statement

        :param c_f_data: cash flow statement data
        :return: (report by periods, metrics report)
        """
        period_rep = self.create_report_by_period(c_f_data, self.report_by_period_schema['cash_flow_statement'])
        period_rep['corr_coef_debt_issuance_last_5y'] = flipped_linear_corrcoef(
            c_f_data['debt_issuance_retirement_net_total'], 5)
        period_rep['corr_coef_equity_issued_last_5y'] = flipped_linear_corrcoef(
            c_f_data['net_total_equity_issued_repurchased'], 5)
        period_rep['corr_coef_net_income_per_op_cash_flow'] = flipped_linear_corrcoef(
            [div(net_inc, c_f_data['cash_flow_from_operating_activities'][idx])
             for idx, net_inc in enumerate(c_f_data['net_income_loss'])], 5)

        metrics_rep = OrderedDict({
            'average_debt_issuance_per_net_income_prev_0_4_y': div(
                period_rep['average_debt_issuance_retirement_net_total_prev_0_4_y'],
                period_rep['average_net_income_loss_prev_0_4_y']
            ) if (
                period_rep['average_debt_issuance_retirement_net_total_prev_0_4_y'] and
                period_rep['average_debt_issuance_retirement_net_total_prev_0_4_y'] > 0
            ) else None,
            'average_equity_issued_per_net_income_prev_0_4_y': div(
                period_rep['average_net_total_equity_issued_repurchased_prev_0_4_y'],
                period_rep['average_net_income_loss_prev_0_4_y']
            ) if (
                period_rep['average_net_total_equity_issued_repurchased_prev_0_4_y'] and
                period_rep['average_net_total_equity_issued_repurchased_prev_0_4_y'] > 0
            ) else None,
            'latest_dividend_net_income_ratio': div(
                reverse_sign(c_f_data['total_common_and_preferred_stock_dividends_paid'][0]),
                c_f_data['net_income_loss'][0]
            ),
            'average_dividend_net_income_ratio_prev_0_4_y':  div(
                reverse_sign(period_rep['average_total_common_and_preferred_stock_dividends_paid_prev_0_4_y']),
                period_rep['average_net_income_loss_prev_0_4_y']
            )
        })
        return period_rep, metrics_rep

    def generate_price_reports(
            self, price_data, i_s_data, b_s_data, c_f_data, i_s_period_rep, b_s_period_rep, c_f_period_rep
    ) -> Tuple[OrderedDict, OrderedDict]:
        price_period_rep = self.create_report_by_period(price_data, self.report_by_period_schema['price'])
        price_metrics_rep = OrderedDict()
        price_metrics_rep['latest_price'] = price_data['year_close'][0] if price_data['year_close'] else None

        # p/e related
        price_metrics_rep['latest_eps'] = i_s_data['eps_earnings_per_share'][0]
        price_metrics_rep['latest_pe'] = div(price_metrics_rep['latest_price'], price_metrics_rep['latest_eps'])
        price_metrics_rep['average_eps_prev_0_4_y'] = i_s_period_rep['average_eps_earnings_per_share_prev_0_4_y']
        price_metrics_rep['latest_p_average_e_prev_0_4_y'] = div(
            price_metrics_rep['latest_price'], i_s_period_rep['average_eps_earnings_per_share_prev_0_4_y'])
        price_metrics_rep['average_pe_prev_0_4_y'] = div(
            price_period_rep['average_average_stock_price_prev_0_4_y'],
            i_s_period_rep['average_eps_earnings_per_share_prev_0_4_y']
        )
        price_metrics_rep['average_pe_prev_5_9_y'] = div(
            price_period_rep['average_average_stock_price_prev_5_9_y'],
            i_s_period_rep['average_eps_earnings_per_share_prev_5_9_y']
        )

        # p/cash related
        price_metrics_rep['latest_cash_per_share'] = div(
            b_s_data['cash_on_hand'][0], i_s_data['shares_outstanding'][0])
        price_metrics_rep['latest_p_cash'] = div(
            price_metrics_rep['latest_price'], price_metrics_rep['latest_cash_per_share']
        )
        price_metrics_rep['average_cash_per_share_prev_0_4_y'] = div(
            b_s_period_rep['average_cash_on_hand_prev_0_4_y'], i_s_period_rep['average_shares_outstanding_prev_0_4_y'])
        price_metrics_rep['latest_p_average_cash_prev_0_4_y'] = div(
            price_metrics_rep['latest_price'], price_metrics_rep['average_cash_per_share_prev_0_4_y'])
        price_metrics_rep['average_p_cash_prev_0_4_y'] = div(
            price_period_rep['average_average_stock_price_prev_0_4_y'],
            price_metrics_rep['average_cash_per_share_prev_0_4_y']
        )
        price_metrics_rep['average_net_cash_flow_per_share_prev_0_4_y'] = div(
            c_f_period_rep['average_net_cash_flow_prev_0_4_y'], i_s_period_rep['average_shares_outstanding_prev_0_4_y'])
        price_metrics_rep['latest_p_net_cash_flow_prev_0_4_y'] = div(
            price_metrics_rep['latest_price'], price_metrics_rep['average_net_cash_flow_per_share_prev_0_4_y'])
        price_metrics_rep['average_p_net_cash_flow_prev_0_4_y'] = div(
            price_period_rep['average_average_stock_price_prev_0_4_y'],
            price_metrics_rep['average_net_cash_flow_per_share_prev_0_4_y']
        )

        # p/bv related
        latest_bv = sub(b_s_data['share_holder_equity'][0], b_s_data['goodwill_and_intangible_assets'][0]) if \
            b_s_data['goodwill_and_intangible_assets'][0] else b_s_data['share_holder_equity'][0]
        price_metrics_rep['latest_bv_per_share'] = div(latest_bv, i_s_data['shares_outstanding'][0])
        price_metrics_rep['latest_p_bv'] = div(
            price_metrics_rep['latest_price'], price_metrics_rep['latest_bv_per_share'])
        avg_intangible = b_s_period_rep['average_goodwill_and_intangible_assets_prev_0_4_y'] if \
            b_s_period_rep['average_goodwill_and_intangible_assets_prev_0_4_y'] else 0
        price_metrics_rep['average_bv_per_share_prev_0_4_y'] = div(
            sub(b_s_period_rep['average_share_holder_equity_prev_0_4_y'], avg_intangible),
            i_s_period_rep['average_shares_outstanding_prev_0_4_y']
        )
        price_metrics_rep['latest_p_average_bv_prev_0_4_y'] = div(
            price_metrics_rep['latest_price'], price_metrics_rep['average_bv_per_share_prev_0_4_y'])
        price_metrics_rep['average_p_bv_prev_0_4_y'] = div(
            price_period_rep['average_average_stock_price_prev_0_4_y'],
            price_metrics_rep['average_bv_per_share_prev_0_4_y']
        )

        price_metrics_rep['average_ROA_prev_0_4_y'] = div(
            i_s_period_rep['average_net_income_prev_0_4_y'], b_s_period_rep['average_total_assets_prev_0_4_y']
        )
        price_period_rep['corr_coef_ROA_last_5y'] = flipped_linear_corrcoef([
            div(i_s_data['net_income'][idx], assets) for idx, assets in enumerate(b_s_data['total_assets'])], 5
        )
        price_metrics_rep['average_ROE_prev_0_4_y'] = div(
            i_s_period_rep['average_net_income_prev_0_4_y'], b_s_period_rep['average_share_holder_equity_prev_0_4_y']
        )
        price_period_rep['corr_coef_ROE_last_5y'] = flipped_linear_corrcoef([
            div(i_s_data['net_income'][idx], s_e) for idx, s_e in enumerate(b_s_data['share_holder_equity'])], 5
        )
        price_metrics_rep['average_ROEC_prev_0_4_y'] = div(
            i_s_period_rep['average_net_income_prev_0_4_y'],
            sub(b_s_period_rep['average_total_assets_prev_0_4_y'], b_s_period_rep['average_total_liabilities_prev_0_4_y'])
        )
        price_period_rep['corr_coef_ROEC_last_5y'] = flipped_linear_corrcoef([
            div(n_i, sub(b_s_data['total_assets'][idx], b_s_data['total_liabilities'][idx]))
            for idx, n_i in enumerate(i_s_data['net_income'])], 5
        )
        price_metrics_rep['latest_dividend_per_share'] = div(
            reverse_sign(c_f_data['total_common_and_preferred_stock_dividends_paid'][0]),
            i_s_data['shares_outstanding'][0]
        )
        price_metrics_rep['latest_dividend_yield'] = div(
            price_metrics_rep['latest_dividend_per_share'], price_metrics_rep['latest_price']
        )
        return price_period_rep, price_metrics_rep

    def generate_period_and_metrics_report(self, stock_ticker) -> Tuple[OrderedDict, OrderedDict]:

        LOG.debug(f'generating {stock_ticker} period and metrics report')
        stock_data = self.get_stock_data(stock_ticker)

        i_s_data = stock_data['income_statement']
        i_s_period_rep, i_s_metrics_rep = self.generate_income_statement_reports(i_s_data)

        b_s_data = stock_data['balance_sheet']
        b_s_period_rep, b_s_metrics_rep = self.generate_balance_sheet_reports(b_s_data)

        c_f_data = stock_data['cash_flow_statement']
        c_f_period_rep, c_f_metrics_rep = self.generate_cash_flow_reports(c_f_data)

        price_data = stock_data['price']
        price_period_rep, price_metrics_rep = self.generate_price_reports(
            price_data, i_s_data, b_s_data, c_f_data, i_s_period_rep, b_s_period_rep, c_f_period_rep)

        other_metrics = OrderedDict({
            'average_net_cash_flow_per_market_cap_prev_0_4_y': div(
                price_metrics_rep['average_net_cash_flow_per_share_prev_0_4_y'], stock_data['market_cap']
            ),
            'average_solvency_ratio_prev_0_4_y': div(
                sub(
                    i_s_period_rep['average_net_income_prev_0_4_y'],
                    i_s_period_rep['average_total_non_operating_income_expense_prev_0_4_y']
                ),
                b_s_period_rep['average_total_liabilities_prev_0_4_y']
            ),

            'average_cash_flow_margin_prev_0_4_y': div(
                c_f_period_rep['average_cash_flow_from_operating_activities_prev_0_4_y'],
                i_s_period_rep['average_revenue_prev_0_4_y']
            )
        })

        other_period_metrics = OrderedDict({
            'corr_coef_solvency_ratio_last_5y': flipped_linear_corrcoef(
                [div(sub(i_s_data['net_income'][idx], i_s_data['total_non_operating_income_expense'][idx]), t_l)
                 for idx, t_l in enumerate(b_s_data['total_liabilities'])],
                5
            ),
            'corr_coef_cash_flow_margin_last_5y': flipped_linear_corrcoef(
                [div(c_f, i_s_data['revenue'][idx])
                 for idx, c_f in enumerate(c_f_data['cash_flow_from_operating_activities'])],
                5
            )
        })

        metrics_rep = OrderedDict(**{
            'ticker': stock_ticker,
            'country': stock_data['country'],
            'market_cap': stock_data['market_cap'],
            'industry': stock_data['industry']
        }, **i_s_metrics_rep, **b_s_metrics_rep, **c_f_metrics_rep, **other_metrics, **price_metrics_rep)
        period_rep = OrderedDict(
            **{'ticker': stock_ticker}, **i_s_period_rep, **b_s_period_rep, **c_f_period_rep, **price_period_rep,
            **other_period_metrics)
        return period_rep, metrics_rep

    @staticmethod
    def default_filter(
            period_report: Dict,
            metrics_report: Dict,
            market_cap: int = 100,
            filtering_country: Iterable[str] = (
                'argentina', 'australia', 'bermuda', 'brazil', 'chile', 'china', 'columbia',
                'hong_kong_sar_china', 'india', 'indonesia', 'israel', 'japan', 'mexico', 'russia', 'south_africa',
                'hong_kong, _sar_china'
            ),
            positive_avg_earning: bool = True,
            positive_cash_on_hand: bool = True,
            solid_liquidity: bool = True,
            null_data_ratio: float = 0.8,
    ):
        ticker = metrics_report['ticker']
        if metrics_report['market_cap'] < market_cap:
            LOG.info(f'filtered {ticker}: Market cap < {market_cap}')
            return False
        if filtering_country:
            if metrics_report['country'] in filtering_country:
                LOG.info(f'filtered {ticker}: Filtered country: {metrics_report["country"]}')
                return False
        if positive_avg_earning:
            if check_lt(period_report['average_eps_earnings_per_share_prev_0_4_y'], 0) or \
                    check_lt(period_report['average_eps_earnings_per_share_prev_5_9_y'], 0):
                LOG.info(f'filtered {ticker}: Either avg EPS prev 0-4y or 5-9y < 0')
                return False
        if positive_cash_on_hand:
            if check_lt(period_report['average_cash_on_hand_prev_0_4_y'], 0):
                LOG.info(f'filtered {ticker}: Negative cash on hand')
        if solid_liquidity:
            if check_lt(metrics_report['latest_current_assets_liabilities_ratio'], 1) or \
                    check_lt(metrics_report['average_current_assets_liabilities_ratio_prev_0_4_y'], 1):
                LOG.info(f'filtered {ticker}: Latest or avg. current assets/liabilities last 5 years < 1')
                return False
            if check_lt(metrics_report['latest_total_assets_liabilities_ratio'], 1) or \
                    check_lt(metrics_report['average_total_assets_liabilities_ratio_prev_0_4_y'], 1):
                LOG.info(f'filtered {ticker}: Latest or avg. total assets/liabilities  last 5 years < 1')
                return False
            if check_lt(metrics_report['latest_p_bv'], 0) or check_lt(metrics_report['average_p_bv_prev_0_4_y'], 0):
                LOG.info(f'filtered {ticker}: Negative book value')
                return False
        if len([val for val in metrics_report.values()
                if not val or (not isinstance(val, str) and np.isnan(val))])/len(metrics_report) > null_data_ratio:
            LOG.info(f'filtered {ticker}: More than 80% data is null')
            return False
        return True

    @staticmethod
    def outlier_filter(metrics_report: Dict, group_avg: Dict, group_std: Dict):
        """filter report outside of avg +- std

        :param metrics_report:
        :param group_avg:
        :param group_std:
        :return:
        """
        ticker = metrics_report['ticker']
        for field in (
            'eps_growth_prev_0_4_vs_5_9_y',
            'average_current_assets_liabilities_ratio_prev_0_4_y', 'average_total_assets_liabilities_ratio_prev_0_4_y',
            'average_cash_total_liabilities_prev_0_4_y', 'average_solvency_ratio_prev_0_4_y',
            'average_gross_profit_margin_prev_0_4_y', 'average_net_income_margin_prev_0_4_y',
            'average_pre_tax_net_income_margin_prev_0_4_y',
            'average_ROA_prev_0_4_y', 'average_ROE_prev_0_4_y', 'average_ROEC_prev_0_4_y',
            'average_cash_flow_margin_prev_0_4_y', 'average_net_cash_flow_per_market_cap_prev_0_4_y',

        ):
            if check_lt(metrics_report[field], group_avg[field] - 0.675 * group_std[field]):
                LOG.info(f'filtered {ticker}: {field}: {metrics_report[field]} '
                         f'< group avg - std: {group_avg[field] - 0.675 * group_std[field]}')
                return False
        for field in (
            'average_op_expenses_margin_prev_0_4_y', 'average_non_op_per_op_expense_prev_0_4_y',
            'average_pe_prev_0_4_y', 'average_p_cash_prev_0_4_y', 'average_p_bv_prev_0_4_y',
            'average_dividend_net_income_ratio_prev_0_4_y', 'average_debt_issuance_per_net_income_prev_0_4_y',
            'average_equity_issued_per_net_income_prev_0_4_y', 'average_inventory_current_assets_ratio_prev_0_4_y'
        ):
            if check_lt(group_avg[field] + 0.675 * group_std[field], metrics_report[field]):
                LOG.info(f'filtered {ticker}: {field}: {metrics_report[field]} '
                         f'> group avg + std: {group_avg[field] + 0.675 * group_std[field]}')
                return False

        return True

    @staticmethod
    def add_warnings_to_metrics_rep(period_reports: Dict, metrics_report: Dict) -> Dict:
        """return metrics report with warnings

        :param period_reports:
        :param metrics_report:
        :return:
        """
        warnings = []
        for field in (
            'revenue', 'eps',
            'current_assets_liabilities', 'total_assets_liabilities', 'solvency_ratio',
            'net_income_margin', 'gross_profit_margin', 'pre_tax_net_income_margin', 'cash_flow_margin',
            'ROA', 'ROE', 'ROEC'
        ):
            if check_lte(period_reports[f'corr_coef_{field}_last_5y'], -0.71):
                warnings.append(f'strong downward trend of {field} in last 5yrs')
        for field in ('op_expenses_margin', 'shares_outstanding', 'debt_issuance', 'equity_issued'):
            if check_lte(0.71, period_reports[f'corr_coef_{field}_last_5y']):
                warnings.append(f'strong upward trend of {field} in last 5yrs')
        metrics_report['warnings'] = '; '.join(warnings)
        return metrics_report

    @staticmethod
    def write_reports_to_csv(output_file_path: Path, fields: List[str], reports: List[Optional[Dict]]):
        LOG.debug(f'Writing {len(reports)} reports to {output_file_path}')
        with output_file_path.open('w') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for rep in reports:
                if rep:
                    writer.writerow(rep)

    @staticmethod
    def create_group_average_report(reports: List[Dict]) -> Optional[Dict]:
        if not reports:
            return None
        fields_name = list(reports[0].keys())
        avg_rep = {'ticker': 'average'}
        for field in fields_name:
            if field not in ['ticker', 'country', 'industry']:
                try:
                    avg_rep[field] = apply_f_excl_none(np.average, [rep[field] for rep in reports])
                except Exception as e:
                    LOG.exception(f'fail to find average of `{field}`: {e}')
                    avg_rep[field] = None
        LOG.info(f'average_report: {avg_rep}')
        return avg_rep

    @staticmethod
    def create_group_std_report(reports: List[Dict]) -> Dict:
        fields_name = list(reports[0].keys())
        std_rep = {'ticker': 'std'}
        for field in fields_name:
            if field != 'ticker':
                try:
                    std_rep[field] = apply_f_excl_none(np.std, [rep[field] for rep in reports])
                except Exception as e:
                    LOG.exception(f'fail to find std of `{field}`: {e}')
                    std_rep[field] = None
        LOG.info(f'std_report: {std_rep}')
        return std_rep

    def create_reports_from_multiple_tickers(
            self,
            tickers: List,
            auto_filter: bool = True,
            unfiltered_period_report_out_file_path: Path = None,
            unfiltered_metrics_report_out_file_path: Path = None,
            filtered_period_report_out_file_path: Path = None,
            filtered_metrics_report_out_file_path: Path = None
    ) -> Optional[List[Tuple[OrderedDict, OrderedDict]]]:
        """create period and metric reports from multiple tickers and an average of the group
        :param tickers: list of tickers
        :param auto_filter: filter the group with the default filter:
        :param unfiltered_period_report_out_file_path: output file for unfiltered period report
        :param unfiltered_metrics_report_out_file_path: output file for unfiltered metrics report
        :param filtered_period_report_out_file_path: output file for filtered period reports
        :param filtered_metrics_report_out_file_path: output file for filtered metrics report
        :return:
        """
        p_and_m_reps = [self.generate_period_and_metrics_report(tkr) for tkr in tickers]
        if unfiltered_period_report_out_file_path:
            period_reports = [rep[0] for rep in p_and_m_reps]
            fields_name = list(period_reports[0].keys())
            self.write_reports_to_csv(unfiltered_period_report_out_file_path, fields_name, period_reports)
        if unfiltered_metrics_report_out_file_path:
            metrics_reports = [rep[1] for rep in p_and_m_reps]
            fields_name = list(metrics_reports[0].keys())
            self.write_reports_to_csv(unfiltered_metrics_report_out_file_path, fields_name, metrics_reports)

        if auto_filter:
            p_and_m_reps = [rep for rep in p_and_m_reps if self.default_filter(rep[0], rep[1])]
            if p_and_m_reps:
                filtered_group_avg = self.create_group_average_report([rep[1] for rep in p_and_m_reps])
                filtered_group_std = self.create_group_std_report([rep[1] for rep in p_and_m_reps])
                p_and_m_reps = [(rep[0], self.add_warnings_to_metrics_rep(rep[0], rep[1])) for rep in p_and_m_reps if
                                self.outlier_filter(rep[1], filtered_group_avg, filtered_group_std)]
        if not p_and_m_reps:
            LOG.info('No report remained after filtering')
            return None

        if filtered_period_report_out_file_path:
            period_reports = [rep[0] for rep in p_and_m_reps]
            fields_name = list(period_reports[0].keys())
            self.write_reports_to_csv(filtered_period_report_out_file_path, fields_name, period_reports)
        if filtered_metrics_report_out_file_path:
            metrics_reports = [rep[1] for rep in p_and_m_reps]
            fields_name = list(metrics_reports[0].keys())
            self.write_reports_to_csv(filtered_metrics_report_out_file_path, fields_name, metrics_reports)
        return p_and_m_reps
