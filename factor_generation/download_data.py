# -*- coding: utf-8 -*-

import tushare as ts
import pandas as pd
ts_api = ts.pro_api('38590c7cd96615fe29f02d4d72fe76bbb6224155d87b2528a62c99a7')


def download_kline_data(stock_code):
    df = ts.pro_bar(pro_api=ts_api, ts_code=stock_code, asset='E', start_date='20010101', end_date='20181011',
                    factors=['tor', 'vr'], adj='qfq')
    df.to_pickle("./market_kline/%s.pkl" % stock_code)


def download_financial_data(stock_code):
    income_df = ts_api.income(ts_code=stock_code, start_date='20010101', end_date='20181030')
    balancesheet_df = ts_api.balancesheet(ts_code=stock_code, start_date='20010101', end_date='20181030')
    cashflow_df = ts_api.cashflow(ts_code=stock_code, start_date='20010101', end_date='20181030')
    financial_index_df = ts_api.fina_indicator(ts_code=stock_code, start_date='20010101', end_date='20181030')
    income_df.to_pickle("./income/%s.pkl" % stock_code)
    balancesheet_df.to_pickle("./balancesheet/%s.pkl" % stock_code)
    # print balancesheet_df
    cashflow_df.to_pickle("./cashflow/%s.pkl" % stock_code)
    financial_index_df.to_pickle("./financial_index/%s.pkl" % stock_code)





download_kline_data('000001.SZ')
download_kline_data('000002.SZ')
download_kline_data('000858.SZ')
download_kline_data('600519.SH')
download_kline_data('601398.SH')
# df = pd.read_pickle("./market_kline/000001.SZ.pkl")
# print df.tail(1).T
# download_financial_data('000858.SZ')
# download_financial_data('000002.SZ')