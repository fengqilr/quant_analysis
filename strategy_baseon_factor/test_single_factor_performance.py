# -*- coding: utf-8 -*-


import pandas as pd
from pymongo import MongoClient
import time
import os
from util import MongodbUtils

# Client = MongoClient(host='localhost', port=27017)
# db = Client['factor_db']
# price_collection = db['stock_price']
# quality_collection = db['stock_quality']

# price_table = MongodbUtils('factor_db', 'stock_price', '127.0.0.1', 27017)
# quality_table = MongodbUtils('factor_db', 'stock_quality', '127.0.0.1', 27017)

cash_data_dir = "/home/lianrui/quant/quant_analysis/cash_data"
def read_data(trade_date):
    s1 = time.time()
    trade_date_file_name = "%s.pkl" % trade_date
    print "start to read data %s" % trade_date
    cash_files = os.listdir(cash_data_dir)
    if trade_date_file_name in cash_files:
        index_df = pd.read_pickle(cash_data_dir + os.sep + trade_date_file_name)
    else:

        with MongodbUtils('factor_db', 'stock_price', '127.0.0.1', 27017) as price_collection:
            price_cur = price_collection.find({'trade_date': trade_date})
        # price_infos = list(price_cur)
            price_infos = [info for info in price_cur]
            price_df = pd.DataFrame(price_infos)

        end_date = price_df['end_date'].values[0]
        with MongodbUtils('factor_db', 'stock_quality', '127.0.0.1', 27017) as quality_collection:
            quality_cur = quality_collection.find({'end_date': end_date})
            quality_df = pd.DataFrame([info for info in quality_cur])
        index_df = pd.merge(quality_df, price_df, on='ts_code', how='inner')
        index_df['goodwillratio'].fillna(value=0, inplace=True)
        index_df.to_pickle(cash_data_dir + os.sep + trade_date_file_name)
    s2 = time.time()
    print "end to read data, cost %s" % (s2-s1)
    return index_df

def test_single_performance(factor_name, start_month, end_month, group_count):
    trade_dates = pd.read_pickle("trade_dates.pkl")
    # date_ranges = [trade_date for trade_date in trade_dates if trade_date >= start_date and trade_date <= end_date]
    trade_date_df = pd.DataFrame({'trade_date': trade_dates})
    trade_date_df['trade_month'] = trade_date_df['trade_date'].map(lambda x: x[:6])
    trade_date_df = trade_date_df[trade_date_df['trade_month'].map(lambda x: x <= end_month and x>=start_month)]
    month_perf_list = []
    for trade_month, trade_date_df in trade_date_df.groupby('trade_month'):
        first_date = trade_date_df['trade_date'].values[0]
        last_date = trade_date_df['trade_date'].values[-1]
        first_date_index_df = read_data(first_date)
        valid_index_df = first_date_index_df[pd.notna(first_date_index_df[factor_name])]
        invalid_index_df = first_date_index_df[pd.isna(first_date_index_df[factor_name])]
        valid_index_df.sort_values(by=factor_name, inplace=True)

        group_size = len(valid_index_df)/group_count
        valid_index_df['range'] = range(len(valid_index_df))
        valid_index_df['group'] = valid_index_df['range'].map(lambda x: min(x/group_size+1, group_count))
        # print valid_index_df[[factor_name, 'ts_code', 'group']].head(10)
        # print valid_index_df[[factor_name, 'ts_code', 'group']].tail(10)
        last_date_index_df = read_data(last_date)
        valid_index_df = pd.merge(valid_index_df[['ts_code', 'pivot', 'group']],
                                  last_date_index_df[['ts_code', 'pivot']], on='ts_code', how='inner')
        perf_dict = {}
        for group_name, group_price_df in valid_index_df.groupby('group'):

            group_price_df['return'] = group_price_df['pivot_y']/group_price_df['pivot_x']
            group_price_df.fillna(1, inplace=True)
            perf_dict.update({group_name: group_price_df['return'].mean()})

        if len(invalid_index_df):
            invalid_index_df = pd.merge(invalid_index_df[['ts_code', 'pivot']],
                                      last_date_index_df[['ts_code', 'pivot']], on='ts_code', how='inner')
            invalid_index_df['return'] = invalid_index_df['pivot_y']/invalid_index_df['pivot_x']
            perf_dict.update({'none': invalid_index_df['return'].mean()})
        else:
            perf_dict.update({'none': 1})
        # print perf_dict
        perf_dict.update({"trade_month": trade_month})
        month_perf_list.append(perf_dict)
    month_perf_df = pd.DataFrame(month_perf_list)
    month_perf_df.fillna(1, inplace=True)
    return month_perf_df

perf_df = test_single_performance('roe_std', '200707', '201806', 4)
# perf_df.to_excel('roe_std_perf.xls')
print perf_df.prod()

    # for idx, trade_date in enumerate(date_ranges):
    #     if idx and idx < len(date_ranges)-1:
    #         last_trade_date = date_ranges[idx-1]
    #         next_trade_date = date_ranges[idx+1]
    #         this_month = trade_date[4:6]
    #         last_month = trade_date[4: 6]
    #         next_month = trade_date[4:6]
    #         if this_month != last_month:
    #             price_cur = price_collection.find({'trade_date': trade_date})
    #             # price_infos = list(price_cur)
    #             price_infos = [info for info in price_cur]
    #             price_df = pd.DataFrame(price_infos)
    #
    #             end_date = price_df['end_date'].values[0]
    #
    #             quality_cur = quality_collection.find({'end_date': end_date})
    #             quality_df = pd.DataFrame([info for info in quality_cur])
    #             index_df = pd.merge(quality_df, price_df, on='ts_code', how='inner')