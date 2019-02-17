# -*- coding: utf-8 -*-


import pandas as pd
from pymongo import MongoClient
import time
import os
from util import MongodbUtils, read_data

# Client = MongoClient(host='localhost', port=27017)
# db = Client['factor_db']
# price_collection = db['stock_price']
# quality_collection = db['stock_quality']

# price_table = MongodbUtils('factor_db', 'stock_price', '127.0.0.1', 27017)
# quality_table = MongodbUtils('factor_db', 'stock_quality', '127.0.0.1', 27017)


basic_df = pd.read_pickle("../data/basic_info.pkl")
terminated_stock_df = pd.read_pickle("../data/terminated_stock_info.pkl")
stock_listdate_df = pd.concat([basic_df[['ts_code', 'list_date']], terminated_stock_df[['ts_code', 'list_date']]])
stock_2_listdate = dict(zip(stock_listdate_df['ts_code'].values, stock_listdate_df['list_date'].values))




def cal_months_between_dates(begin_date, end_date):

    begin_year = int(begin_date[:4])
    begin_month = int(begin_date[4: 6])
    end_year = int(end_date[: 4])
    end_month = int(end_date[4: 6])

    last_months = (end_year-begin_year)*12 + (end_month-begin_month)
    
    return last_months
       

def get_market_year(stock_name, stock_listdate_info, trade_date):
    stock_listdate = stock_listdate_info[stock_name]
    market_month = cal_months_between_dates(stock_listdate, trade_date)
    market_year = market_month/12
    return market_year


def test_single_performance(factor_name, start_month, end_month, group_count, stock_listdate_info):
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
        s1 = time.time()
        first_date_index_df = first_date_index_df[first_date_index_df['market_year'] > 3]
        valid_index_df = first_date_index_df[pd.notna(first_date_index_df[factor_name])]
        invalid_index_df = first_date_index_df[pd.isna(first_date_index_df[factor_name])]
        valid_index_df.sort_values(by=factor_name, inplace=True)

        group_size = int(len(valid_index_df)/group_count)
#         valid_index_df['range'] = range(len(valid_index_df))
        valid_index_df.insert(0, 'range', range(len(valid_index_df)))
        valid_index_df.insert(0, 'group', valid_index_df['range'].map(lambda x: min(int(x/group_size)+1, group_count)))
#         valid_index_df['group'] = valid_index_df['range'].map(lambda x: min(x/group_size+1, group_count))
        # print valid_index_df[[factor_name, 'ts_code', 'group']].head(10)
        # print valid_index_df[[factor_name, 'ts_code', 'group']].tail(10)
        s2 = time.time()
        last_date_index_df = read_data(last_date)
        s3 = time.time()
        valid_index_df = pd.merge(valid_index_df[['ts_code', 'pivot', 'group']],
                                  last_date_index_df[['ts_code', 'pivot']], on='ts_code', how='inner')
        s4 = time.time()
        perf_dict = {}
        for group_name, group_price_df in valid_index_df.groupby('group'):
            group_price_df.insert(0, 'return',  group_price_df['pivot_y']/group_price_df['pivot_x'])
#             group_price_df['return'] = group_price_df['pivot_y']/group_price_df['pivot_x']
            group_price_df.fillna(1, inplace=True)
            perf_dict.update({group_name: group_price_df['return'].mean()})
        s5 = time.time()
        if len(invalid_index_df):
            invalid_index_df = pd.merge(invalid_index_df[['ts_code', 'pivot']],
                                      last_date_index_df[['ts_code', 'pivot']], on='ts_code', how='inner')
                              
#             invalid_index_df['return'] = invalid_index_df['pivot_y']/invalid_index_df['pivot_x']
            invalid_index_df.insert(0, 'return', invalid_index_df['pivot_y']/invalid_index_df['pivot_x'])
            perf_dict.update({'none': invalid_index_df['return'].mean()})
        else:
            perf_dict.update({'none': 1})
        # print perf_dict
        s6 = time.time()
#         print("1 2 %s, 3 4 cost %s, 4 5 cost %s 5 6 cost %s" % (s2-s1, s4-s3, s5-s4, s6-s5))
        perf_dict.update({"trade_month": trade_month})
        month_perf_list.append(perf_dict)
    month_perf_df = pd.DataFrame(month_perf_list)
    month_perf_df.fillna(1, inplace=True)
    return month_perf_df

def test_market_year_performance(start_month, end_month):
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
        first_date_index_df.insert(0, 'group', first_date_index_df['market_year'].map(lambda x: min(int(x)+1, 4)))

        s2 = time.time()
        last_date_index_df = read_data(last_date)
        s3 = time.time()
        index_df = pd.merge(first_date_index_df[['ts_code', 'pivot', 'group']],
                                  last_date_index_df[['ts_code', 'pivot']], on='ts_code', how='inner')
        s4 = time.time()
        perf_dict = {}
        for group_name, group_price_df in index_df.groupby('group'):
            group_price_df.insert(0, 'return',  group_price_df['pivot_y']/group_price_df['pivot_x'])
            group_price_df.fillna(1, inplace=True)
            perf_dict.update({group_name: group_price_df['return'].mean()})

        perf_dict.update({"trade_month": trade_month})
        month_perf_list.append(perf_dict)
    month_perf_df = pd.DataFrame(month_perf_list)
    month_perf_df.fillna(1, inplace=True)
    return month_perf_df


def test_two_factors_inersection_effect(first_factor, second_factor, start_month, end_month, group_count):
    trade_dates = pd.read_pickle("trade_dates.pkl")
    # date_ranges = [trade_date for trade_date in trade_dates if trade_date >= start_date and trade_date <= end_date]
    trade_date_df = pd.DataFrame({'trade_date': trade_dates})
    trade_date_df['trade_month'] = trade_date_df['trade_date'].map(lambda x: x[:6])
    trade_date_df = trade_date_df[trade_date_df['trade_month'].map(lambda x: x <= end_month and x>=start_month)]
    month_perf_list = []
    month_portfolio_count_list = []
    for trade_month, trade_date_df in trade_date_df.groupby('trade_month'):
        first_date = trade_date_df['trade_date'].values[0]
        last_date = trade_date_df['trade_date'].values[-1]
        first_date_index_df = read_data(first_date)

        first_date_index_df = first_date_index_df[first_date_index_df['market_year'] > 3]
        print(first_factor)
        valid_index_df = first_date_index_df[pd.notna(first_date_index_df[first_factor])]
        valid_index_df = first_date_index_df[pd.notna(first_date_index_df[second_factor])]
        group_size = int(len(valid_index_df)/group_count)
        valid_index_df.sort_values(by=first_factor, inplace=True)
        valid_index_df.insert(0, 'first_range', range(len(valid_index_df)))
        valid_index_df.insert(0, 'first_group', valid_index_df['first_range'].map(lambda x: "%s_%s" % (first_factor, min(int(x/group_size)+1, group_count))))
        valid_index_df.sort_values(by=second_factor, inplace=True)
        valid_index_df.insert(0, 'second_range', range(len(valid_index_df)))
        valid_index_df.insert(0, 'second_group', valid_index_df['second_range'].map(lambda x: "%s_%s" % (second_factor, min(int(x/group_size)+1, group_count))))

        last_date_index_df = read_data(last_date)

        valid_index_df = pd.merge(valid_index_df[['ts_code', 'pivot', 'first_group', 'second_group']],
                                  last_date_index_df[['ts_code', 'pivot']], on='ts_code', how='inner')

        perf_dict = {}
        portfolio_count_dict = {}
        for (first_factor_name, second_factor_name), group_price_df in valid_index_df.groupby(['first_group', 'second_group']):
            group_price_df.insert(0, 'return',  group_price_df['pivot_y']/group_price_df['pivot_x'])
            group_price_df.fillna(1, inplace=True)

            perf_dict.update({(first_factor_name, second_factor_name): group_price_df['return'].mean()})
            portfolio_count_dict.update({(first_factor_name, second_factor_name): group_price_df['return'].count()})
#         perf_dict.update({"trade_month": trade_month})
#         portfolio_count_dict.update({'trade_month': trade_month})
        month_perf_list.append(perf_dict)
        month_portfolio_count_list.append(portfolio_count_dict)
    month_perf_df = pd.DataFrame(month_perf_list)
    month_perf_df.fillna(1, inplace=True)
    month_portfolio_count_df = pd.DataFrame(month_portfolio_count_list)
    month_portfolio_count_df.fillna(0, inplace=True)
    portfolio_perf_s = month_perf_df.prod()
    
#     portfolio_perf_s = pd.Series(portfolio_perf_s.values, index=[list(i) for i in perf_index_])
#     portfolio_perf_df = portfolio_perf_s.unstack()
    portfolio_count_s = month_portfolio_count_df.mean()
    portfoilo_perf_count_df = pd.concat([portfolio_perf_s, portfolio_count_s], axis=1)
    portfoilo_perf_count_df.columns = ['perf', 'count']
    portfolio_perf_count_s = portfoilo_perf_count_df.apply(lambda x: ("{:.2f}/{:.2f}".format(x['perf'],x['count'])), axis=1)
    perf_index_ = list(zip(*portfolio_perf_count_s.index.values))
    portfolio_perf_count_s = pd.Series(portfolio_perf_count_s.values, index=[list(i) for i in perf_index_])
    portfolio_perf_count_df = portfolio_perf_count_s.unstack()
    
    return portfolio_perf_count_df

# df = test_two_factors_inersection_effect('circ_mv', 'roe', '200707', '201806', 4)
# print(df)