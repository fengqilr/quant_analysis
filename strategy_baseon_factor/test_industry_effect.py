# -*- coding: utf-8 -*-


import pandas as pd
from pymongo import MongoClient
import time
import os
from util import MongodbUtils, read_data


def test_industry_single_performance(factor_name, start_month, end_month, group_count, industry_info):
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

#         valid_index_df.insert(0, 'range', range(len(valid_index_df)))
#         valid_index_df.insert(0, 'group', valid_index_df['range'].map(lambda x: min(int(x/group_size)+1, group_count)))
        valid_index_df.insert(0, 'industry', valid_index_df['ts_code'].map(industry_info))
#         valid_index_df['group'] = valid_index_df['range'].map(lambda x: min(x/group_size+1, group_count))
        # print valid_index_df[[factor_name, 'ts_code', 'group']].head(10)
        # print valid_index_df[[factor_name, 'ts_code', 'group']].tail(10)
        s2 = time.time()
        last_date_index_df = read_data(last_date)
        s3 = time.time()
        valid_index_df = pd.merge(valid_index_df[['ts_code', 'pivot', 'industry']],
                                  last_date_index_df[['ts_code', 'pivot']], on='ts_code', how='inner')
        s4 = time.time()
        perf_dict = {}
        industry_price_dfs = []
        for industry_name, industry_price_df in valid_index_df.groupby('industry'):
            group_size = len(industry_price_df)/group_count
            industry_price_df.insert(0, 'range', range(len(industry_price_df)))
            industry_price_df.insert(0, 'group', industry_price_df['range'].map(lambda x: min(int(x/group_size)+1, group_count)))
            
            industry_price_df.insert(0, 'return',  industry_price_df['pivot_y']/industry_price_df['pivot_x'])
#             group_price_df['return'] = group_price_df['pivot_y']/group_price_df['pivot_x']
            industry_price_df.fillna(1, inplace=True)
#             print(industry_price_df)
            industry_price_dfs.append(industry_price_df)
        if len(industry_price_dfs):
            industry_group_price_df = pd.concat(industry_price_dfs)
#         print(industry_group_price_df)
            for group_name, group_price_df in industry_group_price_df.groupby('group'):
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

if __name__ == '__main__':
#     industry_info = pd.read_pickle('../data/stock_2_industry.pkl')
    basic_info_df = pd.read_pickle('../data/basic_info.pkl')
    industry_info = dict(zip(basic_info_df['ts_code'].values, basic_info_df['industry'].values))
    
    factors = [ 'price_2_dividend','total_mv','pe_ttm','cashflowratio', 'cf_liabilities', 'cf_nm', 'cf_sales','currentratio',  'goodwillratio', 'gross_profit_rate',  'net_profit_ratio', 'net_profit_ratio_v2', 'quickratio', 'rateofreturn', 'roe', 'roe_std',
       'roe_trend', 'roe_v2', 'roe_v2_std', 'roe_v2_trend', 'sheqratio',  'circ_mv', 'cmp_gain','pb']
    industry_factor_effect_info_dict=  {}
    for factor in factors:
        perf_df = test_industry_single_performance(factor,  '200707', '201806', 4, industry_info)
        industry_factor_effect_info_dict.update({factor: perf_df})
    pd.to_pickle(industry_factor_effect_info_dict, 'ts_industry_factor_effect.pkl')
    factor_perf = pd.read_pickle("ts_industry_factor_effect.pkl")
    perfs = []
    for factor_name, perf in factor_perf.items():

    #     print(factor_name)
        perf_dict = perf.prod().to_dict()
        perf_dict.update({'factor_name': factor_name})
        perfs.append(perf_dict)
    perf_df = pd.DataFrame(perfs)
    print(perf_df)
