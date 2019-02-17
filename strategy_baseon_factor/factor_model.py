# -*- coding: utf-8 -*-

import pandas as pd
from pymongo import MongoClient
import time
import os
import math
from util import MongodbUtils, read_data, get_stocks_period_day_return, get_stock_mv_info
import numpy as np

def cal_beta(market_returns, portfolios_return):
    beta = np.cov(market_returns, portfolios_return)/np.cov(market_returns)
    return beta

def cal_industry_return(code_2_industry_info):
    industry_2_code_info = {}
    for code, industry_name in code_2_industry_info.items():
        if industry_2_code_info.has_key(industry_name):
            industry_2_code_info[industry_name].append(code)
        else:
            industry_2_code_info.update({industry_name: code})
    for industry_name, stock_codes in industry_2_code_info.items():
        

def cal_quality_value_factor_return(factor_name, start_month, end_month):
    trade_dates = pd.read_pickle("trade_dates.pkl")
    trade_date_df = pd.DataFrame({'trade_date': trade_dates})
    trade_date_df['trade_month'] = trade_date_df['trade_date'].map(lambda x: x[:6])
    trade_date_df = trade_date_df[trade_date_df['trade_month'].map(lambda x: x <= end_month and x>=start_month)]
    market_value_group_factor_return = {1: [], 2: [], 3: [], 4: []}
    for trade_month, trade_date_df in trade_date_df.groupby('trade_month'):
        print(trade_month)
        first_date = trade_date_df['trade_date'].values[0]
        last_date = trade_date_df['trade_date'].values[-1]
        first_date_index_df = read_data(first_date)
        first_date_index_df = first_date_index_df[first_date_index_df['market_year'] > 3]
        first_date_index_df.sort_values(by="total_mv", inplace=True)
        group_size = len(first_date_index_df)/4
        first_date_index_df.insert(0, 'mv_range', range(len(first_date_index_df)))
        first_date_index_df.insert(0, 'mv_group', first_date_index_df['mv_range'].map(lambda x: min(int(x/group_size)+1, 4)))
        mv_group_factor_return = {}
        for mv_group, group_index_df in first_date_index_df.groupby('mv_group'):
            print("mv group %s" % mv_group)
            factor_median_value = group_index_df[factor_name].quantile(0.5)
            factor_low_group_df = group_index_df[group_index_df[factor_name] < factor_median_value]
            if len(factor_low_group_df):
                factor_high_group_df = group_index_df[group_index_df[factor_name] >= factor_median_value]
            else:
                factor_low_group_df = group_index_df[group_index_df[factor_name] <= factor_median_value]
                factor_high_group_df = group_index_df[group_index_df[factor_name] > factor_median_value]
            high_stocks = factor_high_group_df['ts_code'].values
            low_stocks = factor_low_group_df['ts_code'].values
            high_stocks_return = get_stocks_period_day_return(high_stocks, first_date, last_date)
            low_stocks_return = get_stocks_period_day_return(low_stocks, first_date, last_date)
            high_minus_low_stock_return = (high_stocks_return-low_stocks_return)+1
            high_minus_low_stock_log_return = high_minus_low_stock_return.map(math.log)
            market_value_group_factor_return[mv_group].append(high_minus_low_stock_log_return)
    market_value_group1_return = pd.concat(market_value_group_factor_return[1])
    market_value_group2_return = pd.concat(market_value_group_factor_return[2])
    market_value_group3_return = pd.concat(market_value_group_factor_return[3])
    market_value_group4_return = pd.concat(market_value_group_factor_return[4])
    return [market_value_group1_return, market_value_group2_return, market_value_group3_return, market_value_group4_return]

# basic_info_df = pd.read_pickle("../data/basic_info.pkl")
# ts_codes = basic_info_df['ts_code'].values

# for idx, ts_code in enumerate(ts_codes):
#     get_stock_mv_info(ts_code)
#     if idx % 10 == 0:
#         print(idx)
# factors = [ 'price_2_dividend','pe_ttm','cashflowratio', 'cf_liabilities', 'cf_nm', 'cf_sales','currentratio',  'goodwillratio', 'gross_profit_rate',  'net_profit_ratio', 'net_profit_ratio_v2', 'quickratio', 'rateofreturn', 'roe', 'roe_std', 'roe_trend', 'roe_v2', 'roe_v2_std', 'roe_v2_trend', 'sheqratio',  'cmp_gain','pb']
# for factor_name in factors:
#     factor_mv_group_return = cal_quality_value_factor_return(factor_name, '200707', '201806')
#     pd.to_pickle(factor_mv_group_return, '/home/lianrui/quant/factor_return/%s.pkl' % factor_name)
    
            
            
            