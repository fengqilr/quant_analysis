# -*- coding: utf-8 -*-
db_pool = {}

from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import time
import os

cash_data_dir = "/home/lianrui/quant/quant_analysis/cash_data"


class MongodbUtils(object):
    def __init__(self, table, collection, ip, port):
        self.table = table
        self.ip = ip
        self.port = port
        self.collection = collection
        if (ip, port) not in db_pool:
            db_pool[(ip, port)] = self.db_connection()
        self.db = db_pool[(ip, port)]
        self.db_table = self.db_table_connect()

    def __enter__(self):
        return self.db_table

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def db_connection(self):
        db = None
        try:
            db = MongoClient(self.ip, self.port)
        except Exception as e:
            print('can not connect mongodb')
            raise e
        return db

    def db_table_connect(self):
        table_db = self.db[self.table][self.collection]
        return table_db
    

def show_figure(df, y1_columns, y2_columns, start_date, end_date, time_graduation='month'):
    df = df[df.index.map(lambda x: x>=start_date and x<=end_date)]
    dates = df.index.values
    xs = [datetime.strptime(d, '%Y%m%d').date() for d in dates]
    # ys = data_s.values
    # 配置横坐标
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y%m%d'))
    assert time_graduation in ['day', 'month', 'year']
    if time_graduation == 'day':
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    elif time_graduation == 'month':
        plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    else:
        plt.gca().xaxis.set_major_locator(mdates.YearLocator())
    # Plot
    # for column_name in columns:
    #     plt.plot(xs, df[column_name].values)
    # plt.legend(columns)
    plt.gcf().autofmt_xdate()  # 自动旋转日期标记
   
    fig = plt.figure()

    ax1 = fig.add_subplot(111)
    for y1_column_name in y1_columns:
        ax1.plot(xs, df[y1_column_name])
    ax1.legend(y1_columns)
    
def read_data(trade_date):
    s1 = time.time()
    trade_date_file_name = "%s.pkl" % trade_date
    cash_files = os.listdir(cash_data_dir)
    if trade_date_file_name in cash_files:
        index_df = pd.read_pickle(cash_data_dir + os.sep + trade_date_file_name)
    else:

        with MongodbUtils('factor_db', 'stock_price', '127.0.0.1', 27017) as price_collection:
            price_cur = price_collection.find({'trade_date': trade_date})
        # price_infos = list(price_cur)
            price_infos = [info for info in price_cur]
            price_df = pd.DataFrame(price_infos)
            price_df['pe_ttm'].fillna(1e5, inplace=True)
            price_df['price_2_dividend'].fillna(1e8, inplace=True)
        end_date = price_df['end_date'].values[0]
        with MongodbUtils('factor_db', 'stock_quality', '127.0.0.1', 27017) as quality_collection:
            quality_cur = quality_collection.find({'end_date': end_date})
            quality_df = pd.DataFrame([info for info in quality_cur])
            quality_df['roe'] = quality_df['roe'].map(lambda x: max(x,0))
            quality_df['roe_v2'] = quality_df['roe_v2'].map(lambda x: max(x, 0))
            quality_df['gross_profit_rate'] = quality_df['gross_profit_rate'].map(lambda x: max(x, 0)) 
            quality_df['net_profit_ratio'] = quality_df['net_profit_ratio'].map(lambda x: max(x, 0)) 
        index_df = pd.merge(quality_df, price_df, on='ts_code', how='inner')
        index_df['goodwillratio'].fillna(value=0, inplace=True)
        index_df['cmp_gain'] = index_df['cmp_gain'].map(lambda x: max(x,0))
        index_df['market_year'] = index_df.apply(lambda x: get_market_year(x['ts_code'], stock_2_listdate, trade_date), axis=1)
        index_df.to_pickle(cash_data_dir + os.sep + trade_date_file_name)
    s2 = time.time()
    return index_df


def get_stock_mv_info(stock_name):
    mv_dir = "/home/lianrui/quant/market_value_data"
    cash_files = os.listdir(mv_dir)
    cash_stocks = [file_name[: -4] for file_name in cash_files]
    if stock_name in cash_stocks:
        stock_mv_df = pd.read_pickle(mv_dir + os.sep + stock_name + '.pkl')
    else:
        with MongodbUtils('raw_data', 'day_index', '127.0.0.1', 27017) as day_index_collection:
            stock_mv_cur = day_index_collection.find({'ts_code': stock_name}, {'trade_date': 1, 'total_mv': 1, '_id':0})
            stock_mv_infos = [info for info in stock_mv_cur]
            stock_mv_df = pd.DataFrame(stock_mv_infos)
            stock_mv_df.set_index('trade_date', inplace=True)
            stock_mv_df.to_pickle(mv_dir + os.sep + stock_name + '.pkl')
    return stock_mv_df

def get_stock_total_day_return(stocks):
    """
         获得行业整体收益 
    """
#     trade_dates = pd.read_pickle("./trade_dates.pkl")

    stock_total_mv_infos = []
    for stock_name in stocks:
        stock_mv_df = get_stock_mv_info(stock_name)
        stock_total_mv_infos.append(stock_mv_df)
    stock_mv_df = pd.concat(stock_total_mv_infos, axis=1)
    stock_mv_df.columns = stocks
    stock_mv_df.sort_index(inplace=True)
    stock_mv_df.fillna(method='ffill', inplace=True)
    
    trade_dates = stock_mv_df.index.values
    
    stock_day_returns = []
    for idx, trade_date in enumerate(trade_dates):
        if idx:
            last_trade_date = trade_dates[idx-1]
            tmp_df = stock_mv_df.loc[[last_trade_date, trade_date]]
            tmp_df.dropna(axis=1, inplace=True)
            stocks_mv_s = tmp_df.sum(axis=1)
            r = stocks_mv_s.iloc[1]/stocks_mv_s.iloc[0]
            stock_day_returns.append(r)
    stocks_day_retrun_s = pd.Series(stock_day_returns, index=trade_dates[1:])

    return stocks_day_retrun_s


def get_stocks_period_day_return(stocks, start_date, end_date):
#     total_stocks_return = get_stock_total_day_return(stocks)
    stock_total_mv_infos = []
    for stock_name in stocks:
        stock_mv_df = get_stock_mv_info(stock_name)
        stock_total_mv_infos.append(stock_mv_df)
    stock_mv_df = pd.concat(stock_total_mv_infos, axis=1)
    stock_mv_df.columns = stocks
    stock_mv_df.sort_index(inplace=True)
    stock_mv_df.fillna(method='ffill', inplace=True)
    stocks_total_mv_s = stock_mv_df.sum(axis=1)
    last_date_mv_s = pd.Series([None]+list(stocks_total_mv_s.values[: -1]), index=stocks_total_mv_s.index)
    
    total_stocks_return = stocks_total_mv_s/last_date_mv_s
    period_stocks_return = total_stocks_return[total_stocks_return.index.map(lambda x: x>=start_date and x<=end_date)]
    return period_stocks_return
    
    
# print(get_stocks_day_return(['000001.SZ', '000002.SZ']))