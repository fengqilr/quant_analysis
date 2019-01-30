# -*- coding: utf-8 -*-

import pandas as pd
from pymongo import MongoClient
import time

import sys
# sys.path.append("D:\PycharmProjects\quant_analyze\quartz\quantify")
# from core import StrategyBase
s1 = time.time()
Client = MongoClient(host='127.0.0.1', port=27017)
db = Client['factor_db']
collection = db['stock_quality']
cur = collection.find({"end_date": "20110930"})
infos = list(cur)
s2 = time.time()
print s2-s1
df = pd.DataFrame(infos)
print df.isna().sum()/float(len(df))
#
# cur = collection.find_one(projection=['trade_date', 'end_date'])
# print cur
# info_list = list(cur)
# print info_list[0]
# trade_dates = list(set([info['trade_date'] for info in info_list]))
#
# trade_dates = info_list
# trade_dates.sort()
# #
# pd.to_pickle(trade_dates, "trade_dates.pkl")
# trade_dates = pd.read_pickle('trade_dates.pkl')
# strategy_clf = StrategyBase(start='20100101', end='20150101', hist=1, benchmark='000001.SZ', universe='a',
#                             capital_base=10000, commission=0.001, slippage=0, db_ip='47.110.156.244', db_port=27017)
# print trade_dates[5000]
# df = strategy_clf.read_data(trade_dates[5000])
# print pd.isna(df).sum()