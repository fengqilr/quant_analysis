# -*- coding: utf-8 -*-

from util import MongodbUtils
import time
import pandas as pd


with MongodbUtils('factor_db', 'stock_price', '127.0.0.1', 27017) as price_collection:
    cur = price_collection.find(projection=['trade_date']).distinct('trade_date')
    trade_dates = list(cur)
    for trade_date in trade_dates:
        print(trade_date)
        # _id = info['_id']
        # trade_date = info['trade_date']
        trade_date_stamp = int(time.mktime(time.strptime(trade_date, '%Y%m%d')))
        price_collection.update_many({"trade_date": trade_date}, {"$set": {'trade_date_stamp': trade_date_stamp}})


# with MongodbUtils('factor_db', 'stock_price', '127.0.0.1', 27017) as price_collection:
#     cur = price_collection.find(projection=['trade_date']).distinct('trade_date')
#     trade_dates = list(cur)
#     infos=  []
#     for trade_date in trade_dates[:2]:
#         cur = price_collection.find({'trade_date': trade_date})
#         infos.extend(list(cur))
#     print pd.DataFrame(infos)