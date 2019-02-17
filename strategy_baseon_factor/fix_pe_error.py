# -*- coding: utf-8 -*-


from util import MongodbUtils
import numpy as np
import time




with MongodbUtils('raw_data', 'day_index', '127.0.0.1', 27017) as day_index_collection:
    cur = day_index_collection.find({'pe_ttm': np.NaN})
    idx = 0
    with MongodbUtils('factor_db', 'stock_price', '127.0.0.1', 27017) as price_collection:
        for info in cur:
            if idx % 10000 == 0:
                print('count %s, at %s' % (idx, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))))
            ts_code = info['ts_code']
            trade_date = info['trade_date']
            price_collection.update_many({'ts_code':ts_code,'trade_date':trade_date},{"$set":{"pe_ttm":np.NaN}})
            idx+=1