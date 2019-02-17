
# coding: utf-8

# In[1]:


import pandas as pd
# -*- coding: utf-8 -*-
from test_factor_performance import test_market_year_performance,test_single_performance, test_two_factors_inersection_effect


# In[1]:


# df = test_market_year_performance('200707', '201806')


# In[2]:


factors = [ 'price_2_dividend','total_mv','pe_ttm','cashflowratio', 'cf_liabilities', 'cf_nm', 'cf_sales','currentratio',  'goodwillratio', 'gross_profit_rate',  'net_profit_ratio', 'net_profit_ratio_v2', 'quickratio', 'rateofreturn', 'roe', 'roe_std',
       'roe_trend', 'roe_v2', 'roe_v2_std', 'roe_v2_trend', 'sheqratio',  'circ_mv', 'cmp_gain','pb']


# In[10]:

# print("factor count %s" % (len(factors)))
# factor_perf = {}
# for factor_name in factors:
#     df = test_single_performance(factor_name, '200707', '201806', 4, {})
#     perfs = df.prod()
#     factor_perf.update({factor_name: perfs})

# print("factor perf count %s" % len(factor_perf))

# In[19]:





# In[16]:


# perfs = []
# for factor_name, perf in factor_perf.items():

# #     print(factor_name)
#     perf_dict = perf.to_dict()
#     perf_dict.update({'factor_name': factor_name})
#     perfs.append(perf_dict)
# perf_df = pd.DataFrame(perfs)
# print(perf_df)


# In[9]:

for factor_name in factors:
    if factor_name not in ['total_mv', 'circ_mv']:
        df = test_two_factors_inersection_effect('circ_mv', factor_name, '200707', '201806', 4)
        df.to_csv("./two_factor_effect/{}.csv".format(factor_name))


# In[10]:





# In[12]:




