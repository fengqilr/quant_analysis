# -*- coding: utf-8 -*-


from util import get_stocks_day_return
import pandas as pd
import math
from sklearn import covariance, cluster


def cluster_industry(industry_return_df, start_date, end_date):
    industry_return_df = industry_return_df[industry_return_df.index.map(lambda x: x>=start_date and x<=end_date)]
    edge_model = covariance.GraphLassoCV()
#     edge_model.fit(industry_return_df.values)
#     centers, labels = cluster.affinity_propagation(edge_model.covariance_)
    kmeans_clf = cluster.KMeans(40)
    labels = kmeans_clf.fit_predict(industry_return_df.T.values)
    cluster_res_df = pd.DataFrame({'industry': industry_return_df.columns, 'label': labels})
    for label, tmp_df in cluster_res_df.groupby('label'):
        industry_names = " ".join(tmp_df['industry'].values)
        print("label %s: %s" % (label, industry_names))
    

def generate_industry_return():
    basic_info_path = "../data/basic_info.pkl"
    basic_info_df = pd.read_pickle(basic_info_path)
    industry_returns = []
    industry_names = []
    for industry, stock_info_df in basic_info_df.groupby('industry'):
        print("industry:%s, industry count %s" % (industry, len(stock_info_df['ts_code'].values)))
        industry_return = get_stocks_day_return(stock_info_df['ts_code'].values)
        industry_returns.append(industry_return)
        industry_names.append(industry)
    industry_return_df = pd.concat(industry_returns, axis=1)
#     industry_return_df = pd.read_pickle('industry_return.pkl')
    industry_return_df.columns = industry_names
    industry_return_df.fillna(1, inplace=True)
    industry_return_df = industry_return_df.applymap(math.log)
    print(industry_return_df)
    industry_return_df.to_pickle("industry_return.pkl")
    
if __name__ == '__main__':
#     generate_industry_return()
    industry_return_df = pd.read_pickle("industry_return.pkl")
    cluster_industry(industry_return_df, '20180701', '20181001')