# -*- coding: utf-8 -*-

import pandas as pd

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import matplotlib.pyplot as plt
from util import *
import pandas as pd
import time, datetime
from sklearn.ensemble import IsolationForest


financial_dir = "/home/lian/market_kline/tushare_data_pro/financial_rpt_data"
none_fq_dir = "/home/lian/market_kline/tushare_data/hist_stock_price_none_fq"
qfq_dir = "/home/lian/market_kline/tushare_data/hist_stock_price_qfq"
basic_info_dir = "/home/lian/market_kline/tushare_data_pro/basic_info"
month_price_dir = "/home/lian/market_kline/tushare_data/hist_stock_month_price"
perf_index_dir = "/home/lian/market_kline/tushare_data_pro/perf_index"
def analysis_return(ts_code):
    none_fq_hist_stock = pd.read_pickle(none_fq_dir + os.sep + "%s.pkl" % ts_code)
    # print none_fq_hist_stock.head(10)
    none_fq_hist_stock = none_fq_hist_stock[none_fq_hist_stock['pct_change'].map(lambda x: x <= 10 and x>=-10) ]
    none_fq_hist_stock['pct_change'].hist(bins=200)

    # print none_fq_hist_stock['pct_change'].max()
    plt.show()


def get_financial_rpt_data(ts_code):
    code = ts_code[:6]
    rpt_data = pd.read_pickle(financial_dir + os.sep + "%s.pkl" % code)
    # print rpt_data['income'][[ 'total_cogs', 'oper_cost','end_date' ,'admin_exp']]
    keys_list = list(rpt_data['income'].columns)
    for key in keys_list:
        print "%s : %s" % (key, rpt_data['income'][key].values[-1])





def get_performance_index_general_last_year(ts_code):
    financial_rpt_data = pd.read_pickle(financial_dir + os.sep + "%s.pkl" % ts_code[:6])
    balance_sheet_df = financial_rpt_data['balancesheet']
    print balance_sheet_df.head(1)
    cash_flow_df = financial_rpt_data['cashflow']
    print cash_flow_df.head(1)
    profit_df = financial_rpt_data['income']
    print profit_df.head(1)

    # total_performance_df = pd.concat([profit_df[['total_cogs', 'total_revenue', "operate_profit", 'basic_eps',
    #                                              'total_profit', 'fin_exp', 'n_income_attr_p', 'ebit', 'ebitda']],
    #                                   cash_flow_df[['n_cashflow_act']],
    #                                   balance_sheet_df[['total_share', "total_hldr_eqy_exc_min_int", 'accounts_receiv',
    #                                                     'inventories', 'total_cur_assets', 'total_liab', 'total_cur_liab',
    #                                                     'money_cap', 'total_hldr_eqy_inc_min_int', 'total_assets']]], axis=1)
    profit_items = ['total_cogs', 'total_revenue', "operate_profit", 'basic_eps', 'total_profit', 'fin_exp',
                    'n_income_attr_p', 'ebit', 'ebitda', 'ann_date']
    cashflow_items = ['n_cashflow_act']
    balancesheet_items = ['total_share', "total_hldr_eqy_exc_min_int", 'accounts_receiv', 'inventories',
                          'total_cur_assets', 'total_liab', 'total_cur_liab',
                          'money_cap', 'total_hldr_eqy_inc_min_int', 'total_assets']
    total_performance_df = pd.merge(profit_df, balance_sheet_df, on='end_date', how='inner')
    total_performance_df = pd.merge(total_performance_df, cash_flow_df, on='end_date', how='inner')
    total_performance_df.set_index('end_date', inplace=True)
    total_performance_df = total_performance_df[profit_items+cashflow_items+balancesheet_items]

    total_performance_df['n_income_attr_p_last_year'] = \
        get_last_year_peformance_index(total_performance_df['n_income_attr_p'])
    # print total_performance_df['total_cogs']
    total_performance_df['total_cogs_last_year'] = get_last_year_peformance_index(total_performance_df['total_cogs'])
    total_performance_df['total_revenue_last_year'] = get_last_year_peformance_index(total_performance_df['total_revenue'])
    total_performance_df['operate_profit_last_year'] = get_last_year_peformance_index(total_performance_df['operate_profit'])
    # print total_performance_df.head(2).T

    total_performance_df['eps_last_year'] = total_performance_df.apply(lambda x: x['n_income_attr_p_last_year']/x['total_share'] if x['total_share'] and x['n_income_attr_p_last_year'] is not None else None, axis=1)
    total_performance_df['total_profit_last_year'] = get_last_year_peformance_index(total_performance_df['total_profit'])
    total_performance_df['fin_exp_last_year'] = get_last_year_peformance_index(total_performance_df['fin_exp'])
    total_performance_df['n_cashflow_act_last_year'] = \
        get_last_year_peformance_index(total_performance_df['n_cashflow_act'])
    total_performance_df['accounts_receiv_last_year'] = get_last_year_performance_index_v2(total_performance_df['accounts_receiv'])
    total_performance_df['inventories_last_year'] = get_last_year_performance_index_v2(total_performance_df['inventories'])
    total_performance_df['total_cur_assets_last_year'] = get_last_year_performance_index_v2(total_performance_df['total_cur_assets'])

    ### 净利率 ###
    total_performance_df['net_profit_ratio'] = \
        total_performance_df.apply(lambda x: x['n_income_attr_p_last_year']/x['total_revenue_last_year']
        if x['total_revenue_last_year'] and x['n_income_attr_p_last_year'] is not None else None, axis=1)
    ###毛利率####
    total_performance_df['gross_profit_rate'] = \
        total_performance_df.apply(lambda x: x['operate_profit_last_year']/x['total_revenue_last_year']
        if x['total_revenue_last_year'] and x['operate_profit_last_year'] is not None else None, axis=1)
    ###净资产收益率=净利润/股东权益###
    total_performance_df['roe'] = total_performance_df.\
        apply(lambda x: x['n_income_attr_p_last_year']/x['total_hldr_eqy_exc_min_int']
    if x['total_hldr_eqy_exc_min_int'] and x['n_income_attr_p_last_year'] is not None else None, axis=1)
    ###流动比率＝流动资产合计/流动负债合计###
    total_performance_df['currentratio'] = total_performance_df.\
        apply(lambda x: x['total_cur_assets']/x['total_cur_liab'] if x['total_cur_liab'] and x['total_cur_assets'] is not None else None, axis=1)
    ### 速动比率=速动资产/流动负债 ###
    total_performance_df['quickratio'] = total_performance_df.\
        apply(lambda x: (x['total_cur_assets']-x['inventories'])/x['total_cur_liab']
    if x['total_cur_liab'] and x['total_cur_assets'] is not None and x['inventories'] is not None else None, axis=1)
    ######现金比率=（现金+有价证券）/流动负债 ###
    total_performance_df['cashratio'] = total_performance_df.\
        apply(lambda x: x['money_cap']/x['total_cur_liab'] if x['total_cur_liab'] and x['money_cap'] is not None else None, axis=1)
    ###利息支付倍数=息税前利润/利息费用###
    total_performance_df['icratio'] = total_performance_df.\
        apply(lambda x: x['total_profit_last_year']/x['fin_exp_last_year']+1 if x['fin_exp_last_year'] and x['total_profit_last_year'] is not None else None, axis=1)
    ###股东权益比例＝股东权益总额/资产总额###
    total_performance_df['sheqratio'] = total_performance_df.\
        apply(lambda x: x['total_hldr_eqy_inc_min_int']/x['total_assets'] if x['total_assets'] and x['total_hldr_eqy_inc_min_int'] is not None else None, axis=1)
    ###经营现金净流量对销售收入比率=经营活动现金净流量/主营业务收入###
    total_performance_df['cf_sales'] = total_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['total_revenue_last_year'] if x['total_revenue_last_year'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)
    ###资产的经营现金流量回报率=经营活动现金净流量/总资产###
    total_performance_df['rateofreturn'] = total_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['total_assets'] if x['total_assets'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)
    ####经营现金净流量与净利润的比率#####
    total_performance_df['cf_nm'] = total_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['n_income_attr_p_last_year'] if x['n_income_attr_p_last_year'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)
    ####经营现金净流量对负债比率=经营活动产生的现金净流量/负债总额###
    total_performance_df['cf_liabilities'] = total_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['total_liab'] if x['total_liab'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)
    ####经营现金净流量对负债比率=经营活动产生的现金净流量/流动负债###
    total_performance_df['cashflowratio'] = total_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['total_cur_liab'] if x['total_cur_liab'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)
    ### 应收账款周转率=销售净收入/平均应收账款###
    total_performance_df['arturnover'] = total_performance_df.\
        apply(lambda x: x['total_revenue_last_year']/x['accounts_receiv_last_year'] if x['accounts_receiv_last_year'] and x['total_revenue_last_year'] is not None else None, axis=1)
    ### 存货周转率= 销售成本/平均存货余额####
    total_performance_df['inventory_turnover'] = total_performance_df.\
        apply(lambda x: x['total_cogs_last_year']/x['inventories_last_year'] if x['inventories_last_year'] and x['total_cogs_last_year'] is not None else None, axis=1)
    ####流动资产周转率=主营业务收入净额/平均流动资产总额###
    total_performance_df['currentasset_turnover'] = total_performance_df.\
        apply(lambda x: x['total_revenue_last_year']/x['total_cur_assets_last_year'] if x['total_cur_assets_last_year'] and x['total_revenue_last_year'] is not None else None, axis=1)
    total_performance_df['ts_code'] = ts_code
    return total_performance_df


def analyze_stock_financial_index():
    basic_dir = "/home/lian/market_kline/tushare_data_pro/basic_info"
    stock_basic_info = pd.read_pickle(basic_dir+os.sep+"basic_info.pkl")
    terminated_stock_info = pd.read_pickle(basic_dir+os.sep+"terminated_stock_info.pkl")
    ts_code_list = list(stock_basic_info['ts_code'].values) + list(terminated_stock_info['ts_code'].values)
    perf_index_list = []
    for idx, ts_code in enumerate(ts_code_list):
        try:
            if (idx+1) % 50 == 0:
                print "prcess %s: %s" % (idx+1, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
            perf_index_df = get_performance_index_general_last_year(ts_code)
            perf_index_list.append(perf_index_df)
        except Exception, e:
            print ts_code
            print e
    total_perf_index_df = pd.concat(perf_index_list, axis=0)
    return total_perf_index_df.reset_index()


def analyse_financial_index_abnormal():
    perf_index_dir = "/home/lian/market_kline/tushare_data_pro/perf_index"
    total_perf_index_df = pd.read_pickle(perf_index_dir + os.sep + "rpt_perf_index.pkl")
    df = total_perf_index_df[total_perf_index_df['end_date'] == '20091231']
    df = df[['net_profit_ratio', 'gross_profit_rate', 'roe', 'currentratio',
           'quickratio', 'cashratio', 'icratio', 'sheqratio', 'cf_sales',
           'rateofreturn', 'cf_nm', 'cf_liabilities', 'cashflowratio', 'arturnover',
           'inventory_turnover', 'currentasset_turnover']]
    # cal_mean_std_50_percent_with_boostrap(df['roe'])
    # df = total_perf_index_df[total_perf_index_df['end_date'] == '20091231']
    # print df.max()
    # print df.min()
    # # print df[df['roe'].isna()]
    roe_s = df['cashflowratio'][df['cashflowratio'].notna()]
    # print "mean %s, std %s" % (roe_s.mean(), roe_s.std())
    # print roe_s.quantile(0.02)
    # print roe_s.quantile(0.98)
    # print roe_s.quantile(0.5)
    # roe_s = roe_s[roe_s.map(lambda x: x<1.0 and x>-1)]
    is_forest = IsolationForest()
    roe_values = roe_s.values.reshape(-1, 1)
    is_forest.fit(roe_values)
    res = is_forest.predict(roe_values)
    roe_df = pd.DataFrame({'roe': roe_s.values, 'predict_res': res})
    # print roe_df
    roe_df.sort_values(by='roe', inplace=True)
    outliers = roe_df[roe_df['predict_res'] == -1]
    for info in outliers.to_dict('records'):
        print "roe %s" % info['roe']
    print "outlier count %s" % len(outliers)
    # print res
    # print "mean %s, std %s" % (roe_s.mean(), roe_s.std())


def get_action_price(ts_code, action_date, total_price_info):
    code_price_df = total_price_info[total_price_info['ts_code']==ts_code]
    limit_date = "%s15" % action_date[:6]
    code_price_df = code_price_df[code_price_df['trade_date'].map(lambda x: x>=action_date and x<=limit_date)]
    if len(code_price_df):
        return code_price_df['open'].values[0]
    else:
        return None


def update_perf_index_df(total_perf_index_df):
    ts_code_list = list(total_perf_index_df['ts_code'].unique())
    def get_action_date(end_date):
        year = end_date[:4]
        month = end_date[4:6]
        if month == '03':
            action_date = "%s0501" % year
        elif month == '06':
            action_date = "%s0901" % year
        elif month == '09':
            action_date = "%s1101" % year
        else:
            action_date = "%s0501" % (int(year)+1)
        return action_date
    total_perf_index_df['action_date'] = total_perf_index_df['end_date'].map(get_action_date)
    def get_action_date_(trade_date):
        month_date = trade_date[4:]

        if month_date>='0501' and month_date<='0515':
            return "%s0501" % trade_date[:4]
        elif month_date>='0901' and month_date<='0915':
            return "%s0901" % trade_date[:4]
        elif month_date>='1101' and month_date<='1115':
            return "%s1101" % trade_date[:4]
        else:
            return None
    ts_code_price_info_dict = {}
    ts_code_nonfq_price_info_dict = {}
    for ts_code in ts_code_list:
        # qfq_price = pd.read_pickle(qfq_dir+os.sep+"%s.pkl" % ts_code)
        # qfq_price['month_date'] = qfq_price['trade_date'].map(lambda x: x[4:])
        # qfq_price['action_date'] = qfq_price['trade_date'].map(get_action_date_)
        # qfq_price.dropna(subset=['action_date'], inplace=True, axis=0)
        # qfq_price.drop_duplicates(subset=['ts_code', 'action_date'], inplace=True)
        # ts_code_price_info_dict.update(
        #     {ts_code: dict(zip(qfq_price['action_date'].values, qfq_price['open'].values))})

        non_fq_price = pd.read_pickle(none_fq_dir + os.sep + "%s.pkl" % ts_code)
        non_fq_price['month_date'] = non_fq_price['trade_date'].map(lambda x: x[4:])
        non_fq_price['action_date'] = non_fq_price['trade_date'].map(get_action_date_)
        non_fq_price.dropna(subset=['action_date'], inplace=True, axis=0)
        non_fq_price.drop_duplicates(subset=['ts_code', 'action_date'], inplace=True)
        ts_code_nonfq_price_info_dict.update(
            {ts_code: dict(zip(non_fq_price['action_date'].values, non_fq_price['open'].values))})


    # total_perf_index_df['action_price'] = \
    #     total_perf_index_df.apply(lambda x: ts_code_price_info_dict.get(x['ts_code'], {}).get(x['action_date'], None), axis=1)
    total_perf_index_df['action_price_non_fq'] = \
        total_perf_index_df.apply(lambda x: ts_code_nonfq_price_info_dict.get(x['ts_code'], {}).get(x['action_date'], None), axis=1)
    total_perf_index_df['market_price'] = \
        total_perf_index_df.apply(
            lambda x: x['action_price_non_fq'] * x['total_share']
            if x['action_price_non_fq'] is not None and x['total_share'] is not None else None, axis=1)
    return total_perf_index_df


def test_perf_effect(total_perf_index_df, perf_names):
    total_perf_index_df.drop_duplicates(subset=['ts_code', 'end_date'], inplace=True)
    total_perf_index_df['stock_actiondate'] = total_perf_index_df.apply(lambda x: (x['ts_code'], x['action_date']), axis=1)
    stock_action_date_price_dict = dict(zip(total_perf_index_df['stock_actiondate'].values, total_perf_index_df['action_price'].values))
    stock_res_info_list = []
    for perf_name in perf_names:
        for year in range(2004, 2012):
            print "year %s, perf %s" % (year, perf_name)
            this_year_perf = total_perf_index_df[total_perf_index_df['end_date'] == "%s0331" % year]
            # print this_year_perf.head(1).T
            # this_year_perf.dropna(subset=[perf_name], axis=0, inplace=True)
            # print this_year_perf['roe'].isna().sum()
            # this_year_perf.sort_values(by='market_price', ascending=False, inplace=True)
            # this_year_perf = this_year_perf.head(300)
            this_year_perf['sell_date'] = this_year_perf.apply(lambda x: (x['ts_code'], "%s%s" % (int(x['action_date'][:4])+1, x['action_date'][4:])), axis=1)
            this_year_perf['sell_price'] = this_year_perf['sell_date'].map(lambda x: stock_action_date_price_dict.get(x, None))
            this_year_perf['return'] = this_year_perf.apply(lambda x: x['sell_price']/x['action_price'], axis=1)

            percent_75 = this_year_perf[perf_name].quantile(0.75)
            percent_50 = this_year_perf[perf_name].quantile(0.5)
            percent_25 = this_year_perf[perf_name].quantile(0.25)
            return_rank1 = this_year_perf[this_year_perf[perf_name] > percent_75]['return'].mean()
            return_rank2 = this_year_perf[this_year_perf[perf_name].map(lambda x: x<=percent_75 and x>percent_50)]['return'].mean()
            return_rank3 = this_year_perf[this_year_perf[perf_name].map(lambda x: x<=percent_50 and x> percent_25)]['return'].mean()
            return_rank4 = this_year_perf[this_year_perf[perf_name].map(lambda x: x<=percent_25)]['return'].mean()
            return_nan = this_year_perf[this_year_perf[perf_name].isna()]['return'].mean()
            print "rank1 %s" % this_year_perf[this_year_perf[perf_name] > percent_75]['return'].mean()
            print "rank2 %s" % this_year_perf[this_year_perf[perf_name].map(lambda x: x<=percent_75 and x>percent_50)]['return'].mean()
            print "rank3 %s" % this_year_perf[this_year_perf[perf_name].map(lambda x: x<=percent_50 and x> percent_25)]['return'].mean()
            print "rank4 %s" % this_year_perf[this_year_perf[perf_name].map(lambda x: x<=percent_25)]['return'].mean()
            print "nall %s" % this_year_perf[this_year_perf[perf_name].isna()]['return'].mean()
            stock_res_info_list.append({'year': year, 'perf_name': perf_name, 'rank1': return_rank1,
                                        'rank2': return_rank2, 'rank3': return_rank3, 'rank4': return_rank4,
                                        'ranknan': return_nan})
    stock_res_df = pd.DataFrame(stock_res_info_list)
    return stock_res_df


def get_stock_industry():

    basic_dir = "/home/lian/market_kline/tushare_data_pro/basic_info"
    stock_basic_info = pd.read_pickle(basic_dir+os.sep+"basic_info.pkl")
    terminated_stock_info = pd.read_pickle(basic_dir+os.sep+"terminated_stock_info.pkl")
    stock_industry_df = pd.concat([stock_basic_info[['ts_code', 'industry']], terminated_stock_info[['ts_code', 'industry']]])
    ts_code_2_industry = dict(zip(stock_industry_df['ts_code'].values, stock_industry_df['industry'].values))
    return ts_code_2_industry


def test_perf_effect_with_industry(perf_name_list, total_perf_index_df):
    tscode_2_indu = get_stock_industry()
    total_perf_index_df.drop_duplicates(subset=['ts_code', 'end_date'], inplace=True)
    total_perf_index_df['stock_actiondate'] = total_perf_index_df.apply(lambda x: (x['ts_code'], x['action_date']), axis=1)
    stock_action_date_price_dict = dict(zip(total_perf_index_df['stock_actiondate'].values, total_perf_index_df['action_price'].values))
    total_perf_index_df['industry'] = total_perf_index_df['ts_code'].map(tscode_2_indu)
    for year in range(2004, 2010):
        for perf_name in perf_name_list:
            print "year %s perf %s" % (year, perf_name)
            this_year_perf = total_perf_index_df[total_perf_index_df['end_date'] == "%s0331" % year]
            this_year_perf['sell_date'] = this_year_perf.apply(lambda x: (x['ts_code'], "%s%s" % (int(x['action_date'][:4])+1, x['action_date'][4:])), axis=1)
            this_year_perf['sell_price'] = this_year_perf['sell_date'].map(lambda x: stock_action_date_price_dict.get(x, None))
            this_year_perf['return'] = this_year_perf.apply(lambda x: x['sell_price']/x['action_price'], axis=1)
            industry_perfs = []
            for industry_name, industry_perf in this_year_perf.groupby('industry'):
                percent_75 = industry_perf[perf_name].quantile(0.75)
                percent_50 = industry_perf[perf_name].quantile(0.5)
                percent_25 = industry_perf[perf_name].quantile(0.25)
                def get_rank(perf_value):
                    if perf_value is None:
                        return None
                    if perf_value > percent_75:
                        return 1
                    elif perf_value > percent_50:
                        return 2
                    elif perf_value > percent_25:
                        return 3
                    else:
                        return 4
                industry_perf['rank'] = industry_perf[perf_name].map(lambda x: get_rank(x))
                # print industry_perf
                industry_perfs.append(industry_perf)
            update_this_year_perf = pd.concat(industry_perfs)
            print "rank1 %s" % update_this_year_perf[update_this_year_perf['rank'] == 1]['return'].mean()
            print "rank2 %s" % update_this_year_perf[update_this_year_perf['rank'] == 2]['return'].mean()
            print "rank3 %s" % update_this_year_perf[update_this_year_perf['rank'] == 3]['return'].mean()
            print "rank4 %s" % update_this_year_perf[update_this_year_perf['rank'] == 4]['return'].mean()
            print "nan %s" % update_this_year_perf[update_this_year_perf['rank'].isna()]['return'].mean()


def get_stock_month_return():
    qfq_dir = "/home/lian/market_kline/tushare_data/hist_stock_price_qfq"
    stock_price_file_name_lisrt = os.listdir(qfq_dir)
    tradeday_list = []
    print 'start to get total trade date'
    # for stock_price_file_name in stock_price_file_name_lisrt:
    #     price_df = pd.read_pickle(qfq_dir + os.sep + stock_price_file_name)
    #     # print price_df.columns
    #     tradeday_list.extend(price_df['trade_date'].values)
    # tradeday_list = list(set(tradeday_list))
    # tradeday_list.sort()
    # total_tradeday_s = pd.Series(tradeday_list)
    # total_tradeday_s.to_pickle("total_trade_date.pkl")
    total_tradeday_s = pd.read_pickle("total_trade_date.pkl")
    print "start to get price "
    ts_code_price_info = {}

    process_count = 0
    for stock_price_file_name in stock_price_file_name_lisrt:
        price_df = pd.read_pickle(qfq_dir + os.sep + stock_price_file_name)
        max_day = price_df['trade_date'].max()
        min_day = price_df['trade_date'].min()
        tmp_days = total_tradeday_s[total_tradeday_s.map(lambda day: day <= max_day and day >= min_day)]
        tradeday_df = pd.DataFrame(tmp_days, columns=['trade_date'])

        price_df = pd.merge(price_df, tradeday_df, how='outer', on='trade_date')
        price_df = price_df.fillna(method='bfill')
        price_df['price'] = price_df.apply(lambda x: x[['open', 'close']].mean(), axis=1)
        price_df['trade_month'] = price_df['trade_date'].map(lambda x: x[:6])
        price_df.drop_duplicates(subset=['trade_month'], keep='last', inplace=True)
        # print price_df[['trade_month', 'price']]
        # yearmonth_2_price = dict(zip(price_df['trade_month'].values, price_df['price'].values))
        # ts_code_price_info.update({stock_price_file_name[:9]: yearmonth_2_price})
        process_count += 1
        if process_count % 100 == 0:
            print "process %s, at %s" % (process_count, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        price_df.to_pickle(month_price_dir + os.sep + "%s.pkl" % stock_price_file_name[:9])


def get_month_between_min_max(start_year_month, end_year_month):
    start_year = int(start_year_month[:4])
    start_month = int(start_year_month[4:])
    end_year = int(end_year_month[:4])
    end_month = int(end_year_month[4:])
    year_month_list = []
    for j in range(12*start_year+start_month, 12*end_year+end_month+1):
        year_month_list.append("%d%02d" % ((j-1)/12, (j-1)%12+1))
    return year_month_list


def get_next_month(tmp_year_month):
    tmp_year = int(tmp_year_month[:4])
    tmp_month = int(tmp_year_month[4:])
    if tmp_month == 12:
        next_month = "%s01" % (tmp_year+1)
    else:
        next_month = "%s%02d" % (tmp_year, tmp_month+1)
    return next_month

total_month_s = get_month_between_min_max('198001', '201812')
# print total_month_s
month_2_index = {month_: idx for idx, month_ in enumerate(total_month_s)}

def get_relative_month(tmp_year_month, k):
    # tmp_year = int(tmp_year_month[:4])
    # tmp_month = int(tmp_year_month[4:])
    # j = tmp_year*12+tmp_month
    # relative_j = j + k
    # relative_month = "%d%02d" % ((relative_j-1)/12, (relative_j-1)%12+1)
    # print tmp_year_month
    relative_month = total_month_s[month_2_index[tmp_year_month]+k]
    return relative_month



def get_relative_return(tmp_year_month, k, stock_month_price_info):
    relative_k_month = get_relative_month(tmp_year_month, -k)
    tmp_price = stock_month_price_info.get(tmp_year_month, None)
    relative_k_price = stock_month_price_info.get(relative_k_month, None)
    if tmp_price is not None and relative_k_price is not None:
        return tmp_price/relative_k_price
    else:
        return None


def test_factor_month_effect(total_perf_index_df, factor_name_list):
    month_price_file_list = os.listdir(month_price_dir)
    stock_month_price_info = {}
    print "start to get stock month price"
    for month_price_file in month_price_file_list:
        month_price_df = pd.read_pickle(month_price_dir + os.sep + month_price_file)
        stock_name = month_price_file[:9]
        tmp_stock_info = dict(zip(month_price_df['trade_month'].values, month_price_df['price'].values))
        stock_month_price_info.update({stock_name: tmp_stock_info})
    total_perf_index_df.sort_values(by=['ts_code', 'end_date', 'action_date'], inplace=True)
    total_perf_index_df.drop_duplicates(['ts_code', 'action_date'], keep='last', inplace=True)
    stock_factor_info_df_list = []
    print "start to get factor info"
    stock_process_count = 0
    for stock_code, factor_info_df in total_perf_index_df.groupby('ts_code'):
        stock_process_count += 1
        if stock_process_count % 50 == 0:
            print "stock process count %s" % stock_process_count
        factor_info_df['action_month'] = factor_info_df['action_date'].map(lambda x: x[:6])
        factor_info_df.sort_values(by='action_month', inplace=True)
        min_month = factor_info_df['action_month'].min()
        max_month = factor_info_df['action_month'].max()

        factor_info_df = factor_info_df.set_index('action_month')
        year_months = get_month_between_min_max(min_month, max_month)
        factor_info_df = factor_info_df.reindex(year_months)
        factor_info_df.fillna(method='ffill', inplace=True)
        tmp_stock_month_price_info = stock_month_price_info[stock_code]
        factor_info_df['tmp_month_price'] = factor_info_df.index.map(lambda x: tmp_stock_month_price_info.get(x, None))
        factor_info_df['next_month_price'] = factor_info_df.index.map(lambda x: tmp_stock_month_price_info.get(get_next_month(x), None))
        factor_info_df['one_month_return'] = \
            factor_info_df.apply(lambda x: x['next_month_price']/x['tmp_month_price']
            if x['next_month_price'] is not None and x['tmp_month_price'] is not None else None, axis=1)
        factor_info_df['last_3_month_gain'] = factor_info_df.index.map(lambda x: get_relative_return(x, 3, tmp_stock_month_price_info))
        factor_info_df['last_1_month_gain'] = factor_info_df.index.map(
            lambda x: get_relative_return(x, 1, tmp_stock_month_price_info))
        factor_info_df['last_3_month_gain'] = factor_info_df.index.map(
            lambda x: get_relative_return(x, 3, tmp_stock_month_price_info))
        factor_info_df['last_6_month_gain'] = factor_info_df.index.map(
            lambda x: get_relative_return(x, 6, tmp_stock_month_price_info))
        factor_info_df['last_9_month_gain'] = factor_info_df.index.map(
            lambda x: get_relative_return(x, 9, tmp_stock_month_price_info))
        factor_info_df['last_12_month_gain'] = factor_info_df.index.map(
            lambda x: get_relative_return(x, 12, tmp_stock_month_price_info))
        factor_info_df['last_18_month_gain'] = factor_info_df.index.map(
            lambda x: get_relative_return(x, 18, tmp_stock_month_price_info))
        factor_info_df['last_24_month_gain'] = factor_info_df.index.map(
            lambda x: get_relative_return(x, 24, tmp_stock_month_price_info))
        stock_factor_info_df_list.append(factor_info_df)
    total_factor_info_df = pd.concat(stock_factor_info_df_list)
    total_factor_info_df.to_pickle(perf_index_dir+os.sep+"factor_month_info.pkl")
    total_factor_info_df.reset_index(inplace=True)
    print "analyze factor return"
    for factor_name in factor_name_list:
        for action_month, factor_info in total_factor_info_df.groupby('action_month'):
            rank_corr = factor_info[factor_name].corr(factor_info['one_month_return'], method='spearman')
            print "ann month %s, factor %s, corr %s" % (action_month, factor_name, rank_corr)


def test_factor_change(total_perf_index_df, start_year, end_year, factor_name):
    start_year_factor_df = total_perf_index_df[total_perf_index_df['end_date']==("%s1231" % start_year)][['ts_code', factor_name]]
    # start_year_factor_df = start_year_factor_df.set_index('ts_code')
    for year in range(start_year+1, end_year+1):
        this_year_factor_df = \
            total_perf_index_df[total_perf_index_df['end_date']==("%s1231" % year)][['ts_code', factor_name]]
        # this_year_factor_df = this_year_factor_df.set_index('ts_code')
        # print this_year_factor_df
        merged_factor_df = pd.merge(start_year_factor_df, this_year_factor_df, on='ts_code', how='inner')
        corr_rank = merged_factor_df['%s_x' % factor_name].corr(merged_factor_df['%s_y' % factor_name], method='spearman')
        print "rank corr between year %s and year %s: %s" % (start_year, end_year, corr_rank)




total_perf_index_df = pd.read_pickle(perf_index_dir + os.sep + "rpt_perf_index.pkl")
total_perf_index_df = update_perf_index_df(total_perf_index_df)
# print total_perf_index_df
# total_perf_index_df.to_pickle(perf_index_dir + os.sep + "rpt_perf_index.pkl")
total_perf_index_df = analyze_stock_financial_index()
# perf_index_dir = "/home/lian/market_kline/tushare_data_pro/perf_index"
# total_perf_index_df.to_pickle(perf_index_dir + os.sep + "rpt_perf_index.pkl")
#
# analyse_financial_index_abnormal()
# get_performance_index_general_last_year('000508.SZ')

# perf_name_list = ['net_profit_ratio', 'gross_profit_rate', 'roe', 'currentratio',
#            'quickratio', 'cashratio', 'icratio', 'sheqratio', 'cf_sales',
#            'rateofreturn', 'cf_nm', 'cf_liabilities', 'cashflowratio', 'arturnover',
#            'inventory_turnover', 'currentasset_turnover', 'market_price']
# total_perf_index_df['ep'] = total_perf_index_df.apply(
#     lambda x: x['n_income_attr_p']/x['market_price'] if x['n_income_attr_p'] is not None and x['market_price'] is not None else None, axis=1)
# perf_name_list = ['ep']
# total_perf_index_df.to_pickle(perf_index_dir + os.sep + "rpt_perf_index.pkl")
# feature_perf_df = test_perf_effect(total_perf_index_df, perf_name_list)
# feature_perf_df.to_excel("feature_perf.xls")
# test_perf_effect_with_industry(['ep'], total_perf_index_df)

# get_stock_month_return()
# test_factor_month_effect(total_perf_index_df, ['roe', 'last_1_month_gain',
# 'last_3_month_gain'])
test_factor_change(total_perf_index_df, 2004, 2012, 'roe')